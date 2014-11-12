/*
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.http.util

import java.util.concurrent.atomic.AtomicBoolean
import java.io.InputStream

import org.reactivestreams.{ Subscriber, Publisher }

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }
import akka.stream.impl.fusing.DeterministicOp
import akka.stream.impl.fusing.Directive
import akka.stream.impl.fusing.Context

import akka.actor.Props
import akka.util.ByteString

import akka.stream.{ impl, FlowMaterializer }
import akka.stream.scaladsl._

import akka.http.model.RequestEntity

/**
 * INTERNAL API
 */
private[http] object StreamUtils {

  /**
   * Creates a transformer that will call `f` for each incoming ByteString and output its result. After the complete
   * input has been read it will call `finish` once to determine the final ByteString to post to the output.
   */
  def byteStringTransformer(f: ByteString ⇒ ByteString, finish: () ⇒ ByteString): ByteStringTransformer =
    new ByteStringTransformer {
      override def onNext(element: ByteString): ByteString = f(element)
      override def onComplete(): ByteString = finish()
    }

  def failedPublisher[T](ex: Throwable): Publisher[T] =
    impl.ErrorPublisher(ex).asInstanceOf[Publisher[T]]

  def mapErrorTransformer(f: Throwable ⇒ Throwable): ByteStringTransformer =
    new ByteStringTransformer {
      override def onNext(element: ByteString): ByteString = element
      override def onError(cause: Throwable): Throwable = f(cause)
    }

  def sliceBytesTransformer(start: Long, length: Long): ByteStringTransformer =
    new ByteStringTransformer {
      type State = ByteStringTransformer

      def skipping = new State {
        var toSkip = start
        override def onNext(element: ByteString): ByteString =
          if (element.length < toSkip) {
            // keep skipping
            toSkip -= element.length
            ByteStringTransformer.NoElement
          } else {
            become(taking(length))
            // toSkip <= element.length <= Int.MaxValue
            currentState.onNext(element.drop(toSkip.toInt))
          }
      }
      def taking(initiallyRemaining: Long) = new State {
        var remaining: Long = initiallyRemaining
        override def onNext(element: ByteString): ByteString = {
          val data = element.take(math.min(remaining, Int.MaxValue).toInt)
          remaining -= data.size
          if (remaining <= 0) become(finishing)
          data
        }
      }
      def finishing = new State {
        override def isComplete: Boolean = true
        override def onNext(element: ByteString): ByteString =
          throw new IllegalStateException("onNext called on complete stream")
      }

      var currentState: State = if (start > 0) skipping else taking(length)
      def become(state: State): Unit = currentState = state

      override def isComplete: Boolean = currentState.isComplete
      override def onNext(element: ByteString): ByteString = currentState.onNext(element)
      override def onComplete(): ByteString = currentState.onComplete()
    }

  /**
   * Applies a sequence of transformers on one source and returns a sequence of sources with the result. The input source
   * will only be traversed once.
   */
  def transformMultiple(input: Source[ByteString], transformers: immutable.Seq[() ⇒ ByteStringTransformer])(implicit materializer: FlowMaterializer): immutable.Seq[Source[ByteString]] =
    transformers match {
      case Nil      ⇒ Nil
      case Seq(one) ⇒ Vector(input.transform("transformMultipleElement", () ⇒ ByteStringTransformerOp(one())))
      case multiple ⇒
        val results = Vector.fill(multiple.size)(Sink.publisher[ByteString])
        val mat =
          FlowGraph { implicit b ⇒
            import FlowGraphImplicits._

            val broadcast = Broadcast[ByteString]("transformMultipleInputBroadcast")
            input ~> broadcast
            (multiple, results).zipped.foreach { (trans, sink) ⇒
              broadcast ~> Flow[ByteString].transform("transformMultipleElement", () ⇒ ByteStringTransformerOp(trans())) ~> sink
            }
          }.run()
        results.map(s ⇒ Source(mat.get(s)))
    }

  def mapEntityError(f: Throwable ⇒ Throwable): RequestEntity ⇒ RequestEntity =
    _.transformDataBytes(() ⇒ mapErrorTransformer(f))

  /**
   * Simple blocking Source backed by an InputStream.
   *
   * FIXME: should be provided by akka-stream, see #15588
   */
  def fromInputStreamSource(inputStream: InputStream, defaultChunkSize: Int = 65536): Source[ByteString] = {
    import akka.stream.impl._

    def props(materializer: ActorBasedFlowMaterializer): Props = {
      val iterator = new Iterator[ByteString] {
        var finished = false
        def hasNext: Boolean = !finished
        def next(): ByteString =
          if (!finished) {
            val buffer = new Array[Byte](defaultChunkSize)
            val read = inputStream.read(buffer)
            if (read < 0) {
              finished = true
              inputStream.close()
              ByteString.empty
            } else ByteString.fromArray(buffer, 0, read)
          } else ByteString.empty
      }

      Props(new IteratorPublisherImpl(iterator, materializer.settings)).withDispatcher(materializer.settings.fileIODispatcher)
    }

    new AtomicBoolean(false) with SimpleActorFlowSource[ByteString] {
      override def attach(flowSubscriber: Subscriber[ByteString], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
        create(materializer, flowName)._1.subscribe(flowSubscriber)

      override def isActive: Boolean = true
      override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Publisher[ByteString], Unit) =
        if (!getAndSet(true)) {
          val ref = materializer.actorOf(props(materializer), name = s"$flowName-0-InputStream-source")
          val publisher = ActorPublisher[ByteString](ref)
          ref ! ExposedPublisher(publisher.asInstanceOf[impl.ActorPublisher[Any]])

          (publisher, ())
        } else (ErrorPublisher(new IllegalStateException("One time source can only be instantiated once")).asInstanceOf[Publisher[ByteString]], ())
    }
  }

  /**
   * Returns a source that can only be used once for testing purposes.
   */
  def oneTimeSource[T](other: Source[T]): Source[T] = {
    import akka.stream.impl._
    val original = other.asInstanceOf[ActorFlowSource[T]]
    new AtomicBoolean(false) with SimpleActorFlowSource[T] {
      override def attach(flowSubscriber: Subscriber[T], materializer: ActorBasedFlowMaterializer, flowName: String): Unit =
        create(materializer, flowName)._1.subscribe(flowSubscriber)
      override def isActive: Boolean = true
      override def create(materializer: ActorBasedFlowMaterializer, flowName: String): (Publisher[T], Unit) =
        if (!getAndSet(true)) (original.create(materializer, flowName)._1, ())
        else (ErrorPublisher(new IllegalStateException("One time source can only be instantiated once")).asInstanceOf[Publisher[T]], ())
    }
  }
}

/**
 * INTERNAL API
 */
private[http] class EnhancedByteStringSource(val byteStringStream: Source[ByteString]) extends AnyVal {
  def join(implicit materializer: FlowMaterializer): Future[ByteString] =
    byteStringStream.fold(ByteString.empty)(_ ++ _)
  def utf8String(implicit materializer: FlowMaterializer, ec: ExecutionContext): Future[String] =
    join.map(_.utf8String)
}

object ByteStringTransformer {
  val NoElement: ByteString = null

  def join(a: ByteString, b: ByteString): ByteString = {
    val a2 = if (a eq NoElement) ByteString.empty else a
    if (b eq NoElement) a2
    else a2 ++ b
  }

  def noElementAsEmpty(elem: ByteString): ByteString =
    if (elem eq NoElement) ByteString.empty else elem
}

abstract class ByteStringTransformer {
  import ByteStringTransformer.NoElement
  def onNext(element: ByteString): ByteString
  def isComplete: Boolean = false
  def onComplete(): ByteString = NoElement
  def onError(cause: Throwable): Throwable = cause
}

/**
 * INTERNAL API
 */
private[http] final case class ByteStringTransformerOp(transformer: ByteStringTransformer) extends DeterministicOp[ByteString, ByteString] {
  import ByteStringTransformer.NoElement

  private var completed = false

  override def onPush(elem: ByteString, ctxt: Context[ByteString]): Directive = {
    val transformed = transformer.onNext(elem)
    completed = transformer.isComplete
    if ((transformed eq NoElement) && completed) ctxt.finish()
    else if (transformed eq NoElement) ctxt.pull()
    else if (completed) ctxt.pushAndFinish(elem)
    else ctxt.push(transformed)
  }

  override def onPull(ctxt: Context[ByteString]): Directive = {
    if (isFinishing || completed) {
      val trailer = transformer.onComplete()
      if (trailer eq NoElement) ctxt.finish()
      else ctxt.pushAndFinish(trailer)

    } else {
      ctxt.pull()
    }
  }

  override def onFailure(cause: Throwable, ctxt: Context[ByteString]): Directive =
    ctxt.fail(transformer.onError(cause))

  override def onUpstreamFinish(ctxt: Context[ByteString]): Directive =
    ctxt.absorbTermination()
}

