#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

StreamInQueryResultCacheTransform::StreamInQueryResultCacheTransform(
    const Block & header_,
    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer_,
    QueryResultCacheWriter::ChunkType chunk_type_,
    std::shared_ptr<StreamInQueryResultCacheSubqueryCompletion> subquery_completion_)
    : ISimpleTransform(header_, header_, false)
    , query_result_cache_writer(query_result_cache_writer_)
    , chunk_type(chunk_type_)
    , subquery_completion(std::move(subquery_completion_))
{
    if (subquery_completion)
        ++subquery_completion->unfinished_streams;
}

void StreamInQueryResultCacheTransform::transform(Chunk & chunk)
{
    compactReplicatedColumns(chunk);
    query_result_cache_writer->buffer(chunk.clone(), chunk_type);
}

void StreamInQueryResultCacheTransform::transform(std::exception_ptr &)
{
    if (subquery_completion)
        subquery_completion->has_exception = true;
}

void StreamInQueryResultCacheTransform::onFinish()
{
    if (!subquery_completion || stream_finished)
        return;
    stream_finished = true;

    if (--subquery_completion->unfinished_streams != 0)
        return;

    /// All streams of the subquery are finished. If the result is incomplete (a consumer stopped reading it early), the writer
    /// knows that and doesn't store it. The enclosing query calls `finalizeWriteInQueryResultCache` later, which is a no-op then.
    if (subquery_completion->has_exception || isCancelled())
        return;

    query_result_cache_writer->finalizeWrite();
    subquery_completion->herd_token_holder->release(/*succeeded=*/ true);
}

IProcessor::Status StreamInQueryResultCacheTransform::prepare()
{
    /// A consumer may stop reading before the upstream is exhausted, e.g. `LIMIT` or `DISTINCT` with a limit hint. The base class then
    /// closes our input, and the chunks buffered so far are only a prefix of the query result. Such a result must not be cached.
    /// Several transforms (one per stream) share the writer, so the writer remembers that the result is incomplete.
    if (output.isFinished() && !(input.isFinished() && !has_input))
        query_result_cache_writer->markResultIncomplete();

    return ISimpleTransform::prepare();
}

void StreamInQueryResultCacheTransform::cancel(CancelReason reason) noexcept
{
    /// The client cancelled the query after a first partial result (setting `partial_result_on_first_cancel`). Only the sources stop,
    /// the rest of the pipeline drains and the query finishes successfully with the rows read so far. This transform then sees an
    /// exhausted input, but the buffered result is only a prefix, so it must not be stored.
    if (reason == CancelReason::PartialResult)
        query_result_cache_writer->markResultIncomplete();

    ISimpleTransform::cancel(reason);
}

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    const bool cancelled = isCancelled();
    if (!cancelled)
        query_result_cache_writer->finalizeWrite();

    /// Tell the waiters whether they can expect the result in the cache (or, if it could not be stored there, should run
    /// independently), or whether one of them should take over the execution.
    if (subquery_completion)
        subquery_completion->herd_token_holder->release(/*succeeded=*/ !cancelled);
}

};
