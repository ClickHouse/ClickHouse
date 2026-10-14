#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

StreamInQueryResultCacheTransform::StreamInQueryResultCacheTransform(
    const Block & header_,
    std::shared_ptr<QueryResultCacheWriter> query_result_cache_writer_,
    QueryResultCacheWriter::ChunkType chunk_type_)
    : ISimpleTransform(header_, header_, false)
    , query_result_cache_writer(query_result_cache_writer_)
    , chunk_type(chunk_type_)
{
}

void StreamInQueryResultCacheTransform::transform(Chunk & chunk)
{
    compactReplicatedColumns(chunk);
    query_result_cache_writer->buffer(chunk.clone(), chunk_type);
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

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    if (!isCancelled())
        query_result_cache_writer->finalizeWrite();
}

};
