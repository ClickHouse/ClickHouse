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
    /// Remember whether the upstream finished and all its data was processed before we go to the finished state.
    /// A consumer may close its input port before that (e.g. DISTINCT with a limit hint), in which case
    /// the base class closes our input and the buffered result is partial.
    if (!finished && input.isFinished() && !has_input)
        input_exhausted = true;

    auto status = ISimpleTransform::prepare();
    if (status == Status::Finished)
        finished = true;
    return status;
}

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    /// Store the result only if the whole result passed through the transform. Otherwise the cache entry would be truncated.
    if (!isCancelled() && input_exhausted)
        query_result_cache_writer->finalizeWrite();
}

};
