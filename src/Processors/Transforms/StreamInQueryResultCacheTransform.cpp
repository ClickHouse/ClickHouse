#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

void StreamInQueryResultCacheTransform::onPartialResult() noexcept
{
    std::lock_guard lock(cache_publication_mutex);
    partial_result = true;
}

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

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    std::lock_guard lock(cache_publication_mutex);
    if (!isCancelled() && !partial_result)
        query_result_cache_writer->finalizeWrite();
}

};
