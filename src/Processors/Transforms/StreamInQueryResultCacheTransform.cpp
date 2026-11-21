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
    query_result_cache_writer->buffer(chunk.clone(), chunk_type);
}

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    if (!isCancelled())
        query_result_cache_writer->finalizeWrite();
}

};
