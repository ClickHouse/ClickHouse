#include <Processors/Transforms/StreamInQueryCacheTransform.h>

namespace DB
{

StreamInQueryCacheTransform::StreamInQueryCacheTransform(
    const Block & header_,
    std::shared_ptr<QueryCacheWriter> query_cache_writer_,
    QueryCacheWriter::ChunkType chunk_type_,
    const std::string& query_)
    : ISimpleTransform(header_, header_, false)
    , query_cache_writer(query_cache_writer_)
    , chunk_type(chunk_type_)
    , query(query_)
{
    LOG_TRACE(getLogger("QueryCache"),
                        "Build stream in query cache {} {}", chunk_type_, query_);
}

void StreamInQueryCacheTransform::transform(Chunk & chunk)
{
    LOG_TRACE(getLogger("QueryCache"),
                        "Transform stream in query cache {}", query);
                        
    query_cache_writer->buffer(chunk.clone(), chunk_type);
}

void StreamInQueryCacheTransform::finalizeWriteInQueryCache()
{
    if (!isCancelled())
        query_cache_writer->finalizeWrite();
}

};
