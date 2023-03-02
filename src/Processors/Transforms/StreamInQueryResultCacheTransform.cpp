#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

StreamInQueryResultCacheTransform::StreamInQueryResultCacheTransform(
    const Block & header_, QueryResultCachePtr cache, const QueryResultCache::Key & cache_key, std::chrono::milliseconds min_query_duration)
    : ISimpleTransform(header_, header_, false)
    , cache_writer(cache->createWriter(cache_key, min_query_duration))
{
}

void StreamInQueryResultCacheTransform::transform(Chunk & chunk)
{
    cache_writer.buffer(chunk.clone());
}

void StreamInQueryResultCacheTransform::finalizeWriteInQueryResultCache()
{
    if (!isCancelled())
        cache_writer.finalizeWrite();
}

};
