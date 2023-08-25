#include <Processors/Transforms/StreamInQueryCacheTransform.h>

namespace DB
{

StreamInQueryCacheTransform::StreamInQueryCacheTransform(
    const Block & header_, QueryCachePtr cache, const QueryCache::Key & cache_key, std::chrono::milliseconds min_query_duration)
    : ISimpleTransform(header_, header_, false)
    , cache_writer(cache->createWriter(cache_key, min_query_duration))
{
}

void StreamInQueryCacheTransform::transform(Chunk & chunk)
{
    cache_writer.buffer(chunk.clone());
}

void StreamInQueryCacheTransform::finalizeWriteInQueryCache()
{
    if (!isCancelled())
        cache_writer.finalizeWrite();
}

};
