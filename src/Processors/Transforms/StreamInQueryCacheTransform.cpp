#include <Processors/Transforms/StreamInQueryCacheTransform.h>

namespace DB
{

StreamInQueryCacheTransform::StreamInQueryCacheTransform(
    const Block & header_,
    QueryCachePtr cache,
    const QueryCache::Key & cache_key,
    std::chrono::milliseconds min_query_duration,
    bool squash_partial_results,
    size_t max_block_size,
    size_t max_query_cache_size_in_bytes_quota, size_t max_query_cache_entries_quota)
    : ISimpleTransform(header_, header_, false)
    , cache_writer(cache->createWriter(cache_key, min_query_duration, squash_partial_results, max_block_size, max_query_cache_size_in_bytes_quota, max_query_cache_entries_quota))
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
