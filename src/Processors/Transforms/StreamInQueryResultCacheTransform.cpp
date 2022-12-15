#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

StreamInQueryResultCacheTransform::StreamInQueryResultCacheTransform(const Block & header_, QueryResultCachePtr cache, const QueryResultCache::Key & cache_key,
    size_t max_entries, size_t max_entry_size_in_bytes, size_t max_entry_size_in_rows, std::chrono::milliseconds min_query_duration)
    : ISimpleTransform(header_, header_, false)
    , cache_writer(cache->createWriter(cache_key, max_entries, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_duration))
{
}

void StreamInQueryResultCacheTransform::transform(Chunk & chunk)
{
    cache_writer.buffer(chunk.clone());
}

};
