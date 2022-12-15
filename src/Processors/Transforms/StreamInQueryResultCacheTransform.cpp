#include <Processors/Transforms/StreamInQueryResultCacheTransform.h>

namespace DB
{

StreamInQueryResultCacheTransform::StreamInQueryResultCacheTransform(
    const Block & header_,
    QueryResultCachePtr cache,
    QueryResultCache::Key cache_key,
    size_t max_entries,
    size_t max_entry_size)
    : ISimpleTransform(header_, header_, false)
    , cache_writer(cache->createWriter(cache_key, max_entries, max_entry_size))
{
}

void StreamInQueryResultCacheTransform::transform(Chunk & chunk)
{
    cache_writer.buffer(chunk.clone());
}

};
