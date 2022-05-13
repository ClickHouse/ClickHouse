#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    data->second.push_back(chunk.clone());
    cache->updateCacheSize(cache_key);
}

};
