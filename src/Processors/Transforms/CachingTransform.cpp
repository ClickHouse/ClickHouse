#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    CacheKey cache_key{query_ptr, header};
    if (cache.get(cache_key) == nullptr)
    {
        cache.set(cache_key, std::make_shared<Data>(header, Chunks{}));
    }
    cache.get(cache_key)->second.push_back(chunk.clone());
}

};
