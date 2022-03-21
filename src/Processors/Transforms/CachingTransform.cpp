#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    CacheKey cache_key{query_ptr, header};
    auto cache_value = cache.get(cache_key);

    auto header_ = cache_value->first;
    auto chunks = std::move(cache_value->second);
    chunks.push_back(chunk.clone());
    cache.set(cache_key, std::make_shared<Data>(header_, std::move(chunks)));
}

};
