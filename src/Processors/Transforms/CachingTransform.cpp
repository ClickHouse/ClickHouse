#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    cache->addChunk(cache_key, chunk.clone());
}

};
