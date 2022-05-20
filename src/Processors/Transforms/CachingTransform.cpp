#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    if (!fits_into_memory)
    {
        return;
    }

    fits_into_memory = cache->insertChunk(cache_key, chunk.clone());
}

};
