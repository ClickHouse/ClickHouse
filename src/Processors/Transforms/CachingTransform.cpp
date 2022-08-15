#include <Processors/Transforms/CachingTransform.h>

namespace DB
{

void CachingTransform::transform(Chunk & chunk)
{
    holder.insertChunk(chunk.clone());
}

};
