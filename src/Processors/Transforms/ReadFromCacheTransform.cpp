#include <Processors/Transforms/ReadFromCacheTransform.h>

namespace DB
{

Chunk ReadFromCacheTransform::generate()
{
    if (chunks_read_count < chunks.size())
    {
        return chunks[chunks_read_count++].clone();
    }
    return {};
}

ReadFromCacheTransform::ReadFromCacheTransform(const Block & header, const Chunks& chunks_)
    : ISource(header)
    , chunks(chunks_)
    , chunks_read_count(0)
{}

};

