#include <Processors/Transforms/SquashingChunksTransform.h>

namespace DB
{

SquashingChunksTransform::SquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_memory)
    : IAccumulatingTransform(header, header)
    , squashing(min_block_size_rows, min_block_size_bytes, reserve_memory)
{
}

void SquashingChunksTransform::consume(Chunk chunk)
{
    if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
    {
        setReadyChunk(Chunk(block.getColumns(), block.rows()));
    }
}

Chunk SquashingChunksTransform::generate()
{
    auto block = squashing.add({});
    return Chunk(block.getColumns(), block.rows());
}

}
