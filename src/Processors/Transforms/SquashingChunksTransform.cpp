#include <Processors/Transforms/SquashingChunksTransform.h>
#include <iostream>

namespace DB
{

SquashingChunksTransform::SquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_memory)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(min_block_size_rows, min_block_size_bytes, reserve_memory)
{
}

void SquashingChunksTransform::transform(Chunk & chunk)
{
    if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
    {
        chunk.setColumns(block.getColumns(), block.rows());
    }
}

void SquashingChunksTransform::onFinish()
{
    auto block = squashing.add({});
    finish_chunk.setColumns(block.getColumns(), block.rows());
}

void SquashingChunksTransform::work()
{
    if (has_exception)
    {
        data.chunk.clear();
        ready_input = false;
        return;
    }

    ExceptionKeepingTransform::work();
    if (finish_chunk)
    {
        data.chunk = std::move(finish_chunk);
        ready_output = true;
    }
}

}
