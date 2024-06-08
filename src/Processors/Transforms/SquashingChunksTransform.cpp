#include <Processors/Transforms/SquashingChunksTransform.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

SquashingChunksTransform::SquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SquashingChunksTransform::onConsume(Chunk chunk)
{
    if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
    {
        cur_chunk.setColumns(block.getColumns(), block.rows());
    }
}

SquashingChunksTransform::GenerateResult SquashingChunksTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void SquashingChunksTransform::onFinish()
{
    auto block = squashing.add({});
    finish_chunk.setColumns(block.getColumns(), block.rows());
}

void SquashingChunksTransform::work()
{
    if (stage == Stage::Exception)
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

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header, header), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::consume(Chunk chunk)
{
    Block current_block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    squashed_chunk.setColumns(current_block.getColumns(), current_block.rows());
}

Chunk SimpleSquashingChunksTransform::generate()
{
    if (squashed_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result_chunk;
    result_chunk.swap(squashed_chunk);
    return result_chunk;
}

bool SimpleSquashingChunksTransform::canGenerate()
{
    return !squashed_chunk.empty();
}

Chunk SimpleSquashingChunksTransform::getRemaining()
{
    Block current_block = squashing.add({});
    squashed_chunk.setColumns(current_block.getColumns(), current_block.rows());

    Chunk result_chunk;
    result_chunk.swap(squashed_chunk);
    return result_chunk;
}

}
