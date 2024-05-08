#include <Processors/Transforms/SquashingTransform.h>
#include <Common/logger_useful.h>

namespace DB
{

SquashingTransform::SquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SquashingTransform::onConsume(Chunk chunk)
{
    if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
        cur_chunk.setColumns(block.getColumns(), block.rows());
}

SquashingTransform::GenerateResult SquashingTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void SquashingTransform::onFinish()
{
    auto block = squashing.add({});
    finish_chunk.setColumns(block.getColumns(), block.rows());
}

void SquashingTransform::work()
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

SimpleSquashingTransform::SimpleSquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ISimpleTransform(header, header, false), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
            chunk.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        auto block = squashing.add({});
        chunk.setColumns(block.getColumns(), block.rows());
    }
}

IProcessor::Status SimpleSquashingTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        finished = true;
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}
}
