#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Common/logger_useful.h>

namespace DB
{

SquashingChunksTransform::SquashingChunksTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SquashingChunksTransform::onConsume(Chunk chunk)
{
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: !finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk.hasChunkInfo());
    if (auto block = squashing.add(std::move(chunk)))
        cur_chunk.setColumns(block.getColumns(), block.rows());
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
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, structure of block: {}", reinterpret_cast<void*>(this), block.dumpStructure());
    finish_chunk.setColumns(block.getColumns(), block.rows());
    LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, hasInfo: {}", reinterpret_cast<void*>(this), finish_chunk.hasChunkInfo());
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
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, [[maybe_unused]] bool skip_empty_chunks_)
    : ISimpleTransform(header, header, false), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: !finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk.hasChunkInfo());
        if (auto block = squashing.add(std::move(chunk)))
            chunk.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        LOG_TRACE(getLogger("squashing"), "{}, SquashingTransform: finished, hasInfo: {}", reinterpret_cast<void*>(this), chunk.hasChunkInfo());
        auto block = squashing.add({});
        chunk.setColumns(block.getColumns(), block.rows());
    }
}

IProcessor::Status SimpleSquashingChunksTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        finished = true;
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}
}
