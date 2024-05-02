#include <Processors/CursorInfo.h>
#include <Processors/Transforms/SquashingChunksTransform.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

Block buildBlock(const Block & header, Chunk & chunk)
{
    auto block = header.cloneWithColumns(chunk.detachColumns());

    if (auto chunk_info = chunk.getChunkInfo(CursorInfo::INFO_SLOT))
        if (const auto * cursor_info = typeid_cast<const CursorInfo *>(chunk_info.get()))
            block.info.cursors = std::move(cursor_info->cursors);

    return block;
}

void completeChunk(Chunk & chunk, Block block)
{
    chunk.setColumns(block.getColumns(), block.rows());

    if (!block.info.cursors.empty())
        chunk.setChunkInfo(std::make_shared<CursorInfo>(std::move(block.info.cursors)), CursorInfo::INFO_SLOT);
}

}

SquashingChunksTransform::SquashingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SquashingChunksTransform::onConsume(Chunk chunk)
{
    if (auto block = squashing.add(buildBlock(getInputPort().getHeader(), chunk)))
        completeChunk(cur_chunk, std::move(block));
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
    : ISimpleTransform(header, header, true), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        if (auto block = squashing.add(buildBlock(getInputPort().getHeader(), chunk)))
            completeChunk(chunk, std::move(block));
    }
    else
    {
        if (chunk.hasRows())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk expected to be empty, otherwise it will be lost");

        auto block = squashing.add({});
        completeChunk(chunk, std::move(block));
    }
}

IProcessor::Status SimpleSquashingChunksTransform::prepare()
{
    if (!finished && input.isFinished())
    {
        if (output.isFinished())
            return Status::Finished;

        if (!output.canPush())
            return Status::PortFull;

        if (has_output)
        {
            output.pushData(std::move(output_data));
            has_output = false;
            return Status::PortFull;
        }

        finished = true;
        /// On the next call to transform() we will return all data buffered in `squashing` (if any)
        return Status::Ready;
    }
    return ISimpleTransform::prepare();
}

}
