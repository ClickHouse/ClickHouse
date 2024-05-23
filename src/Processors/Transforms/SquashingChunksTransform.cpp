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

Block buildBlock(const Block & header, Chunk chunk)
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

    if (block.info.cursors.has_value())
        chunk.setChunkInfo(std::make_shared<CursorInfo>(std::move(block.info.cursors.value())), CursorInfo::INFO_SLOT);
}

}

SquashingChunksTransform::SquashingChunksTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SquashingChunksTransform::onConsume(Chunk chunk)
{
    if (auto block = squashing.add(buildBlock(getInputPort().getHeader(), std::move(chunk))))
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
    completeChunk(finish_chunk, std::move(block));
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
    Block current_block = squashing.add(buildBlock(getInputPort().getHeader(), std::move(chunk)));
    completeChunk(squashed_chunk, std::move(current_block));
}

Chunk SimpleSquashingChunksTransform::generate()
{
    if (squashed_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    return std::move(squashed_chunk);
}

bool SimpleSquashingChunksTransform::canGenerate()
{
    return !squashed_chunk.empty();
}

Chunk SimpleSquashingChunksTransform::getRemaining()
{
    Block current_block = squashing.add({});
    completeChunk(squashed_chunk, std::move(current_block));
    return std::move(squashed_chunk);
}

}
