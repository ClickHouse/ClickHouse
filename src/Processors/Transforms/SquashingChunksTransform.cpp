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
        LOG_DEBUG(getLogger("SquashingChunksTransform"),
              "onConsume {}", chunk.getNumRows());

    if (cur_chunkinfos.empty())
        cur_chunkinfos = chunk.getChunkInfos().clone();

    auto result = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    if (result.block)
    {
        cur_chunk.setColumns(result.block.getColumns(), result.block.rows());
        cur_chunk.setChunkInfos(std::move(cur_chunkinfos));
        cur_chunkinfos = {};
    }

    if (cur_chunkinfos.empty() && result.input_block_delayed)
    {
        cur_chunkinfos = chunk.getChunkInfos().clone();
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
    auto result = squashing.add({});
    finish_chunk.setColumns(result.block.getColumns(), result.block.rows());
    finish_chunk.setChunkInfos(std::move(cur_chunkinfos));
    cur_chunkinfos = {};
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
    LOG_DEBUG(getLogger("SimpleSquashingChunksTransform"),
              "transform rows {}, size {}, columns {}, infos: {}/{}",
              chunk.getNumRows(), chunk.bytes(), chunk.getNumColumns(),
              chunk.getChunkInfos().size(), chunk.getChunkInfos().debug());

    auto result = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));

    squashed_chunk.setColumns(result.block.getColumns(), result.block.rows());
    if (result.input_block_delayed)
    {
        squashed_chunk.setChunkInfos(std::move(squashed_info));
        squashed_info = std::move(chunk.getChunkInfos());
    }
    else
    {
        squashed_chunk.setChunkInfos(std::move(chunk.getChunkInfos()));
    }
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
    auto result = squashing.add({});
    squashed_chunk.setColumns(result.block.getColumns(), result.block.rows());
    squashed_chunk.setChunkInfos(std::move(squashed_info));

    Chunk result_chunk;
    result_chunk.swap(squashed_chunk);
    return result_chunk;
}

}
