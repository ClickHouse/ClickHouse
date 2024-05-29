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
        // LOG_DEBUG(getLogger("SquashingChunksTransform"),
        //       "onConsume {}", chunk.getNumRows());

    auto result = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    cur_chunk = Chunk(result.block.getColumns(), result.block.rows());

    if (result.block)
    {
        cur_chunk.setColumns(result.block.getColumns(), result.block.rows());
        if (result.input_block_delayed)
        {
            cur_chunk.setChunkInfos(std::move(cur_chunkinfos));
            cur_chunkinfos = std::move(chunk.getChunkInfos());
        }
        else
        {
            cur_chunk.setChunkInfos(chunk.getChunkInfos());
            cur_chunkinfos = {};
        }

        // LOG_DEBUG(getLogger("SquashingChunksTransform"),
        //       "got result rows {}, size {}, columns {}, infos: {}/{}",
        //       cur_chunk.getNumRows(), cur_chunk.bytes(), cur_chunk.getNumColumns(),
        //       cur_chunk.getChunkInfos().size(), cur_chunk.getChunkInfos().debug());
    }
    else
    {
        assert(result.input_block_delayed);
        cur_chunkinfos = std::move(chunk.getChunkInfos());
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
    // LOG_DEBUG(getLogger("SimpleSquashingChunksTransform"),
    //           "transform rows {}, size {}, columns {}, infos: {}/{}",
    //           chunk.getNumRows(), chunk.bytes(), chunk.getNumColumns(),
    //           chunk.getChunkInfos().size(), chunk.getChunkInfos().debug());

    auto result = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));

    if (result.block)
    {
        squashed_chunk.setColumns(result.block.getColumns(), result.block.rows());
        if (result.input_block_delayed)
        {
            squashed_chunk.setChunkInfos(std::move(squashed_info));
            squashed_info = std::move(chunk.getChunkInfos());
        }
        else
        {
            squashed_chunk.setChunkInfos(chunk.getChunkInfos());
            squashed_info = {};
        }

        // LOG_DEBUG(getLogger("SimpleSquashingChunksTransform"),
        //       "got result rows {}, size {}, columns {}, infos: {}/{}",
        //       squashed_chunk.getNumRows(), squashed_chunk.bytes(), squashed_chunk.getNumColumns(),
        //       squashed_chunk.getChunkInfos().size(), squashed_chunk.getChunkInfos().debug());
    }
    else
    {
        chassert(result.input_block_delayed);
        squashed_info = std::move(chunk.getChunkInfos());
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
