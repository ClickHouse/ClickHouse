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
        cur_chunkinfos = chunk.getChunkInfos();

    auto result = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    if (result.block)
    {
        cur_chunk.setColumns(result.block.getColumns(), result.block.rows());
        cur_chunk.setChunkInfos(std::move(cur_chunkinfos));
        cur_chunkinfos = {};
    }

    if (cur_chunkinfos.empty() && result.input_block_delayed)
    {
        cur_chunkinfos = chunk.getChunkInfos();
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
    : ISimpleTransform(header, header, true), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    LOG_DEBUG(getLogger("SimpleSquashingChunksTransform"),
              "transform {}", chunk.getNumRows());

    if (!finished)
    {
        if (cur_chunkinfos.empty())
            cur_chunkinfos = chunk.getChunkInfos();

        auto result = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));
        if (result.block)
        {
            chunk.setColumns(result.block.getColumns(), result.block.rows());
            chunk.setChunkInfos(std::move(cur_chunkinfos));
            cur_chunkinfos = {};
        }

        if (cur_chunkinfos.empty() && result.input_block_delayed)
        {
            cur_chunkinfos = chunk.getChunkInfos();
        }
    }
    else
    {
        if (chunk.hasRows())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk expected to be empty, otherwise it will be lost");

        auto result = squashing.add({});
        chunk.setColumns(result.block.getColumns(), result.block.rows());
        chunk.setChunkInfos(std::move(cur_chunkinfos));
        cur_chunkinfos = {};
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
