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
    : ISimpleTransform(header, header, true), squashing(min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::transform(Chunk & chunk)
{
    if (!finished)
    {
        if (auto block = squashing.add(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns())))
            chunk.setColumns(block.getColumns(), block.rows());
    }
    else
    {
        if (chunk.hasRows())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk expected to be empty, otherwise it will be lost");

        auto block = squashing.add({});
        chunk.setColumns(block.getColumns(), block.rows());

        /// ISimpleTransform keeps output chunk (result of transform() execution) for some time and push it in the output port within subsequent prepare() call.
        /// Because of our custom prepare() implementation we have to take care of both places where data could be buffered: `output_data` and `squashing`.
        if (output_data.chunk.hasRows())
        {
            auto res = std::move(output_data.chunk);
            output_data.chunk.clear();
            if (chunk.hasRows())
                res.append(chunk);
            chunk = std::move(res);
        }
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
