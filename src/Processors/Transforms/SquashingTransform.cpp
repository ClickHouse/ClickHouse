#include <utility>
#include <Processors/Transforms/SquashingTransform.h>
#include <Interpreters/Squashing.h>
#include <Processors/Chunk.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SquashingTransform::SquashingTransform(
    SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes,
    size_t max_block_size_rows, size_t max_block_size_bytes, bool squash_with_strict_limits)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(header, min_block_size_rows, min_block_size_bytes,
                max_block_size_rows, max_block_size_bytes, squash_with_strict_limits)
{
}

void SquashingTransform::onConsume(Chunk chunk)
{
    squashing.add(std::move(chunk));
}

SquashingTransform::GenerateResult SquashingTransform::onGenerate()
{
    cur_chunk = Squashing::squash(squashing.generate(), getInputPort().getSharedHeader());

    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

bool SquashingTransform::canGenerate()
{
    return squashing.canGenerate();
}

void SquashingTransform::onFinish()
{
    finish_chunk = Squashing::squash(squashing.flush(), getInputPort().getSharedHeader());
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

SimpleSquashingChunksTransform::SimpleSquashingChunksTransform(
    SharedHeader header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header, header)
    , squashing(header, min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::consume(Chunk chunk)
{
    squashing.add(std::move(chunk));
}

Chunk SimpleSquashingChunksTransform::generate()
{
    squashed_chunk = Squashing::squash(squashing.generate(), getOutputPort().getSharedHeader());

    if (squashed_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result;
    result.swap(squashed_chunk);
    return result;
}

bool SimpleSquashingChunksTransform::canGenerate()
{
    return squashing.canGenerate();
}

Chunk SimpleSquashingChunksTransform::getRemaining()
{
    return Squashing::squash(squashing.flush(), getOutputPort().getSharedHeader());
}

}
