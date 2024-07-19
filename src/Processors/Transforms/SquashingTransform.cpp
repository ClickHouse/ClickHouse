#include <utility>
#include <Processors/Transforms/SquashingTransform.h>
#include <Interpreters/Squashing.h>
#include "Processors/Chunk.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SquashingTransform::SquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false)
    , squashing(header, min_block_size_rows, min_block_size_bytes)
{
}

void SquashingTransform::onConsume(Chunk chunk)
{
    cur_chunk = Squashing::squash(squashing.add(std::move(chunk)));
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
    finish_chunk = Squashing::squash(squashing.flush());
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
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header, header)
    , squashing(header, min_block_size_rows, min_block_size_bytes)
{
}

void SimpleSquashingChunksTransform::consume(Chunk chunk)
{
    squashed_chunk = Squashing::squash(squashing.add(std::move(chunk)));
}

Chunk SimpleSquashingChunksTransform::generate()
{
    if (squashed_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result;
    result.swap(squashed_chunk);
    return result;
}

bool SimpleSquashingChunksTransform::canGenerate()
{
    return !squashed_chunk.empty();
}

Chunk SimpleSquashingChunksTransform::getRemaining()
{
    return Squashing::squash(squashing.flush());
}

}
