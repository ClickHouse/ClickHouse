#include <Processors/Transforms/PlanSquashingTransform.h>

#include <Processors/Port.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::    PlanSquashingTransform(
        SharedHeader header_, size_t min_block_size_rows, size_t min_block_size_bytes,
        size_t max_block_size_rows, size_t max_block_size_bytes, bool squash_with_strict_limits)
    : ExceptionKeepingTransform(header_, header_, false)
    , squashing(header_, min_block_size_rows, min_block_size_bytes,
                max_block_size_rows, max_block_size_bytes, squash_with_strict_limits)
{
}

void PlanSquashingTransform::onConsume(Chunk chunk)
{
    squashing.add(std::move(chunk));
}

PlanSquashingTransform::GenerateResult PlanSquashingTransform::onGenerate()
{
    squashed_chunk = squashing.generate();

    if (!squashed_chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in PlanSquashingChunksTransform");

    GenerateResult res;
    res.chunk.swap(squashed_chunk);
    res.is_done = !squashing.canGenerate();
    return res;
}

bool PlanSquashingTransform::canGenerate()
{
    return squashing.canGenerate();
}

PlanSquashingTransform::GenerateResult PlanSquashingTransform::getRemaining()
{
    GenerateResult res;
    res.chunk = squashing.flush();
    res.is_done = true;
    return res;
}
}
