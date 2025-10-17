#include <Processors/Transforms/PlanSquashingTransform.h>

#include <Processors/Port.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::PlanSquashingTransform(
    SharedHeader header_, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header_, header_, false)
    , squashing(header_, min_block_size_rows, min_block_size_bytes)
{
}

void PlanSquashingTransform::onConsume(Chunk chunk)
{
    squashed_chunk = squashing.add(std::move(chunk));
}

PlanSquashingTransform::GenerateResult PlanSquashingTransform::onGenerate()
{
    if (!squashed_chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    GenerateResult res;
    res.chunk.swap(squashed_chunk);
    res.is_done = true;
    return res;
}

bool PlanSquashingTransform::canGenerate()
{
    return bool(squashed_chunk);
}

PlanSquashingTransform::GenerateResult PlanSquashingTransform::getRemaining()
{
    GenerateResult res;
    res.chunk = squashing.flush();
    res.is_done = true;
    return res;
}
}
