#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::PlanSquashingTransform(
    Block header_, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header_, header_)
    , squashing(header_, min_block_size_rows, min_block_size_bytes)
{
}

void PlanSquashingTransform::consume(Chunk chunk)
{
    squashed_chunk = squashing.add(std::move(chunk));
}

Chunk PlanSquashingTransform::generate()
{
    if (!squashed_chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result_chunk;
    result_chunk.swap(squashed_chunk);
    return result_chunk;
}

bool PlanSquashingTransform::canGenerate()
{
    return bool(squashed_chunk);
}

Chunk PlanSquashingTransform::getRemaining()
{
    return squashing.flush();
}
}
