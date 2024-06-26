#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::PlanSquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header, header)
    , squashing(min_block_size_rows, min_block_size_bytes)
{
}

void PlanSquashingTransform::consume(Chunk chunk)
{
    Chunk result = squashing.add(std::move(chunk));
    if (!result.getChunkInfos().empty())
        squashed_chunk = std::move(result);
}

Chunk PlanSquashingTransform::generate()
{
    if (squashed_chunk.getChunkInfos().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result_chunk;
    result_chunk.swap(squashed_chunk);
    return result_chunk;
}

bool PlanSquashingTransform::canGenerate()
{
    return !squashed_chunk.getChunkInfos().empty();
}

Chunk PlanSquashingTransform::getRemaining()
{
    Chunk current_chunk = squashing.flush();
    return current_chunk;
}
}
