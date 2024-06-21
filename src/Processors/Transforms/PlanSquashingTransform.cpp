#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/IProcessor.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::PlanSquashingTransform(
    const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : IInflatingTransform(header, header), squashing(header, min_block_size_rows, min_block_size_bytes)
{
}

void PlanSquashingTransform::consume(Chunk chunk)
{
    LOG_TRACE(getLogger("consume"), "1");
    if (Chunk current_chunk = squashing.add(std::move(chunk)))
        squashed_chunk.swap(current_chunk);
}

Chunk PlanSquashingTransform::generate()
{
    LOG_TRACE(getLogger("generate"), "1");
    if (!squashed_chunk.hasChunkInfo())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in SimpleSquashingChunksTransform");

    Chunk result_chunk;
    result_chunk.swap(squashed_chunk);
    return result_chunk;
}

bool PlanSquashingTransform::canGenerate()
{
    LOG_TRACE(getLogger("canGenerate"), "1");
    return squashed_chunk.hasChunkInfo();
}

Chunk PlanSquashingTransform::getRemaining()
{
    LOG_TRACE(getLogger("getRemaining"), "1");
    Chunk current_chunk = squashing.flush();
    // squashed_chunk.swap(current_chunk);

    // Chunk result_chunk;
    // result_chunk.swap(squashed_chunk);
    return current_chunk;
}
}
