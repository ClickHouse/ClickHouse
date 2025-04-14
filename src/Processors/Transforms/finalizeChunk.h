#pragma once

#include <Interpreters/AggregateDescription.h>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <vector>

namespace DB
{

using ColumnsMask = std::vector<bool>;

ColumnsMask getAggregatesMask(const Block & header, const AggregateDescriptions & aggregates);

/// Convert ColumnAggregateFunction to real values.
///
/// @param aggregates_mask columns to convert (see getAggregatesMask())
void finalizeChunk(Chunk & chunk, const ColumnsMask & aggregates_mask);

}
