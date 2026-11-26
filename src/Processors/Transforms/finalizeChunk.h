#pragma once

#include <vector>
#include <Common/VectorWithMemoryTracking.h>
#include <Interpreters/AggregateDescription.h>
#include <Processors/Chunk.h>

namespace DB
{

class Block;
using ColumnsMask = VectorWithMemoryTracking<bool>;

ColumnsMask getAggregatesMask(const Block & header, const AggregateDescriptions & aggregates);

/// Convert ColumnAggregateFunction to real values.
///
/// @param aggregates_mask columns to convert (see getAggregatesMask())
/// @param finalized_header the expected header after the conversion; throws if a state finalizes to another type
void finalizeChunk(Chunk & chunk, const ColumnsMask & aggregates_mask, const Block & finalized_header);

}
