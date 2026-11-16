#pragma once

#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// To carry part level and virtual row if chunk is produced by a merge tree source
class MergeTreeReadInfo : public ChunkInfoCloneable<MergeTreeReadInfo>
{
public:
    MergeTreeReadInfo() = default;
    explicit MergeTreeReadInfo(size_t part_level);
    explicit MergeTreeReadInfo(size_t part_level, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_);
    MergeTreeReadInfo(const MergeTreeReadInfo & other);

    size_t origin_merge_tree_part_level = 0;

    /// If is virtual_row, block should not be empty.
    Block pk_block;
    ExpressionActionsPtr virtual_row_conversions;
};

size_t getPartLevelFromChunk(const Chunk & chunk);

bool isVirtualRow(const Chunk & chunk);

/// The values which the query filter fixes primary key columns to, by the position of the column in the key,
/// e.g. `a = 1` for `WHERE a = 1 ORDER BY b` over the key `(a, b)`.
using VirtualRowFixedKeys = VectorWithMemoryTracking<std::pair<size_t, Field>>;

/// Whether the index entry `pk_block` has these values; true if there are none. An entry with another value belongs
/// to a filtered-out row: its later key columns do not bound the rows after the filter, so it gives no virtual row.
bool virtualRowMatchesFixedKeys(const Block & pk_block, const VirtualRowFixedKeys & fixed_keys);

/// Returns the block the virtual row was built from, i.e. the sort columns it can announce exactly.
/// Every other column of `header` is filled with a type default and bounds nothing.
Block setVirtualRow(Chunk & chunk, const Block & header, bool apply_virtual_row_conversions);

}
