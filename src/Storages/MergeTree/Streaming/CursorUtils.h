#pragma once

#include <Core/Block.h>
#include <Core/Streaming/CursorTree_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <compare>
#include <map>

namespace DB
{

struct PartitionCursor
{
    Int64 block_number = -1;
    Int64 block_offset = -1;

    auto operator<=>(const PartitionCursor & other) const = default;
};
using MergeTreeCursor = std::map<String, PartitionCursor>;

/// Convert the generic cursor tree (partition_id → {block_number, block_offset})
/// produced by the parser into a flat per-partition map.
MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree);

/// Adds columns needed for streaming cursors recalculation.
Names extendWithAuxiliaryColumns(Names columns);

/// Build an ActionsDAG filter for a single partition's snapshot slice.
FilterDAGInfo buildPartitionFilter(
    const String & partition_id,
    const PartitionCursor & last_emitted_position,
    const Int64 & safe_block_number,
    const Block & input_header,
    const ContextPtr & context);

}
