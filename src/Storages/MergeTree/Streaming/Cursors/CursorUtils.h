#pragma once

#include <Core/Block.h>
#include <Core/Streaming/CursorTree.h>

#include <Interpreters/Context_fwd.h>

#include <Storages/SelectQueryInfo.h>

#include <map>

namespace DB
{

struct PartitionCursor
{
    Int64 block_number = -1;
    Int64 block_offset = -1;

    auto operator<=>(const PartitionCursor & other) const = default;
};

/// Convert between the generic cursor tree (partition_id → {block_number, block_offset}) and the flat
/// per-partition map used while reading.
std::map<String, PartitionCursor> cursorTreeToMergeTreeCursor(const CursorTreeNodePtr & cursor);
CursorTreeNodePtr mergeTreeCursorToCursorTree(const std::map<String, PartitionCursor> & merge_tree_cursor);

/// Build an ActionsDAG filter for a single partition's read round slice.
FilterDAGInfo buildPartitionFilter(
    const String & partition_id,
    const PartitionCursor & last_emitted_position,
    const Int64 & safe_block_number,
    const Block & input_header,
    const ContextPtr & context);

}
