#pragma once

#include <Core/Streaming/CursorTree_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <compare>
#include <map>
#include <optional>

namespace DB
{

struct PartitionCursor
{
    Int64 block_number = -1;
    Int64 block_offset = -1;

    auto operator<=>(const PartitionCursor & other) const = default;
};

/// Per-partition lower bound (exclusive) for commit-order streaming reads.
using MergeTreeCursor = std::map<String, PartitionCursor>;

/// Convert the generic cursor tree (partition_id → {block_number, block_offset})
/// produced by the parser into a flat per-partition map.
MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree);

/// Build an `ActionsDAG`-backed filter from the cursor:
///   (_partition_id = 'p' AND (_block_number, _block_offset) > (bn, bo)) OR …
/// Returns nullopt for an empty cursor.
std::optional<FilterDAGInfo> convertCursorToFilter(const MergeTreeCursor & cursor, SelectQueryInfo & query_info);

}
