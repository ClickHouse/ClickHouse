#pragma once

#include <cstddef>
#include <string>

namespace DB
{

/// Analyze a Keeper snapshot file (or every snapshot in a directory) by streaming it
/// from disk with O(1) memory and printing summary information and statistics.
///
/// If `with_node_stats` is set, the set of all paths is additionally kept in memory
/// (O(number of nodes)) to compute the biggest subtrees - this is the only part that
/// is not O(1) memory.
///
/// `sample_size` random nodes and the `top_children_limit` nodes with the most children are
/// printed too (0 disables).
///
/// Throws if `snapshot_path` is a file that can't be analyzed, or a directory in which some
/// snapshot can't be analyzed (the remaining snapshots in the directory are still analyzed).
void analyzeSnapshot(
    const std::string & snapshot_path,
    bool with_node_stats,
    size_t subtrees_limit,
    size_t sample_size,
    size_t top_children_limit);

}
