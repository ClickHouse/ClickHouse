#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Core/SortDescription.h>

namespace DB
{

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// Result of FINAL BY validation.
/// Produced by `validateFinalBy` and consumed by the query pipeline builder
/// in `ReadFromMergeTree::spreadMarkRangesAmongStreamsFinal`.
struct FinalByValidationResult
{
    ActionsDAG dag;
    std::vector<SortColumnDescription> sort_columns;
    bool has_non_identity = false;
};

/// Validate FINAL BY expressions against the sorting key.
///
/// The ActionsDAG must be pre-built by the Planner from resolved query tree nodes.
/// The first `final_by_count` outputs of the DAG correspond to the FINAL BY
/// expressions (in order); remaining outputs are pass-through INPUT nodes.
///
/// Checks:
///   1. Engine must be AggregatingMergeTree or SummingMergeTree.
///   2. FINAL BY expression count must exactly equal the sorting key column count.
///   3. Each FINAL BY expression must be a monotonic, same-direction function of
///      the corresponding sorting key column (identity is accepted).
///   4. No direction reversal is allowed.
///
/// Returns the validated result (DAG, sort description, identity flag).
/// Throws `BAD_ARGUMENTS` on any validation failure.
FinalByValidationResult validateFinalBy(
    ActionsDAG final_by_dag,
    size_t final_by_count,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::MergingParams::Mode merging_mode);

}
