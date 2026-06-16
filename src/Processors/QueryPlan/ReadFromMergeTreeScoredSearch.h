#pragma once

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <Storages/SelectQueryInfo.h>

#include <optional>

namespace DB
{

class StorageMergeTreeScoredSearchBase;

/// Read step for row-producing scorers (vector, BM25, hybrid).
/// Emits the narrow `(__global_row_index UInt64, _score Float32)` chunks
/// that the lazy column-proxy join widens back to source columns.
/// `applyFilters` captures the WHERE clause for the bitmap subquery.
/// `initializePipeline` creates the scorer and builds the top-K pipeline.
class ReadFromMergeTreeScoredSearch final : public SourceStepWithFilter
{
public:
    ReadFromMergeTreeScoredSearch(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block_,
        std::shared_ptr<StorageMergeTreeScoredSearchBase> storage_,
        RangesInDataPartsPtr ranges_in_data_parts_,
        LazyBitmapSubqueryStatePtr bitmap_state_,
        StorageSnapshotPtr source_storage_snapshot_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        std::optional<FilterDAGInfo> row_policy_,
        PreparedSetsPtr row_policy_sets_,
        size_t num_streams_);

    std::string getName() const override { return "ReadFromMergeTreeScoredSearch"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<StorageMergeTreeScoredSearchBase> storage;
    RangesInDataPartsPtr ranges_in_data_parts;
    /// State shared with DelayedCreatingBitmapsStep.
    LazyBitmapSubqueryStatePtr bitmap_state;
    StorageSnapshotPtr source_storage_snapshot;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    /// Row policy of the source table; merged into the bitmap prefilter.
    std::optional<FilterDAGInfo> row_policy;
    /// Sets of the `IN (subquery)` clauses of the row policy.
    PreparedSetsPtr row_policy_sets;
    /// Bounds the number of scorer worker sources.
    size_t num_streams;

    ExpressionActionsPtr virtual_columns_filter;
    bool applied_filters = false;
    std::shared_ptr<IScorer> scorer_owned;
};

}
