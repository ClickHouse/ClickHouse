#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/IProcessor.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Filters the query applies BEFORE the FINAL merge, so they constrain which rows participate in
/// deduplication. The winner-selection read must apply them too: otherwise its `argMax` picks a row
/// the filter excludes, and the key yields no row at all instead of the highest-versioned row that
/// does satisfy the filter.
///
/// Owned clones, never the reading step's own filters: `ReadFromMergeTree::getQueryInfo` shares
/// those with the false branch's FINAL read.
struct LazyFinalPreFinalFilters
{
    FilterDAGInfoPtr row_level_filter;
    PrewhereInfoPtr prewhere_info;
    /// Storage columns the filters consume, read in addition to the aggregation's own columns.
    Names extra_columns;

    bool empty() const { return !row_level_filter && !prewhere_info; }
};

/// Waits for the set to be built, then:
/// 1. Creates a ReadFromMergeTree step with an IN-set filter
/// 2. Runs index analysis to prune parts/granules
/// 3. Checks if enough marks were filtered (`min_filtered_ratio`)
/// 4. If OK, stores the step in shared state and signals the optimized path
/// 5. Otherwise, signals fallback
class LazyFinalKeyAnalysisTransform final : public IProcessor
{
public:
    LazyFinalKeyAnalysisTransform(
        FutureSetPtr future_set_,
        LazyFinalSharedStatePtr shared_state_,
        StorageMetadataPtr metadata_snapshot_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeSettingsPtr data_settings_,
        const MergeTreeData & data_,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
        RangesInDataPartsPtr ranges_,
        ContextPtr query_context_,
        float min_filtered_ratio_,
        LazyFinalPreFinalFilters pre_final_filters_);

    String getName() const override { return "LazyFinalKeyAnalysisTransform"; }
    Status prepare() override;
    void work() override;

    /// Build a ReadFromMergeTree step for the lazy FINAL path (without IN-set filter).
    /// Used by LazyReadReplacingFinalStep for EXPLAIN.
    static std::unique_ptr<ReadFromMergeTree> buildReadingStep(
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
        const StorageSnapshotPtr & storage_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const MergeTreeData & data,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read,
        RangesInDataPartsPtr ranges,
        ContextPtr query_context,
        const LazyFinalPreFinalFilters & pre_final_filters);

private:
    FutureSetPtr future_set;
    LazyFinalSharedStatePtr shared_state;
    StorageMetadataPtr metadata_snapshot;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr data_settings;
    const MergeTreeData & data;
    PartitionIdToMaxBlockPtr max_block_numbers_to_read;
    RangesInDataPartsPtr ranges;
    ContextPtr query_context;
    float min_filtered_ratio;
    LazyFinalPreFinalFilters pre_final_filters;

    LoggerPtr log;
    bool is_done = false;
    bool should_signal = false;
};

}
