#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyFinalKeyAnalysisStep : public ITransformingStep
{
public:
    LazyFinalKeyAnalysisStep(
        SharedHeader input_header_,
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
        float min_filtered_ratio_);

    String getName() const override { return "LazyFinalKeyAnalysis"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    /// Build a ReadFromMergeTree step without the IN-set filter (for EXPLAIN).
    std::unique_ptr<ReadFromMergeTree> buildReadingStep() const;

private:
    void updateOutputHeader() override {}

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
};

}
