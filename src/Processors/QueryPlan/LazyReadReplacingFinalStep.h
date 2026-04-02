#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyReadReplacingFinalStep : public ISourceStep
{
public:
    LazyReadReplacingFinalStep(
        StorageMetadataPtr metadata_snapshot_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeSettingsPtr data_settings_,
        const MergeTreeData & data_,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
        RangesInDataPartsPtr ranges_,
        ContextPtr query_context_,
        FutureSetPtr future_set_);

    String getName() const override { return "LazyReadReplacingFinal"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    StorageMetadataPtr metadata_snapshot;
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr data_settings;
    const MergeTreeData & data;
    PartitionIdToMaxBlockPtr max_block_numbers_to_read;
    RangesInDataPartsPtr ranges;
    ContextPtr query_context;
    FutureSetPtr future_set;
};

}
