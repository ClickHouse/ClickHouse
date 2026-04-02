#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct RangesInDataParts;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyReadReplacingFinalSource : public IProcessor
{
public:
    LazyReadReplacingFinalSource(
        StorageMetadataPtr metadata_snapshot_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeSettingsPtr data_settings_,
        const MergeTreeData & data_,
        PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
        RangesInDataPartsPtr ranges_,
        ContextPtr query_context_,
        FutureSetPtr future_set_);

    String getName() const override { return "LazyReadReplacingFinalSource"; }
    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    OutputPort * pipeline_output = nullptr;
    const StorageMetadataPtr metadata_snapshot;
    const MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    const StorageSnapshotPtr storage_snapshot;
    const MergeTreeSettingsPtr data_settings;
    const MergeTreeData & data;
    const PartitionIdToMaxBlockPtr max_block_numbers_to_read;
    const RangesInDataPartsPtr ranges;
    const ContextPtr query_context;
    const FutureSetPtr future_set;

    Processors processors;
};

}
