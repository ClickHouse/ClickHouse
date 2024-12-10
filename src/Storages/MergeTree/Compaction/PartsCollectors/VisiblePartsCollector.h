#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>

namespace DB
{

class VisiblePartsCollector : public IPartsCollector
{
    PartProperties buildPartProperties(MergeTreeData::DataPartPtr part) const;

    MergeTreeData::DataPartsVector collectInitial(const MergeTreeTransactionPtr & txn) const;
    MergeTreeData::DataPartsVector filterByPartitions(MergeTreeData::DataPartsVector && parts, const PartitionIdsHint * partitions_hint) const;
    PartsRanges filterByTxVisibility(MergeTreeData::DataPartsVector && parts, const MergeTreeTransactionPtr & txn) const;

public:
    explicit VisiblePartsCollector(const MergeTreeData & data_);
    ~VisiblePartsCollector() override = default;

    PartsRanges collectPartsToUse(const MergeTreeTransactionPtr & txn, const PartitionIdsHint * partitions_hint) const override;

private:
    const MergeTreeData & data;

    const time_t current_time;
    const StorageMetadataPtr metadata_snapshot;
    const StoragePolicyPtr storage_policy;
    const bool has_volumes_with_disabled_merges;
};

}
