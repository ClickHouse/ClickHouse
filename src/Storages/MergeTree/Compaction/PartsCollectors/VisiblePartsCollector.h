#pragma once

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class VisiblePartsCollector : public IPartsCollector
{
public:
    explicit VisiblePartsCollector(const MergeTreeData & data_, MergeTreeTransactionPtr tx_);
    ~VisiblePartsCollector() override = default;

    PartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint) const override;

    tl::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::string & partition_id) const override;

private:
    const MergeTreeData & data;
    MergeTreeTransactionPtr tx;
};

}
