#pragma once

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/ReplicatedMergeTreeMergePredicate.h>

#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

class ReplicatedMergeTreePartsCollector final : public IPartsCollector
{
public:
    ReplicatedMergeTreePartsCollector(const StorageReplicatedMergeTree & storage_, ReplicatedMergeTreeMergePredicatePtr merge_pred_);
    ~ReplicatedMergeTreePartsCollector() override = default;

    PartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint,
        LogSeriesLimiter & series_log,
        bool ignore_prefer_not_to_merge) const override;

    std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::string & partition_id,
        bool ignore_prefer_not_to_merge
    ) const override;

private:
    const StorageReplicatedMergeTree & storage;
    const ReplicatedMergeTreeMergePredicatePtr merge_pred;
};

}
