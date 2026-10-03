#pragma once

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

class MergeTreePartsCollector final : public IPartsCollector
{
public:
    /// Parts whose `min_block` is above `last_allocated_block` are not collected.
    MergeTreePartsCollector(
        StorageMergeTree & storage_, MergeTreeTransactionPtr tx_, MergeTreeMergePredicatePtr merge_pred_, Int64 last_allocated_block_);
    ~MergeTreePartsCollector() override = default;

    CollectedPartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint,
        LogSeriesLimiter & series_log) const override;

    std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::string & partition_id) const override;

private:
    const StorageMergeTree & storage;
    const MergeTreeTransactionPtr tx;
    const MergeTreeMergePredicatePtr merge_pred;
    const Int64 last_allocated_block;
};

}
