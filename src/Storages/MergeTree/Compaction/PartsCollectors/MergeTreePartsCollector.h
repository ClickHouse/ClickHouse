#pragma once

#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

class MergeTreePartsCollector final : public IPartsCollector
{
public:
    MergeTreePartsCollector(StorageMergeTree & storage_, MergeTreeTransactionPtr tx_, MergeTreeMergePredicatePtr merge_pred_);
    ~MergeTreePartsCollector() override = default;

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
        bool ignore_prefer_not_to_merge) const override;

private:
    const StorageMergeTree & storage;
    const MergeTreeTransactionPtr tx;
    const MergeTreeMergePredicatePtr merge_pred;
};

}
