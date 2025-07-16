#include <Storages/MergeTree/Compaction/MergePredicates/ReplicatedMergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/ReplicatedMergeTreePartsCollector.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/Common.h>

namespace DB
{

namespace
{

MergeTreeDataPartsVector collectInitial(const MergeTreeData & data)
{
    return data.getDataPartsVectorForInternalUsage();
}

auto constructPreconditionsPredicate(const StoragePolicyPtr & storage_policy, const ReplicatedMergeTreeMergePredicatePtr & merge_pred)
{
    bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    auto predicate = [storage_policy, merge_pred, has_volumes_with_disabled_merges](const MergeTreeDataPartPtr & part) -> std::expected<void, PreformattedMessage>
    {
        if (has_volumes_with_disabled_merges && !part->shallParticipateInMerges(storage_policy))
            return std::unexpected(PreformattedMessage::create("Merges for part's {} volume are disabled", part->name));

        chassert(merge_pred);
        return merge_pred->canUsePartInMerges(part);
    };

    return predicate;
}

std::vector<MergeTreeDataPartsVector> splitPartsByPreconditions(
    MergeTreeDataPartsVector && parts,
    const StoragePolicyPtr & storage_policy, const ReplicatedMergeTreeMergePredicatePtr & merge_pred, LogSeriesLimiter & series_log)
{
    return splitRangeByPredicate(std::move(parts), constructPreconditionsPredicate(storage_policy, merge_pred), series_log);
}

std::expected<void, PreformattedMessage> checkAllParts(
    const MergeTreeDataPartsVector & parts,
    const StoragePolicyPtr & storage_policy, const ReplicatedMergeTreeMergePredicatePtr & merge_pred)
{
    return checkAllPartsSatisfyPredicate(parts, constructPreconditionsPredicate(storage_policy, merge_pred));
}

}

ReplicatedMergeTreePartsCollector::ReplicatedMergeTreePartsCollector(const StorageReplicatedMergeTree & storage_, ReplicatedMergeTreeMergePredicatePtr merge_pred_)
    : storage(storage_)
    , merge_pred(std::move(merge_pred_))
{
}

PartsRanges ReplicatedMergeTreePartsCollector::grabAllPossibleRanges(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint,
    LogSeriesLimiter & series_log) const
{
    auto parts = filterByPartitions(collectInitial(storage), partitions_hint);
    auto ranges = splitPartsByPreconditions(std::move(parts), storage_policy, merge_pred, series_log);
    return constructPartsRanges(std::move(ranges), metadata_snapshot, current_time);
}

std::expected<PartsRange, PreformattedMessage> ReplicatedMergeTreePartsCollector::grabAllPartsInsidePartition(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id) const
{
    auto parts = filterByPartitions(collectInitial(storage), PartitionIdsHint{partition_id});
    if (auto result = checkAllParts(parts, storage_policy, merge_pred); !result)
        return std::unexpected(std::move(result.error()));

    auto ranges = constructPartsRanges({std::move(parts)}, metadata_snapshot, current_time);
    chassert(ranges.size() == 1);

    return std::move(ranges.front());
}

}
