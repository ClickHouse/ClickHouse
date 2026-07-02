#include <Storages/MergeTree/Compaction/MergePredicates/ReplicatedMergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/ReplicatedMergeTreePartsCollector.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/Common.h>

#include <algorithm>

namespace DB
{

namespace
{

MergeTreeDataPartsVector collectInitial(const MergeTreeData & data)
{
    using Kind = MergeTreeData::DataPartKind;
    MergeTreeData::DataPartsKinds affordable_kinds{Kind::Regular, Kind::Patch};
    return data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, affordable_kinds);
}

/// Collect active parts only from the hinted partitions instead of scanning every active part and
/// filtering afterwards. Equivalent to filterByPartitions(collectInitial(data), hint): a per-partition
/// lookup returns the same parts (patch partitions included) that the filter would keep. Sorting by
/// part info keeps each partition's parts contiguous, which is all downstream range splitting needs.
MergeTreeDataPartsVector collectInitial(const MergeTreeData & data, const std::optional<PartitionIdsHint> & partitions_hint)
{
    if (!partitions_hint)
        return collectInitial(data);

    MergeTreeDataPartsVector parts;
    auto lock = data.readLockParts();

    for (const auto & partition_id : *partitions_hint)
    {
        auto partition_parts = data.getDataPartsVectorInPartitionForInternalUsage(MergeTreeData::DataPartState::Active, partition_id, lock);
        parts.insert(parts.end(), partition_parts.begin(), partition_parts.end());
    }

    std::sort(parts.begin(), parts.end(), MergeTreeData::LessDataPart());
    return parts;
}

auto constructPreconditionsPredicate(const StoragePolicyPtr & storage_policy, const ReplicatedMergeTreeMergePredicatePtr & merge_pred)
{
    auto predicate = [storage_policy, merge_pred](const MergeTreeDataPartPtr & part) -> std::expected<void, PreformattedMessage>
    {
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

CollectedPartsRanges ReplicatedMergeTreePartsCollector::grabAllPossibleRanges(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint,
    LogSeriesLimiter & series_log) const
{
    auto parts = collectInitial(storage, partitions_hint);
    auto partitions_stats = calculateStatisticsForParts(parts, current_time);
    auto ranges = splitPartsByPreconditions(std::move(parts), storage_policy, merge_pred, series_log);
    return {constructPartsRanges(std::move(ranges), metadata_snapshot, storage_policy, current_time), std::move(partitions_stats)};
}

std::expected<PartsRange, PreformattedMessage> ReplicatedMergeTreePartsCollector::grabAllPartsInsidePartition(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id) const
{
    auto parts = collectInitial(storage, PartitionIdsHint{partition_id});
    if (auto result = checkAllParts(parts, storage_policy, merge_pred); !result)
        return std::unexpected(std::move(result.error()));

    auto ranges = constructPartsRanges({std::move(parts)}, metadata_snapshot, storage_policy, current_time);
    chassert(ranges.size() == 1);

    return std::move(ranges.front());
}

}
