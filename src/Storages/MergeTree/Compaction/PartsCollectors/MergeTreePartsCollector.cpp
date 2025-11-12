#include <Storages/MergeTree/Compaction/PartsCollectors/MergeTreePartsCollector.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/Common.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Interpreters/MergeTreeTransaction.h>

namespace DB
{

namespace
{

MergeTreeDataPartsVector collectInitial(const MergeTreeData & data, const MergeTreeTransactionPtr & tx)
{
    MergeTreeData::DataPartsKinds affordable_kinds{MergeTreeData::DataPartKind::Regular, MergeTreeData::DataPartKind::Patch};

    if (!tx)
    {
        /// Simply get all active parts
        return data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, affordable_kinds);
    }

    /// Merge predicate (for simple MergeTree) allows to merge two parts only if both parts are visible for merge transaction.
    /// So at the first glance we could just get all active parts.
    /// Active parts include uncommitted parts, but it's ok and merge predicate handles it.
    /// However, it's possible that some transaction is trying to remove a part in the middle, for example, all_2_2_0.
    /// If parts all_1_1_0 and all_3_3_0 are active and visible for merge transaction, then we would try to merge them.
    /// But it's wrong, because all_2_2_0 may become active again if transaction will roll back.
    /// That's why we must include some outdated parts into `data_part`, more precisely, such parts that removal is not committed.
    MergeTreeDataPartsVector active_parts;
    MergeTreeDataPartsVector outdated_parts;

    {
        auto lock = data.lockParts();
        active_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, affordable_kinds, lock);
        outdated_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Outdated}, affordable_kinds, lock);
    }

    ActiveDataPartSet active_parts_set{data.format_version};
    for (const auto & part : active_parts)
        active_parts_set.add(part->name);

    for (const auto & part : outdated_parts)
    {
        /// We don't need rolled back parts.
        /// NOTE When rolling back a transaction we set creation_csn to RolledBackCSN at first
        /// and then remove part from working set, so there's no race condition
        if (part->version.creation_csn == Tx::RolledBackCSN)
            continue;

        /// We don't need parts that are finally removed.
        /// NOTE There's a minor race condition: we may get UnknownCSN if a transaction has been just committed concurrently.
        /// But it's not a problem if we will add such part to `data_parts`.
        if (part->version.removal_csn != Tx::UnknownCSN)
            continue;

        active_parts_set.add(part->name);
    }

    /// Restore "active" parts set from selected active and outdated parts
    auto remove_pred = [&](const MergeTreeDataPartPtr & part) { return active_parts_set.getContainingPart(part->info) != part->name; };

    std::erase_if(active_parts, remove_pred);
    std::erase_if(outdated_parts, remove_pred);

    MergeTreeDataPartsVector data_parts;
    std::merge(
        active_parts.begin(),
        active_parts.end(),
        outdated_parts.begin(),
        outdated_parts.end(),
        std::back_inserter(data_parts),
        MergeTreeData::LessDataPart());

    return data_parts;
}

auto constructPreconditionsPredicate(const StoragePolicyPtr & storage_policy, const MergeTreeTransactionPtr & tx, const MergeTreeMergePredicatePtr & merge_pred, bool ignore_prefer_not_to_merge)
{
    bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    auto predicate = [storage_policy, tx, merge_pred, has_volumes_with_disabled_merges, ignore_prefer_not_to_merge](const MergeTreeDataPartPtr & part) -> std::expected<void, PreformattedMessage>
    {
        if (tx)
        {
            /// Cannot merge parts if some of them are not visible in current snapshot
            /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
            if (!part->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return std::unexpected(PreformattedMessage::create("Part {} is not visible in transaction {}", part->name, tx->dumpDescription()));

            /// Do not try to merge parts that are locked for removal (merge will probably fail)
            if (part->version.isRemovalTIDLocked())
                return std::unexpected(PreformattedMessage::create("Part {} is locked for removal", part->name));
        }

        if (!ignore_prefer_not_to_merge && has_volumes_with_disabled_merges && !part->shallParticipateInMerges(storage_policy))
            return std::unexpected(PreformattedMessage::create("Merges for part's {} volume are disabled", part->name));

        chassert(merge_pred);
        return merge_pred->canUsePartInMerges(part);
    };

    return predicate;
}

std::vector<MergeTreeDataPartsVector> splitPartsByPreconditions(
    MergeTreeDataPartsVector && parts,
    const StoragePolicyPtr & storage_policy, const MergeTreeTransactionPtr & tx, const MergeTreeMergePredicatePtr & merge_pred, bool ignore_prefer_not_to_merge, LogSeriesLimiter & series_log)
{
    return splitRangeByPredicate(std::move(parts), constructPreconditionsPredicate(storage_policy, tx, merge_pred, ignore_prefer_not_to_merge), series_log);
}

std::expected<void, PreformattedMessage> checkAllParts(
    const MergeTreeDataPartsVector & parts,
    const StoragePolicyPtr & storage_policy, const MergeTreeTransactionPtr & tx, const MergeTreeMergePredicatePtr & merge_pred, bool ignore_prefer_not_to_merge)
{
    return checkAllPartsSatisfyPredicate(parts, constructPreconditionsPredicate(storage_policy, tx, merge_pred, ignore_prefer_not_to_merge));
}

}

MergeTreePartsCollector::MergeTreePartsCollector(StorageMergeTree & storage_, MergeTreeTransactionPtr tx_, MergeTreeMergePredicatePtr merge_pred_)
    : storage(storage_)
    , tx(std::move(tx_))
    , merge_pred(std::move(merge_pred_))
{
}

PartsRanges MergeTreePartsCollector::grabAllPossibleRanges(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint,
    LogSeriesLimiter & series_log,
    bool ignore_prefer_not_to_merge) const
{
    auto parts = filterByPartitions(collectInitial(storage, tx), partitions_hint);
    auto ranges = splitPartsByPreconditions(std::move(parts), storage_policy, tx, merge_pred, ignore_prefer_not_to_merge, series_log);
    return constructPartsRanges(std::move(ranges), metadata_snapshot, current_time);
}

std::expected<PartsRange, PreformattedMessage> MergeTreePartsCollector::grabAllPartsInsidePartition(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id,
    bool ignore_prefer_not_to_merge) const
{
    auto parts = filterByPartitions(collectInitial(storage, tx), PartitionIdsHint{partition_id});
    if (auto result = checkAllParts(parts, storage_policy, tx, merge_pred, ignore_prefer_not_to_merge); !result)
        return std::unexpected(std::move(result.error()));

    auto ranges = constructPartsRanges({std::move(parts)}, metadata_snapshot, current_time);
    chassert(ranges.size() == 1);

    return std::move(ranges.front());
}

}
