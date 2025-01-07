#include <Storages/MergeTree/Compaction/PartsCollectors/VisiblePartsCollector.h>

#include <Interpreters/MergeTreeTransaction.h>

namespace ProfileEvents
{
    extern const Event MergerMutatorsGetPartsForMergeElapsedMicroseconds;
}

namespace DB
{

namespace
{

PartsRanges constructProperties(
    std::vector<MergeTreeDataPartsVector> && ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time)
{
    const bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    PartsRanges properties_ranges;
    properties_ranges.reserve(ranges.size());

    for (const auto & range : ranges)
    {
        PartsRange properties_range;
        properties_ranges.reserve(range.size());

        for (const auto & part : range)
            properties_range.push_back(buildPartProperties(part, metadata_snapshot, storage_policy, current_time, has_volumes_with_disabled_merges));

        properties_ranges.push_back(std::move(properties_range));
    }

    return properties_ranges;
}

MergeTreeDataPartsVector collectInitial(const MergeTreeData & data, const MergeTreeTransactionPtr & tx)
{
    if (!tx)
    {
        /// Simply get all active parts
        return data.getDataPartsVectorForInternalUsage();
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
        active_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, lock);
        outdated_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Outdated}, lock);
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

MergeTreeDataPartsVector filterByPartitions(MergeTreeDataPartsVector && parts, const std::optional<PartitionIdsHint> & partitions_to_keep)
{
    if (!partitions_to_keep)
        return parts;

    Stopwatch partitions_filter_timer;

    std::erase_if(parts, [&partitions_to_keep](const auto & part) { return !partitions_to_keep->contains(part->info.partition_id); });

    ProfileEvents::increment(ProfileEvents::MergerMutatorsGetPartsForMergeElapsedMicroseconds, partitions_filter_timer.elapsedMicroseconds());
    return parts;
}

bool canUsePart(const MergeTreeDataPartPtr & part, const MergeTreeTransactionPtr & tx)
{
    /// Cannot merge parts if some of them are not visible in current snapshot
    /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
    if (!part->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
        return false;

    /// Do not try to merge parts that are locked for removal (merge will probably fail)
    if (part->version.isRemovalTIDLocked())
        return false;

    return true;
}

std::vector<MergeTreeDataPartsVector> splitByInvisibleParts(MergeTreeDataPartsVector && parts, const MergeTreeTransactionPtr & tx)
{
    if (!tx)
    {
        if (parts.empty())
            return {};

        return {std::move(parts)};
    }

    auto build_next_range = [&](auto & parts_it)
    {
        MergeTreeDataPartsVector range;

        while (parts_it != parts.end())
        {
            MergeTreeDataPartPtr part = std::move(*parts_it++);

            if (!canUsePart(part, tx))
                return range;

            /// Otherwise include part to possible ranges
            range.push_back(std::move(part));
        }

        return range;
    };

    std::vector<MergeTreeDataPartsVector> ranges;
    for (auto part_it = parts.begin(); part_it != parts.end();)
        if (auto next_range = build_next_range(part_it); !next_range.empty())
            ranges.push_back(std::move(next_range));

    return ranges;
}

std::expected<void, PreformattedMessage> checkAllPartsVisible(const MergeTreeDataPartsVector & parts, const MergeTreeTransactionPtr & tx)
{
    if (!tx)
        return {};

    for (const auto & part : parts)
        if (!canUsePart(part, tx))
            return std::unexpected(PreformattedMessage::create("Part {} is not visible in transaction {}", part->name, tx->dumpDescription()));

    return {};
}

}

VisiblePartsCollector::VisiblePartsCollector(const MergeTreeData & data_, MergeTreeTransactionPtr tx_)
    : data(data_)
    , tx(std::move(tx_))
{
}

PartsRanges VisiblePartsCollector::grabAllPossibleRanges(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint) const
{
    auto parts = filterByPartitions(collectInitial(data, tx), partitions_hint);
    auto ranges = splitByInvisibleParts(std::move(parts), tx);
    return constructProperties(std::move(ranges), metadata_snapshot, storage_policy, current_time);
}

std::expected<PartsRange, PreformattedMessage> VisiblePartsCollector::grabAllPartsInsidePartition(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id) const
{
    auto parts = filterByPartitions(collectInitial(data, tx), PartitionIdsHint{partition_id});
    if (auto result = checkAllPartsVisible(parts, tx); !result)
        return std::unexpected(std::move(result.error()));

    auto ranges = constructProperties({std::move(parts)}, metadata_snapshot, storage_policy, current_time);
    assert(ranges.size() == 1);

    return std::move(ranges.front());
}

}
