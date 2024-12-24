#include <optional>

#include <Common/ProfileEvents.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>

#include <Interpreters/MergeTreeTransaction.h>

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/VisiblePartsCollector.h>

namespace ProfileEvents
{
    extern const Event MergerMutatorsGetPartsForMergeElapsedMicroseconds;
}

namespace DB
{

namespace
{

std::string astToString(ASTPtr ast_ptr)
{
    if (!ast_ptr)
        return "";

    return queryToString(ast_ptr);
}

std::optional<PartProperties::GeneralTTLInfo> buildGeneralTTLInfo(StorageMetadataPtr metadata_snapshot, MergeTreeDataPartPtr part)
{
    if (!metadata_snapshot->hasAnyTTL())
        return std::nullopt;

    return PartProperties::GeneralTTLInfo{
        .has_any_non_finished_ttls = part->ttl_infos.hasAnyNonFinishedTTLs(),
        .part_min_ttl = part->ttl_infos.part_min_ttl,
        .part_max_ttl = part->ttl_infos.part_max_ttl,
    };
}

std::optional<PartProperties::RecompressTTLInfo> buildRecompressTTLInfo(StorageMetadataPtr metadata_snapshot, MergeTreeDataPartPtr part, time_t current_time)
{
    if (!metadata_snapshot->hasAnyRecompressionTTL())
        return std::nullopt;

    const auto & recompression_ttls = metadata_snapshot->getRecompressionTTLs();
    const auto ttl_description = selectTTLDescriptionForTTLInfos(recompression_ttls, part->ttl_infos.recompression_ttl, current_time, true);

    if (ttl_description)
    {
        const std::string next_codec = astToString(ttl_description->recompression_codec);
        const std::string current_codec = astToString(part->default_codec->getFullCodecDesc());

        return PartProperties::RecompressTTLInfo{
            .will_change_codec = (next_codec != current_codec),
            .next_recompress_ttl = part->ttl_infos.getMinimalMaxRecompressionTTL(),
        };
    }

    return std::nullopt;
}

std::set<std::string> getCalculatedProjectionNames(const MergeTreeDataPartPtr & part)
{
    std::set<std::string> projection_names;

    for (auto && [name, projection_part] : part->getProjectionParts())
        if (!projection_part->is_broken)
            projection_names.insert(name);

    return projection_names;
}

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    bool has_volumes_with_disabled_merges)
{
    return PartProperties{
        .name = part->name,
        .part_info = part->info,
        .uuid = part->uuid,
        .projection_names = getCalculatedProjectionNames(part),
        .shall_participate_in_merges = has_volumes_with_disabled_merges ? part->shallParticipateInMerges(storage_policy) : true,
        .all_ttl_calculated_if_any = part->checkAllTTLCalculated(metadata_snapshot),
        .size = part->getExistingBytesOnDisk(),
        .age = current_time - part->modification_time,
        .general_ttl_info = buildGeneralTTLInfo(metadata_snapshot, part),
        .recompression_ttl_info = buildRecompressTTLInfo(metadata_snapshot, part, current_time),
    };
}

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

}

MergeTreeDataPartsVector VisiblePartsCollector::collectInitial() const
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

MergeTreeDataPartsVector VisiblePartsCollector::filterByPartitions(MergeTreeDataPartsVector && parts, const std::optional<PartitionIdsHint> & partitions_hint) const
{
    if (!partitions_hint)
        return parts;

    Stopwatch partitions_filter_timer;

    std::erase_if(parts, [partitions_hint](const auto & part) { return !partitions_hint->contains(part->info.partition_id); });

    ProfileEvents::increment(ProfileEvents::MergerMutatorsGetPartsForMergeElapsedMicroseconds, partitions_filter_timer.elapsedMicroseconds());
    return parts;
}

std::vector<MergeTreeDataPartsVector> VisiblePartsCollector::filterByTxVisibility(MergeTreeDataPartsVector && parts) const
{
    if (tx == nullptr)
        return {std::move(parts)};

    auto build_next_range = [&](auto & parts_it)
    {
        MergeTreeDataPartsVector range;

        while (parts_it != parts.end())
        {
            MergeTreeDataPartPtr part = std::move(*parts_it++);

            /// Cannot merge parts if some of them are not visible in current snapshot
            /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
            if (part->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return range;

            /// Do not try to merge parts that are locked for removal (merge will probably fail)
            if (part->version.isRemovalTIDLocked())
                return range;

            /// Otherwise include part to possible ranges
            range.push_back(std::move(part));
        }

        return range;
    };

    std::vector<MergeTreeDataPartsVector> ranges;
    for (auto part_it = parts.begin(); part_it != parts.end(); ++part_it)
        if (auto next_range = build_next_range(part_it); !next_range.empty())
            ranges.push_back(std::move(next_range));

    return ranges;
}

VisiblePartsCollector::VisiblePartsCollector(const MergeTreeData & data_, const MergeTreeTransactionPtr & tx_)
    : data(data_)
    , tx(tx_)
{
}

PartsRanges VisiblePartsCollector::collectPartsToUse(
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint) const
{
    auto parts = filterByPartitions(collectInitial(), partitions_hint);
    auto ranges = filterByTxVisibility(std::move(parts));
    return constructProperties(std::move(ranges), metadata_snapshot, storage_policy, current_time);
}

}
