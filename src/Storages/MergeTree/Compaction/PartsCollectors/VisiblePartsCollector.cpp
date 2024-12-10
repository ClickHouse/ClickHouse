#include <optional>

#include <Common/ProfileEvents.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>

#include <Interpreters/MergeTreeTransaction.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/VisiblePartsCollector.h>

namespace ProfileEvents
{
    extern const Event MergerMutatorsGetPartsForMergeElapsedMicroseconds;
}

namespace DB
{

static std::string astToString(ASTPtr ast_ptr)
{
    if (!ast_ptr)
        return "";

    return queryToString(ast_ptr);
}

static std::optional<PartProperties::GeneralTTLInfo> buildGeneralTTLInfo(StorageMetadataPtr metadata_snapshot, MergeTreeData::DataPartPtr part)
{
    if (!metadata_snapshot->hasAnyTTL())
        return std::nullopt;

    return PartProperties::GeneralTTLInfo
    {
        .has_any_non_finished_ttls = part->ttl_infos.hasAnyNonFinishedTTLs(),
        .part_min_ttl = part->ttl_infos.part_min_ttl,
        .part_max_ttl = part->ttl_infos.part_max_ttl,
    };
}

static std::optional<PartProperties::RecompressTTLInfo> buildRecompressTTLInfo(StorageMetadataPtr metadata_snapshot, MergeTreeData::DataPartPtr part, time_t current_time)
{
    if (!metadata_snapshot->hasAnyRecompressionTTL())
        return std::nullopt;

    time_t next_max_recompress_border = part->ttl_infos.getMinimalMaxRecompressionTTL();
    std::optional<std::string> next_recompression_codec;

    const auto & recompression_ttls = metadata_snapshot->getRecompressionTTLs();
    auto ttl_description = selectTTLDescriptionForTTLInfos(recompression_ttls, part->ttl_infos.recompression_ttl, current_time, true);

    if (ttl_description)
        next_recompression_codec = astToString(ttl_description->recompression_codec);

    return PartProperties::RecompressTTLInfo
    {
        .next_max_recompress_border = next_max_recompress_border,
        .next_recompression_codec = std::move(next_recompression_codec),
    };
}

PartProperties VisiblePartsCollector::buildPartProperties(MergeTreeData::DataPartPtr part) const
{
    return PartProperties
    {
        .part_info = part->info,
        .part_compression_codec = astToString(part->default_codec->getFullCodecDesc()),
        .shall_participate_in_merges = has_volumes_with_disabled_merges ? part->shallParticipateInMerges(storage_policy) : true,
        .size = part->getExistingBytesOnDisk(),
        .age = current_time - part->modification_time,
        .general_ttl_info = buildGeneralTTLInfo(metadata_snapshot, part),
        .recompression_ttl_info = buildRecompressTTLInfo(metadata_snapshot, part, current_time),
    };
}

MergeTreeData::DataPartsVector VisiblePartsCollector::collectInitial(const MergeTreeTransactionPtr & tx) const
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
    MergeTreeData::DataPartsVector active_parts;
    MergeTreeData::DataPartsVector outdated_parts;

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
    auto remove_pred = [&](const MergeTreeData::DataPartPtr & part)
    {
        return active_parts_set.getContainingPart(part->info) != part->name;
    };

    std::erase_if(active_parts, remove_pred);
    std::erase_if(outdated_parts, remove_pred);

    MergeTreeData::DataPartsVector data_parts;
    std::merge(
        active_parts.begin(),
        active_parts.end(),
        outdated_parts.begin(),
        outdated_parts.end(),
        std::back_inserter(data_parts),
        MergeTreeData::LessDataPart());

    return data_parts;
}

MergeTreeData::DataPartsVector
VisiblePartsCollector::filterByPartitions(MergeTreeData::DataPartsVector && parts, const PartitionIdsHint * partitions_hint) const
{
    if (!partitions_hint)
        return parts;

    Stopwatch partitions_filter_timer;

    std::erase_if(parts, [partitions_hint](const auto & part) { return !partitions_hint->contains(part->info.partition_id); });

    ProfileEvents::increment(
        ProfileEvents::MergerMutatorsGetPartsForMergeElapsedMicroseconds, partitions_filter_timer.elapsedMicroseconds());

    return parts;
}

PartsRanges VisiblePartsCollector::filterByTxVisibility(MergeTreeData::DataPartsVector && parts, const MergeTreeTransactionPtr & tx) const
{
    using PartsIt = MergeTreeData::DataPartsVector::iterator;

    auto build_next_range = [&](PartsIt & parts_it)
    {
        PartsRange range;

        while (parts_it != parts.end())
        {
            MergeTreeData::DataPartPtr part = std::move(*parts_it++);

            /// Cannot merge parts if some of them are not visible in current snapshot
            /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
            if (part->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return range;

            /// Do not try to merge parts that are locked for removal (merge will probably fail)
            if (part->version.isRemovalTIDLocked())
                return range;

            /// Otherwise include part to possible ranges
            range.push_back(buildPartProperties(std::move(part)));
        }

        return range;
    };

    PartsRanges ranges;
    for (auto part_it = parts.begin(); part_it != parts.end(); ++part_it)
        ranges.push_back(build_next_range(part_it));

    return ranges;
}

VisiblePartsCollector::VisiblePartsCollector(const MergeTreeData & data_)
    : data(data_)
    , current_time(std::time(nullptr))
    , metadata_snapshot(data.getInMemoryMetadataPtr())
    , storage_policy(data.getStoragePolicy())
    , has_volumes_with_disabled_merges(storage_policy->hasAnyVolumeWithDisabledMerges())
{
}

PartsRanges VisiblePartsCollector::collectPartsToUse(const MergeTreeTransactionPtr & tx, const PartitionIdsHint * partitions_hint) const
{
    auto parts = collectInitial(tx);
    parts = filterByPartitions(std::move(parts), partitions_hint);
    return filterByTxVisibility(std::move(parts), tx);
}

}
