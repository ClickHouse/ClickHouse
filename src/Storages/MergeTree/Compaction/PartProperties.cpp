#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexClearTTL.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsBool ttl_clear_index_merges;
}

namespace
{

std::string astToString(ASTPtr ast_ptr)
{
    if (!ast_ptr)
        return "";

    return ast_ptr->formatWithSecretsOneLine();
}

std::optional<PartProperties::GeneralTTLInfo> buildGeneralTTLInfo(StorageMetadataPtr metadata_snapshot, MergeTreeDataPartPtr part)
{
    if (!metadata_snapshot->hasAnyTTL())
        return std::nullopt;

    return PartProperties::GeneralTTLInfo{
        .has_any_non_finished_ttls = part->ttl_infos.hasAnyNonFinishedTTLs(),
        .has_any_non_finished_row_ttls = part->ttl_infos.hasAnyNonFinishedRowTTLs(),
        .has_any_non_finished_column_ttls = part->ttl_infos.hasAnyNonFinishedColumnTTLs(),
        .part_min_ttl = part->ttl_infos.part_min_ttl,
        .part_max_ttl = part->ttl_infos.part_max_ttl,
        .column_min_ttl = part->ttl_infos.getMinimalNonFinishedColumnTTL(),
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
        /// If the part's own default codec could not be recovered exactly (see
        /// `IMergeTreeDataPart::default_codec_is_approximate`), the comparison below cannot be trusted
        /// either way: treat the codec as unknown and always let the merge selector reconsider the
        /// part, rather than risk a wrong guess suppressing a recompression that is still needed.
        if (part->default_codec_is_approximate)
            return PartProperties::RecompressTTLInfo{
                .will_change_codec = true,
                .next_recompress_ttl = part->ttl_infos.getMinimalMaxRecompressionTTL(),
            };

        /// FIXME: Implement in other way -- not string comparison
        const std::string next_codec = astToString(ttl_description->recompression_codec);
        const std::string current_codec = astToString(part->default_codec->getFullCodecDescription());

        return PartProperties::RecompressTTLInfo{
            .will_change_codec = (next_codec != current_codec),
            .next_recompress_ttl = part->ttl_infos.getMinimalMaxRecompressionTTL(),
        };
    }

    return std::nullopt;
}

time_t buildNextIndexClearTTL(StorageMetadataPtr metadata_snapshot, MergeTreeDataPartPtr part, time_t current_time)
{
    if (!metadata_snapshot->hasAnyIndexClearTTL())
        return 0;

    time_t next_index_clear_ttl = 0;
    for (const auto & [index_name, ttl] : getIndexesWithExpiredClearTTL(*metadata_snapshot, part->ttl_infos, current_time))
        if ((!next_index_clear_ttl || ttl < next_index_clear_ttl) && partHasSkipIndexFiles(*part, index_name, *metadata_snapshot))
            next_index_clear_ttl = ttl;

    return next_index_clear_ttl;
}

/// A `TTLClearIndex` merge reserves space on the part's own disk. A part on a full disk would be selected on every pass
/// and fail, which blocks the table's other merges.
bool hasSpaceForIndexClear(const MergeTreeDataPartPtr & part)
{
    /// `MergeTreeData` never reserves less than 1 MiB.
    const auto * disk_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(&part->getDataPartStorage());
    const auto unreserved = disk_storage ? disk_storage->getDisk()->getUnreservedSpace() : std::nullopt;
    return !unreserved || *unreserved >= std::max<UInt64>(estimateDiskSpaceForIndexClear(part), 1024 * 1024);
}

/// Whether a `TTLClearIndex` merge may take the part on this replica.
bool canClearIndexes(const MergeTreeDataPartPtr & part)
{
    /// The new part is one level higher than the source part. For engines that merge rows, `FINAL` and
    /// `OPTIMIZE` treat a part above level 0 as already merged, which a level-0 part may not be.
    return part->getDataPartStorage().getType() == MergeTreeDataPartStorageType::Full
        && part->uuid == UUIDHelpers::Nil
        && !(*part->storage.getSettings())[MergeTreeSetting::assign_part_uuids]
        && (part->info.level > 0 || part->storage.merging_params.mode == MergeTreeData::MergingParams::Ordinary)
        && canHardlinkFilesForIndexClear(part)
        && hasSpaceForIndexClear(part);
}

std::set<std::string> getCalculatedProjectionNames(const MergeTreeDataPartPtr & part)
{
    std::set<std::string> projection_names;

    for (auto && [name, projection_part] : part->getProjectionParts())
        if (!projection_part->is_broken)
            projection_names.insert(name);

    return projection_names;
}

}

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    time_t current_time)
{
    const time_t next_index_clear_ttl = buildNextIndexClearTTL(metadata_snapshot, part, current_time);

    return PartProperties{
        .name = part->name,
        .info = part->info,
        .projection_names = getCalculatedProjectionNames(part),
        .all_ttl_calculated_if_any = part->checkAllTTLCalculated(metadata_snapshot),
        .is_in_volume_where_merges_avoid = !part->shallParticipateInMerges(storage_policy),
        .size = part->getExistingBytesOnDisk(),
        .age = current_time - part->modification_time,
        .rows = part->rows_count,
        .general_ttl_info = buildGeneralTTLInfo(metadata_snapshot, part),
        .recompression_ttl_info = buildRecompressTTLInfo(metadata_snapshot, part, current_time),
        .next_index_clear_ttl = next_index_clear_ttl,
        .can_clear_indexes = next_index_clear_ttl != 0 && (*part->storage.getSettings())[MergeTreeSetting::ttl_clear_index_merges]
            && canClearIndexes(part),
    };
}

}
