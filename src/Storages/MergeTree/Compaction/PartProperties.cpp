#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexClearFiles.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/TTLDescription.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
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
        /// FIXME: Implement in other way -- not string comparison
        const std::string next_codec = astToString(ttl_description->recompression_codec);
        const std::string current_codec = astToString(part->default_codec->getFullCodecDesc());

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

    const auto & index_factory = MergeTreeIndexFactory::instance();
    const auto & secondary_indices = metadata_snapshot->getSecondaryIndices();

    time_t next_index_clear_ttl = 0;
    for (const auto & ttl : metadata_snapshot->getIndexClearTTLs())
    {
        auto it = part->ttl_infos.index_clear_ttl.find(ttl.result_column);
        if (it == part->ttl_infos.index_clear_ttl.end())
            continue;

        const time_t max_ttl = it->second.max;
        if (!max_ttl || max_ttl > current_time || (next_index_clear_ttl && next_index_clear_ttl <= max_ttl))
            continue;

        const auto index_it = std::find_if(
            secondary_indices.begin(), secondary_indices.end(),
            [&](const auto & index) { return index.name == ttl.index_name; });
        if (index_it == secondary_indices.end())
            continue;

        /// Check every index file so an already cleared part is not selected again.
        const auto index = index_factory.get(metadata_snapshot, *index_it, *part->storage.getSettings());
        if (!partHasSkipIndexFiles(*part, index))
            continue;

        next_index_clear_ttl = max_ttl;
    }

    return next_index_clear_ttl;
}


}

bool canPreserveFilesForIndexClear(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartPtr & part)
{
    if ((*part->storage.getSettings())[MergeTreeSetting::assign_part_uuids])
        return false;

    if (part->info.isPatch()
        || part->getDataPartStorage().getType() != MergeTreeDataPartStorageType::Full
        || part->uuid != UUIDHelpers::Nil
        || part->old_part_with_no_metadata_version_on_disk
        || part->getMetadataVersion() != metadata_snapshot->getMetadataVersion())
        return false;

    const auto chosen_format = part->storage.choosePartFormat(
        part->getTotalColumnsSize().data_uncompressed,
        part->rows_count,
        part->info.level + 1,
        /*projection=*/nullptr);

    if (chosen_format.part_type != part->getType()
        || chosen_format.storage_type != MergeTreeDataPartStorageType::Full)
        return false;

    const PartFileCopyOptions copy_options
    {
        .fail_on_temporary_projection_directories = true,
        .fail_on_projection_subdirectories = true,
        .cancellation_callback = {},
    };
    return canCopyPartFilesWithSkip(part->getDataPartStorage(), copy_options);
}

bool canPreserveFilesForIndexClear(const FutureMergedMutatedPart & future_part)
{
    if (future_part.parts.size() != 1 || !future_part.patch_parts.empty())
        return false;

    const auto & source_part = future_part.parts.front();
    if (source_part->getDataPartStorage().getType() != MergeTreeDataPartStorageType::Full
        || future_part.part_format.storage_type != MergeTreeDataPartStorageType::Full
        || future_part.part_format.part_type != source_part->getType()
        || future_part.part_format.storage_type != source_part->getDataPartStorage().getType()
        || future_part.uuid != source_part->uuid)
        return false;

    const PartFileCopyOptions copy_options
    {
        .fail_on_temporary_projection_directories = true,
        .fail_on_projection_subdirectories = true,
        .cancellation_callback = {},
    };
    return canCopyPartFilesWithSkip(source_part->getDataPartStorage(), copy_options);
}

namespace
{

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
    const bool all_ttl_calculated_if_any = part->checkAllTTLCalculated(metadata_snapshot);
    const time_t next_index_clear_ttl = buildNextIndexClearTTL(metadata_snapshot, part, current_time);
    const bool can_preserve_files_for_index_clear = all_ttl_calculated_if_any
        && next_index_clear_ttl != 0
        && canPreserveFilesForIndexClear(metadata_snapshot, part);

    return PartProperties{
        .name = part->name,
        .info = part->info,
        .projection_names = getCalculatedProjectionNames(part),
        .all_ttl_calculated_if_any = all_ttl_calculated_if_any,
        .is_in_volume_where_merges_avoid = !part->shallParticipateInMerges(storage_policy),
        .size = part->getExistingBytesOnDisk(),
        .age = current_time - part->modification_time,
        .rows = part->rows_count,
        .general_ttl_info = buildGeneralTTLInfo(metadata_snapshot, part),
        .recompression_ttl_info = buildRecompressTTLInfo(metadata_snapshot, part, current_time),
        .next_index_clear_ttl = next_index_clear_ttl,
        .can_preserve_files_for_index_clear = can_preserve_files_for_index_clear,
    };
}

}
