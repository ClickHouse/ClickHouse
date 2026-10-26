#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

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

    time_t group_by_min_ttl = 0;
    for (const auto & [name, info] : part->ttl_infos.group_by_ttl)
        if (info.min && (!group_by_min_ttl || info.min < group_by_min_ttl))
            group_by_min_ttl = info.min;

    return PartProperties::GeneralTTLInfo{
        .has_any_non_finished_row_ttls = part->ttl_infos.hasAnyNonFinishedRowTTLs(),
        .has_any_non_finished_column_ttls = part->ttl_infos.hasAnyNonFinishedColumnTTLs(),
        .part_min_ttl = part->ttl_infos.part_min_ttl,
        .part_drop_ttl = part->ttl_infos.getTTLForPartDropMerge(),
        .column_min_ttl = part->ttl_infos.getMinimalNonFinishedColumnTTL(),
        .group_by_min_ttl = group_by_min_ttl,
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

std::set<std::string> getCalculatedProjectionNames(const MergeTreeDataPartPtr & part)
{
    std::set<std::string> projection_names;

    for (auto && [name, projection_part] : part->getProjectionParts())
        if (!projection_part->is_broken)
            projection_names.insert(name);

    return projection_names;
}

/// Mirrors the `projection_part_misses_column` check of `MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild`:
/// a parent table column that the parent part also lacks is a legitimate late addition and does not count.
std::set<std::string> getProjectionsMissingColumns(const MergeTreeDataPartPtr & part, const StorageMetadataPtr & metadata_snapshot)
{
    std::set<std::string> projection_names;

    const auto & parent_table_columns = metadata_snapshot->getColumns();
    for (const auto & projection : metadata_snapshot->getProjections())
    {
        auto it = part->getProjectionParts().find(projection.name);
        if (it == part->getProjectionParts().end() || it->second->is_broken)
            continue;

        for (const auto & column : projection.metadata->getColumns().getAllPhysical())
        {
            if (!it->second->tryGetColumn(column.name)
                && (part->tryGetColumn(column.name)
                    || !parent_table_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column.name)))
            {
                projection_names.insert(projection.name);
                break;
            }
        }
    }

    return projection_names;
}

std::set<std::string> getUnloadedProjectionNames(const MergeTreeDataPartPtr & part, const StorageMetadataPtr & metadata_snapshot)
{
    std::set<std::string> projection_names;

    for (const auto & projection : metadata_snapshot->getProjections())
        if (part->hasUnloadedProjection(projection.name))
            projection_names.insert(projection.name);

    return projection_names;
}

std::map<std::string, PartProperties::ProjectionPartSize> getProjectionSizes(const MergeTreeDataPartPtr & part)
{
    std::map<std::string, PartProperties::ProjectionPartSize> projection_sizes;

    for (auto && [name, projection_part] : part->getProjectionParts())
        if (!projection_part->is_broken)
            projection_sizes.emplace(name, PartProperties::ProjectionPartSize{
                .rows = projection_part->rows_count,
                .size_uncompressed = projection_part->getTotalColumnsSize().data_uncompressed,
                .level = projection_part->info.level,
                .part_type = projection_part->getType(),
            });

    return projection_sizes;
}

/// Mirrors how `MergeTask::ExecuteAndFinalizeHorizontalPart::prepare` fills `expired_columns`. It leans towards
/// reporting a column as absent: a column whose values sit only in patch parts, or that is the target of a
/// pending rename, is still reported.
std::set<std::string> getAbsentColumns(const MergeTreeDataPartPtr & part, const StorageMetadataPtr & metadata_snapshot)
{
    std::set<std::string> absent_columns;

    const auto & columns = metadata_snapshot->getColumns();
    for (const auto & column : columns.getAllPhysical())
    {
        if (!columns.getDefault(column.name) && !part->tryGetColumn(column.name))
            absent_columns.insert(column.name);
    }

    return absent_columns;
}

}

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    time_t current_time)
{
    return PartProperties{
        .name = part->name,
        .info = part->info,
        .projection_names = getCalculatedProjectionNames(part),
        .projections_missing_columns = getProjectionsMissingColumns(part, metadata_snapshot),
        .unloaded_projection_names = getUnloadedProjectionNames(part, metadata_snapshot),
        .projection_sizes = getProjectionSizes(part),
        .absent_columns = getAbsentColumns(part, metadata_snapshot),
        .all_ttl_calculated_if_any = part->checkAllTTLCalculated(metadata_snapshot),
        .is_in_volume_where_merges_avoid = !part->shallParticipateInMerges(storage_policy),
        .size = part->getExistingBytesOnDisk(),
        .size_uncompressed = part->getTotalColumnsSize().data_uncompressed,
        .part_type = part->getType(),
        .age = current_time - part->modification_time,
        .rows = part->rows_count,
        .has_lightweight_delete = part->hasLightweightDelete(),
        .general_ttl_info = buildGeneralTTLInfo(metadata_snapshot, part),
        .recompression_ttl_info = buildRecompressTTLInfo(metadata_snapshot, part, current_time),
    };
}

}
