#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/StorageInMemoryMetadata.h>

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
    time_t current_time)
{
    return PartProperties{
        .name = part->name,
        .info = part->info,
        .projection_names = getCalculatedProjectionNames(part),
        .all_ttl_calculated_if_any = part->checkAllTTLCalculated(metadata_snapshot),
        .size = part->getExistingBytesOnDisk(),
        .age = current_time - part->modification_time,
        .general_ttl_info = buildGeneralTTLInfo(metadata_snapshot, part),
        .recompression_ttl_info = buildRecompressTTLInfo(metadata_snapshot, part, current_time),
    };
}

}
