#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Core/UUID.h>

#include <optional>
#include <set>

namespace DB
{

struct PartProperties
{
    const std::string name;
    const MergeTreePartInfo part_info;
    const UUID uuid = UUIDHelpers::Nil;
    const std::set<std::string> projection_names = {};

    const bool shall_participate_in_merges = true;
    const bool all_ttl_calculated_if_any = false;

    /// Size of data part in bytes.
    const size_t size = 0;

    /// How old this data part in seconds.
    const time_t age = 0;

    /// Information about different TTLs for part. Used by Part/Row Delete Merge Selectors.
    struct GeneralTTLInfo
    {
        const bool has_any_non_finished_ttls;
        const time_t part_min_ttl;
        const time_t part_max_ttl;
    };
    const std::optional<GeneralTTLInfo> general_ttl_info = std::nullopt;

    /// Information about recompression TTL for part. Used by Recompress Merge Selector.
    struct RecompressTTLInfo
    {
        const bool will_change_codec;
        const time_t next_recompress_ttl;
    };
    const std::optional<RecompressTTLInfo> recompression_ttl_info = std::nullopt;
};

using PartsRange = std::vector<PartProperties>;
using PartsRanges = std::vector<PartsRange>;

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    time_t current_time,
    bool has_volumes_with_disabled_merges);

}
