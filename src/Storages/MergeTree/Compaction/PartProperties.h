#pragma once

#include <optional>

#include <Storages/MergeTree/MergeTreePartInfo.h>

namespace DB
{

struct PartProperties
{
    const MergeTreePartInfo part_info;
    const std::string part_compression_codec;
    const bool shall_participate_in_merges = true;

    /// Size of data part in bytes.
    const size_t size = 0;

    /// How old this data part in seconds.
    const time_t age = 0;

    /// Depth of tree of merges by which this part was created. New parts has zero level.
    const uint32_t level = 0;

    /// Information about different TTLs for part. Used by Part/Row Delete Merge Selectors.
    struct GeneralTTLInfo
    {
        const bool has_any_non_finished_ttls = false;
        const time_t part_min_ttl = 0;
        const time_t part_max_ttl = 0;
    };
    const std::optional<GeneralTTLInfo> general_ttl_info;

    struct RecompressTTLInfo
    {
        const time_t next_max_recompress_border = 0;
        const std::optional<std::string> next_recompression_codec;
    };
    const std::optional<RecompressTTLInfo> recompression_ttl_info;
};

/// Parts are belong to partitions. Only parts within same partition could be merged.
using PartsRange = std::vector<PartProperties>;

/// Parts are in some specific order. Parts could be merged only in contiguous ranges.
using PartsRanges = std::vector<PartsRange>;

}
