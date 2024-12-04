#pragma once

#include <ctime>
#include <cstddef>
#include <string>
#include <vector>

#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>

namespace DB
{

struct PartProperties
{
    const std::string name;
    const std::string partition_id;
    const bool shall_participate_in_merges = true;

    /// Size of data part in bytes.
    const size_t size = 0;

    /// How old this data part in seconds.
    const time_t age = 0;

    /// Depth of tree of merges by which this part was created. New parts has zero level.
    const uint32_t level = 0;

    /// Information about different TTLs for part. Can be used by
    /// TTLSelector to assign merges with TTL.
    const MergeTreeDataPartTTLInfos * ttl_infos = nullptr;

    /// Part compression codec definition.
    ASTPtr compression_codec_desc;
};

/// Parts are belong to partitions. Only parts within same partition could be merged.
using PartsRange = std::vector<PartProperties>;

/// Parts are in some specific order. Parts could be merged only in contiguous ranges.
using PartsRanges = std::vector<PartsRange>;

}
