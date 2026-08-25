#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Disks/IStoragePolicy.h>

#include <Core/UUID.h>

#include <optional>
#include <set>
#include <span>
#include <ctime>

namespace DB
{

class IMergeTreeDataPart;
using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
struct FutureMergedMutatedPart;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct PartProperties
{
    const std::string name;
    const MergeTreePartInfo info;
    const std::set<std::string> projection_names = {};

    const bool all_ttl_calculated_if_any = false;
    const bool is_in_volume_where_merges_avoid = false;

    /// Size of data part in bytes.
    const size_t size = 0;

    /// How old this data part in seconds.
    const time_t age = 0;

    /// Number of rows in part.
    const size_t rows = 0;

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

    /// Next expired index-clear TTL for part. Used by clear-index TTL merge selector.
    const time_t next_index_clear_ttl = 0;

    /// Whether the source part can produce a clear-index replacement while preserving files.
    /// When false, the `TTLClearIndex` merge rewrites the part.
    const bool can_preserve_files_for_index_clear = false;
};

using PartsRange = std::vector<PartProperties>;
using PartsRanges = std::vector<PartsRange>;
using PartsRangeView = std::span<const PartProperties>;

bool canPreserveFilesForIndexClear(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartPtr & part);

bool canPreserveFilesForIndexClear(
    const StorageMetadataPtr & metadata_snapshot,
    const FutureMergedMutatedPart & future_part);

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    time_t current_time);

}
