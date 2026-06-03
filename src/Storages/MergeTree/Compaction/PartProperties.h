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
        /// True if at least one TTL kind that contributes to `part_max_ttl`
        /// (table/columns/rows-where/group-by) is still non-finished. Used by
        /// `TTLPartDropMergeSelector` and `TTLRowDeleteMergeSelector` so that
        /// an unfinished move or recompression TTL - which never marks itself
        /// finished and does not feed `part_max_ttl` - cannot keep these
        /// selectors picking the same part forever (issue #105647).
        const bool has_any_non_finished_rows_affecting_ttls;
        /// Minimum `min` watermark among unfinished rows-affecting TTLs.
        /// `TTLRowDeleteMergeSelector` uses this for CENTER selection because
        /// the persisted `part_min_ttl` may come from a finished expired
        /// `GROUP BY` TTL next to a future unfinished rows TTL.
        const time_t part_min_unfinished_rows_affecting_ttl;
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
using PartsRangeView = std::span<const PartProperties>;

PartProperties buildPartProperties(
    const MergeTreeDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    time_t current_time);

}
