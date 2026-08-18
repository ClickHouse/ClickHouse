#pragma once
#include "config.h"

#if USE_AVRO

#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>

namespace DB::Iceberg
{

struct IcebergDataSnapshot
{
    DB::ManifestFileCacheKeys manifest_list_entries;
    Int64 snapshot_id;
    Int64 schema_id_on_snapshot_commit;
    /// From snapshot summary (`total-records`). Compared to the manifest-derived count for a
    /// mismatch warning only — never used as the trivial COUNT answer. Summary totals are
    /// maintained incrementally by writers and can be poisoned by a bad commit in table history.
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
    std::optional<size_t> total_position_delete_rows;
    /// Rows in equality-delete files (snapshot summary). Not a count of deleted data rows;
    /// used only to fail closed early when present and > 0.
    std::optional<size_t> total_equality_delete_rows;
    std::optional<String> partition_key;
    std::optional<String> sorting_key;

    std::optional<size_t> getTotalRows() const
    {
        if (!total_rows.has_value() || !total_position_delete_rows.has_value())
            return std::nullopt;
        /// Fail closed on inconsistent summary: unsigned subtract would wrap to a huge COUNT.
        if (*total_position_delete_rows > *total_rows)
            return std::nullopt;
        return *total_rows - *total_position_delete_rows;
    }
};

using IcebergDataSnapshotPtr = std::shared_ptr<IcebergDataSnapshot>;

struct IcebergHistoryRecord
{
    Int64 snapshot_id{};
    DB::DateTime64 made_current_at{};
    Int64 parent_id{};
    bool is_current_ancestor{};
    Iceberg::IcebergPathFromMetadata manifest_list_path;
    std::optional<Iceberg::SnapshotSummary> snapshot_summary;
};

using IcebergHistory = std::vector<Iceberg::IcebergHistoryRecord>;
}

#endif
