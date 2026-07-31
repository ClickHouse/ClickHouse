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
    /// Row-count hint from the snapshot summary (`total-records`). Only used to log a
    /// warning when it disagrees with the row count derived from the manifest files; never
    /// used as a data source, because the summary is maintained incrementally by writers
    /// and a corrupted commit in the table history poisons it silently.
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
    std::optional<size_t> total_position_delete_rows;
    /// Rows in equality-delete files (snapshot summary). Not a count of deleted data rows;
    /// used only to fail closed trivial COUNT when equality deletes are present.
    std::optional<size_t> total_equality_delete_rows;

    std::optional<size_t> getTotalRows() const
    {
        if (total_rows.has_value() && total_position_delete_rows.has_value())
            return *total_rows - *total_position_delete_rows;
        return std::nullopt;
    }

    /// Summary `total-equality-deletes` is optional. Only trust the cheap getTotalRows() shortcut
    /// when the field is present and explicitly zero; absent or >0 must fall through / fail closed.
    /// Callers must also fail closed when DVs and parquet position deletes coexist in manifests —
    /// snapshot totals alone cannot express Iceberg DV supersession of matching position deletes.
    bool allowsSnapshotTotalRowsShortcut() const
    {
        return total_equality_delete_rows.has_value() && *total_equality_delete_rows == 0;
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
