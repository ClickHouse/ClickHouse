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
    /// Row-count hint from the snapshot summary (`total-records`). Never used as a data
    /// source, because the summary is maintained incrementally by writers and a corrupted
    /// commit in the table history poisons it silently. It is only cross-checked against the
    /// row count derived from the manifest files, and a disagreement disables the
    /// metadata-only count, see `IcebergMetadata::totalRows`.
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
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
