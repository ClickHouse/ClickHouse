#pragma once
#include "config.h"

#if USE_AVRO

#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

namespace Iceberg
{

struct ManifestListEntry
{
    DB::ManifestFileCacheKey manifest_file_cache_key;
    ManifestFileContentType content_type;
};

using ManifestListEntries = std::vector<ManifestListEntry>;

struct IcebergDataSnapshot
{
    ManifestListEntries manifest_list_entries;
    Int64 snapshot_id;
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
    std::optional<size_t> total_position_delete_rows;
    Int32 schema_id;

    std::optional<size_t> getTotalRows() const
    {
        if (total_rows.has_value() && total_position_delete_rows.has_value())
            return *total_rows - *total_position_delete_rows;
        return std::nullopt;
    }
};

struct IcebergTableStateSnapshot
{
    Int32 metadata_version;
    std::optional<Int32> snapshot_id;
    std::optional<Int32> schema_id; // Set if snapshot is null or schema was changed after corresponding data commit
};

using IcebergTableStateSnapshotPtr = std::shared_ptr<IcebergTableStateSnapshot>;

struct IcebergHistoryRecord
{
    Int64 snapshot_id;
    DB::DateTime64 made_current_at;
    Int64 parent_id;
    bool is_current_ancestor;
};
}

#endif
