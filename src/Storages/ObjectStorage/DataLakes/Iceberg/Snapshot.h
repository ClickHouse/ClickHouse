#pragma once
#include "config.h"

#if USE_AVRO

#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

namespace Iceberg
{

struct IcebergSnapshot
{
    DB::ManifestFileCacheKeys manifest_list_entries;
    Int64 snapshot_id;
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
    std::optional<size_t> total_positional_delete_rows;

    std::optional<size_t> getTotalRows() const
    {
        if (total_rows.has_value() && total_positional_delete_rows.has_value())
            return *total_rows - *total_positional_delete_rows;
        return std::nullopt;
    }
};

struct IcebergHistoryRecord
{
    Int64 snapshot_id;
    DB::DateTime64 made_current_at;
    Int64 parent_id;
    bool is_current_ancestor;
};
}

#endif
