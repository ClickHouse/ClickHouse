#pragma once
#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace Iceberg
{

using ManifestList = std::vector<ManifestFilePtr>;
using ManifestListPtr = std::shared_ptr<const ManifestList>;

struct IcebergSnapshot
{
    ManifestListPtr manifest_list;
    Int64 snapshot_id;
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
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
