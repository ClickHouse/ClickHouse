#pragma once
#include "config.h"

#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace Iceberg
{

struct TableStateSnapshot
{
    String metadata_file_path;
    Int32 metadata_version;
    Int32 schema_id;
    std::optional<Int64> snapshot_id;
};

using TableStateSnapshotPtr = std::shared_ptr<TableStateSnapshot>;
}
}
