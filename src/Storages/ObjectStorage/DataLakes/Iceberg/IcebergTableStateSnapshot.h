#pragma once
#include "config.h"

#include "Storages/StorageSnapshot.h"

namespace DB {

namespace Iceberg {

struct IcebergTableStateSnapshot 
{
    String metadata_file_path;
    Int32 metadata_version;
    Int32 schema_id;
    std::optional<Int64> snapshot_id;
};


struct IcebergSpecificSnapshotData : StorageSnapshot::Data {
    IcebergTableStateSnapshot iceberg_table_state;
    explicit IcebergSpecificSnapshotData(const IcebergTableStateSnapshot & iceberg_table_state_)
        : iceberg_table_state(iceberg_table_state_)
    {}
};

using IcebergTableStateSnapshotPtr = std::shared_ptr<IcebergTableStateSnapshot>;

}

}
