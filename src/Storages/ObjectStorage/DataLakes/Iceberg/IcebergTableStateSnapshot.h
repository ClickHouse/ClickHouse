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
    // this filed is from metadata_file_path file last modified time
    UInt64 last_modify_time;

    void serialize(WriteBuffer & out) const;

    static TableStateSnapshot deserialize(ReadBuffer & in, int datalake_state_protocol_version);

    bool operator==(const TableStateSnapshot & other) const;
};

using TableStateSnapshotPtr = std::shared_ptr<TableStateSnapshot>;
}
}
