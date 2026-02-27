#pragma once

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>

namespace DB
{

enum DataLakeTableStateSnapshotType
{
    ICEBERG_TABLE_STATE_SNAPSHOT = 1,
};

// This state should be preserved as simple as possible to allow serialization/deserialization.
using DataLakeTableStateSnapshot = std::variant<Iceberg::TableStateSnapshot>;

void serializeDataLakeTableStateSnapshot(DataLakeTableStateSnapshot state, WriteBuffer & out);
DataLakeTableStateSnapshot deserializeDataLakeTableStateSnapshot(ReadBuffer & in);
}
