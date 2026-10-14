#pragma once

#include "config.h"
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeTableStateSnapshot.h>
#endif

namespace DB
{

enum DataLakeTableStateSnapshotType
{
    ICEBERG_TABLE_STATE_SNAPSHOT = 1,
    DELTA_LAKE_TABLE_STATE_SNAPSHOT = 2,
};

// This state should be preserved as simple as possible to allow serialization/deserialization.
#if USE_PARQUET && USE_DELTA_KERNEL_RS
using DataLakeTableStateSnapshot = std::variant<Iceberg::TableStateSnapshot, DeltaLake::TableStateSnapshot>;
#else
using DataLakeTableStateSnapshot = std::variant<Iceberg::TableStateSnapshot>;
#endif

void serializeDataLakeTableStateSnapshot(DataLakeTableStateSnapshot state, WriteBuffer & out);
DataLakeTableStateSnapshot deserializeDataLakeTableStateSnapshot(ReadBuffer & in);
}
