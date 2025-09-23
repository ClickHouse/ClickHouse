#pragma once 

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>

namespace DB {

// This state should be preserved as simple as possible to allow serialization/deserialization.
using DataLakeTableStateSnapshot = std::variant<Iceberg::TableStateSnapshot>;

}
