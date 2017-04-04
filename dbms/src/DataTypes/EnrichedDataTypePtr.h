#pragma once

#include <DataTypes/IDataType.h>
#include <utility>

namespace DB
{

namespace DataTypeTraits
{

/// This type is declared in a separate header in order to increase
/// compilation speed.
using EnrichedDataTypePtr = std::pair<DataTypePtr, DataTypePtr>;

}

}
