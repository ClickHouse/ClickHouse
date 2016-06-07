#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Functions/EnrichedDataTypePtr.h>

namespace DB
{

namespace Conditional
{

/// Determine the least common type of the elements of an array.
DataTypeTraits::EnrichedDataTypePtr getArrayType(const DataTypes & args);

}

}
