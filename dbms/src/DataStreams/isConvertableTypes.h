#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// Check that type 'from' can be implicitly converted to type 'to'.
bool isConvertableTypes(const DataTypePtr & from, const DataTypePtr & to);

}
