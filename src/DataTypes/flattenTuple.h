#pragma once
#include <DataTypes/IDataType.h>

namespace DB
{

/// Flattens nested Tuple to plain Tuple. I.e extracts all paths and types from tuple:
/// Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64) -> Tuple(t.c1 UInt32, t.c2 String, c3 UInt32)
/// It also goes through arrays:
/// Tuple(arr Array(Tuple(c1 UInt32, c2 String))) -> Tuple(arr.c1 Array(UInt32), arr.c2 Array(String))
DataTypePtr flattenTuple(const DataTypePtr & type);
ColumnPtr flattenTuple(const ColumnPtr & column);


}
