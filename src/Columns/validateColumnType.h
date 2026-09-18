#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Recursively checks that a column's structure matches its declared DataType.
/// Returns true if the column is compatible with the type, false otherwise.
/// This goes deeper than comparing top-level TypeIndex values: it recurses
/// into Array, Nullable, Tuple, and Map to verify nested element types.
bool columnMatchesType(const IColumn & column, const IDataType & type);

}
