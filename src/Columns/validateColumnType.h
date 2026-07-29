#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Recursively checks that a column's structure matches its declared DataType.
/// Returns true if the column is compatible with the type, false otherwise.
/// This goes deeper than comparing top-level TypeIndex values: it recurses
/// into Array, Nullable, Tuple, and Map to verify nested element types.
///
/// With strict_decimal_scale, a Decimal/DateTime64/Time64 column must also have the
/// same scale as the type (getDataType() alone is scale-blind: every scale maps to the
/// same physical TypeIndex). Off by default because a scale-divergent column behaves
/// correctly almost everywhere; the property fuzzer enables it to detect such a column at
/// the producing function's gate before stashing and reusing it as another function's input.
bool columnMatchesType(const IColumn & column, const IDataType & type, bool strict_decimal_scale = false);

}
