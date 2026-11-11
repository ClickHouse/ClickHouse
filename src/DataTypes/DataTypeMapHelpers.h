#pragma once

#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <base/types.h>

#include <functional>
#include <optional>
#include <utility>

namespace DB
{

/// Optimized extraction of values for a given constant key from a Map column
/// stored as Array(Tuple(K, V)).
///
/// For each row in [start, end), finds the key-value pair matching `key`
/// and inserts the corresponding value into `result`. If the key is not found,
/// inserts a default value (or null for Nullable value types).
void extractKeyValueFromMap(
    const IColumn & nested_column,
    const IColumn & key,
    IColumn & result,
    size_t start,
    size_t end);

/// Tells whether the table has a real column (or a subcolumn of one) with the given name.
using IsShadowingColumn = std::function<bool(const String &)>;

/// Try to parse a Map subcolumn reference like `map.key_<serialized_key>`.
/// Returns {map_column_name, serialized_key} if the column name has the expected format.
///
/// Dots are legal in column names, so a real column `m.key_x` may exist beside a Map `m`. It shadows
/// the subcolumn: a predicate reads that column, not the map, so `is_shadowing_column` must answer
/// true for such names and the shape is refused. It is asked only about names of that shape.
std::optional<std::pair<String, String>> tryParseMapSubcolumnName(
    const String & column_name, const IsShadowingColumn & is_shadowing_column);

}
