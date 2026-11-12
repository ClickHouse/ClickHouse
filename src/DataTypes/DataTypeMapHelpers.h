#pragma once

#include <Columns/IColumn.h>
#include <base/types.h>

#include <optional>
#include <utility>

namespace DB
{

class ColumnsDescription;

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

/// Try to parse a Map subcolumn reference like `map.key_<serialized_key>`.
/// Returns {map_column_name, serialized_key} if the column name has the expected format.
///
/// Dots are legal in column names, so `a.b.key_x` may name something other than key `x` of a Map `a.b`:
/// a column `a.b.key_x`, an element of a Tuple `a`, or a path of a JSON `a`. A predicate reads that
/// claimant, not the map, so the shape is refused unless the name resolves to a key of the Map itself.
std::optional<std::pair<String, String>> tryParseMapSubcolumnName(
    const String & column_name, const ColumnsDescription & columns);

}
