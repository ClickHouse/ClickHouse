#pragma once

#include <optional>

#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Information extracted from a column name that references a JSON subcolumn
/// matched against a JSONAllPaths(...) index column.
struct JSONSubcolumnIndexInfo
{
    String json_column_name;       /// e.g., "json"
    String json_all_paths_column;  /// e.g., "JSONAllPaths(json)"
    String path;                   /// e.g., "a.b"
    size_t header_position;        /// position of JSONAllPaths column in the index header
};

/// Try to match a column name from the filter DAG to a JSONAllPaths index column in the header.
/// Iterates all dot positions in `column_name` to handle JSON columns whose names contain dots
/// (e.g., `my.json` JSON or `t Tuple(json JSON)` with index on `JSONAllPaths(t.json)`).
///
/// Returns nullopt if:
///   - No matching JSONAllPaths column is found in the header
///   - The subcolumn is a sub-object access (^ prefix)
std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Block & header);

/// Overload that works with a list of index column names instead of a Block.
std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns);

class RPNBuilderTreeNode; /// forward declaration to avoid heavy include

/// Like `tryMatchJSONSubcolumnToIndex`, but also handles CAST / `::` syntax.
/// Given a tree node, tries direct column-name match first, then unwraps
/// `CAST(json.path, 'Type')` / `_CAST(json.path, 'Type')` and retries.
std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Block & header);

/// Overload that works with a list of index column names instead of a Block.
std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns);

/// Check if a JSON path filter is safe to use for index skipping.
/// When a JSON path is absent in a granule, the expression evaluates to:
///   - NULL if the type is Dynamic or Nullable (always safe — comparisons with NULL are false)
///   - The type's default value if the type is non-Nullable (safe only if the comparison
///     value differs from the default)
///
/// @param key_expression_type  the actual result type of the key expression from the DAG node
/// @param value_field          the constant value being compared against
bool isJSONPathFilterSafe(
    const DataTypePtr & key_expression_type,
    const Field & value_field);

}
