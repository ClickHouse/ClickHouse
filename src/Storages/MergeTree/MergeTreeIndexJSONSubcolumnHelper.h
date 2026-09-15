#pragma once

#include <optional>

#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Information extracted from a column name that references a JSON subcolumn
/// matched against a JSONAllPaths(...) index column.
struct JSONSubcolumnIndexInfo
{
    String json_column_name;       /// e.g., "json"
    String path;                   /// e.g., "a.b"
    size_t header_position;        /// position of JSONAllPaths column in the index header
};

/// Try to match a column name from the filter DAG to a JSON index column in the header.
/// Scans the index columns, not the dot positions of `column_name`, so cost is independent of the
/// name's length. JSON columns whose own names contain dots are handled (e.g., `my.json` JSON or
/// `t Tuple(json JSON)` with index on `JSONAllPaths(t.json)`); the shortest matching one wins.
///
/// The `json_function_name` parameter specifies which index function to look for (e.g. "JSONAllPaths",
/// "JSONAllValues").
///
/// Returns nullopt if:
///   - No matching index column is found in the header
///   - The subcolumn is a sub-object access (^ prefix) or a combined literal+sub-object access (@ prefix)
std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Block & header,
    const String & json_function_name);

/// Overload that works with a list of index column names instead of a Block.
std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns,
    const String & json_function_name);

class RPNBuilderTreeNode; /// forward declaration to avoid heavy include

/// Like `tryMatchJSONSubcolumnToIndex`, but also handles CAST / `::` syntax.
/// Given a tree node, tries direct column-name match first, then unwraps
/// `CAST(json.path, 'Type')` / `_CAST(json.path, 'Type')` and retries.
std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Block & header,
    const String & json_function_name);

/// Overload that works with a list of index column names instead of a Block.
std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns,
    const String & json_function_name);

/// Whether a `JSONAllPaths` index may skip a granule that lacks the path, for an `equals` comparison of
/// a JSON subcolumn with a constant. The caller has already established that the function is `equals`.
/// When a JSON path is absent in a granule, the expression evaluates to:
///   - NULL if the type is Dynamic or Nullable (always safe — comparisons with NULL are false)
///   - The type's default value if the type is non-Nullable (safe only if the comparison
///     does not hold against that default)
///
/// @param key_expression_type  the actual result type of the key expression from the DAG node
/// @param value_field          the constant value being compared against
/// @param value_type           the declared type of that constant
/// @param context              query context, to build the comparison function
bool isJSONPathFilterSafe(
    const DataTypePtr & key_expression_type,
    const Field & value_field,
    const DataTypePtr & value_type,
    const ContextPtr & context);

}
