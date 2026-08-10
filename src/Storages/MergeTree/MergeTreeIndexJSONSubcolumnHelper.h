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
    String path;                   /// e.g., "a.b"
    size_t header_position;        /// position of JSONAllPaths column in the index header
    bool has_type_hint;
    bool has_subcolumn_after_type_hint;
};

struct JSONAllValuesIndexInfo
{
    JSONSubcolumnIndexInfo subcolumn;
    bool is_string_cast = false;
    std::optional<Field> unindexed_value;
};

/// Try to match a column name from the filter DAG to a JSON index column in the header.
/// Iterates all dot positions in `column_name` to handle JSON columns whose names contain dots
/// (e.g., `my.json` JSON or `t Tuple(json JSON)` with index on `JSONAllPaths(t.json)`).
///
/// The `json_function_name` parameter specifies which index function to look for (e.g. "JSONAllPaths",
/// "JSONAllValues").
///
/// Returns nullopt if:
///   - No matching index column is found in the header
///   - The subcolumn is a sub-object access (^ prefix)
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

/// Match a JSON subcolumn against a `JSONAllValues` index without accepting casts that can change
/// the indexed representation. A cast to `String` or `Nullable(String)` is safe because
/// `JSONAllValues` uses the same text representation for non-null JSON values, unless the cast
/// removes nullability and can throw on `NULL`.
std::optional<JSONAllValuesIndexInfo> tryMatchNodeToJSONAllValuesIndex(
    const RPNBuilderTreeNode & node,
    const Block & header);

/// Overload that works with a list of index column names instead of a `Block`.
std::optional<JSONAllValuesIndexInfo> tryMatchNodeToJSONAllValuesIndex(
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

/// Convert a value to the type whose text representation is stored by `JSONAllValues`.
/// The source type is required for domain-specific conversions such as `Date` to `DateTime`.
Field tryConvertJSONValueToType(
    const Field & value,
    const DataTypePtr & source_type,
    const DataTypePtr & target_type);

/// Convert a value to the target type, if specified, and then to the text representation stored by `JSONAllValues`.
std::optional<String> tryConvertAndSerializeJSONValueAsText(
    const Field & value,
    const DataTypePtr & source_type,
    const DataTypePtr & target_type,
    const std::optional<Field> & unindexed_value,
    bool serialize_quoted = false);

}
