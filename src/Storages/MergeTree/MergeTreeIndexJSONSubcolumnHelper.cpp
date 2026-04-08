#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/convertFieldToType.h>

#include <algorithm>
#include <fmt/format.h>

namespace DB
{

/// Extract the JSON path from a subcolumn name, stripping any `.:\`Type\`` suffix.
/// For example:
///   "a.b"            -> "a.b"
///   "a.b.:`Int64`"   -> "a.b"
///   "a.b.:`Array(Int64)`"  -> "a.b"
static String extractPathFromSubcolumn(std::string_view subcolumn_name)
{
    /// Dynamic type subcolumn looks like "some.path.:`TypeName`..."
    /// Find the ".:`" pattern that marks the start of the type specifier.
    auto pos = subcolumn_name.find(".:`");
    if (pos == std::string_view::npos)
        return String(subcolumn_name);

    return String(subcolumn_name.substr(0, pos));
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Block & header)
{
    return tryMatchJSONSubcolumnToIndex(column_name, header.getNames());
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns)
{
    /// Try all possible dot splits of the column name.
    /// For "t.json.some.path" this produces:
    ///   ("t", "json.some.path"), ("t.json", "some.path"), ("t.json.some", "path")
    for (auto [candidate_col, subcolumn_part] : Nested::getAllColumnAndSubcolumnPairs(column_name))
    {
        auto json_all_paths_name = fmt::format("JSONAllPaths({})", candidate_col);
        auto it = std::find(index_columns.begin(), index_columns.end(), json_all_paths_name);
        if (it == index_columns.end())
            continue;

        /// Sub-object access (^ prefix) is not supported for index filtering
        if (subcolumn_part.starts_with("^"))
            return std::nullopt;

        String path = extractPathFromSubcolumn(subcolumn_part);
        if (path.empty())
            return std::nullopt;

        size_t position = static_cast<size_t>(std::distance(index_columns.begin(), it));

        return JSONSubcolumnIndexInfo{
            .json_column_name = String(candidate_col),
            .json_all_paths_column = std::move(json_all_paths_name),
            .path = std::move(path),
            .header_position = position,
        };
    }

    return std::nullopt;
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Block & header)
{
    return tryMatchNodeToJSONIndex(node, header.getNames());
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns)
{
    auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns);

    /// Try CAST unwrapping: CAST(json.path, 'Type') or _CAST(json.path, 'Type')
    if (!json_info && node.isFunction())
    {
        auto func = node.toFunctionNode();
        auto fname = func.getFunctionName();
        if ((fname == "CAST" || fname == "_CAST") && func.getArgumentsSize() == 2)
            json_info = tryMatchJSONSubcolumnToIndex(
                func.getArgumentAt(0).getColumnName(), index_columns);
    }

    return json_info;
}

bool isJSONPathFilterSafe(
    const DataTypePtr & key_expression_type,
    const Field & value_field)
{
    /// Types that can contain NULL (Dynamic, Nullable, LowCardinality(Nullable), Variant)
    /// store NULL for missing paths — always safe to skip.
    if (canContainNull(*key_expression_type))
        return true;

    /// Non-nullable type: missing path produces the type's default value.
    /// If comparing to the default, we cannot safely skip the granule.
    /// Convert value_field to the key expression type before comparing.
    auto converted = convertFieldToType(value_field, *key_expression_type);
    if (converted == key_expression_type->getDefault())
        return false;

    return true;
}

}
