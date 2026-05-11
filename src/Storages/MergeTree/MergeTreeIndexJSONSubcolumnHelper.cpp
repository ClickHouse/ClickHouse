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
    const Block & header,
    const String & json_function_name)
{
    return tryMatchJSONSubcolumnToIndex(column_name, header.getNames(), json_function_name);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumnToIndex(
    const String & column_name,
    const Names & index_columns,
    const String & json_function_name)
{
    /// Try all possible dot splits of the column name.
    /// For "t.json.some.path" this produces:
    ///   ("t", "json.some.path"), ("t.json", "some.path"), ("t.json.some", "path")
    for (auto [candidate_col, subcolumn_part] : Nested::getAllColumnAndSubcolumnPairs(column_name))
    {
        auto index_column_name = fmt::format("{}({})", json_function_name, candidate_col);
        auto it = std::find(index_columns.begin(), index_columns.end(), index_column_name);
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
            .path = std::move(path),
            .header_position = position,
        };
    }

    return std::nullopt;
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Block & header,
    const String & json_function_name)
{
    return tryMatchNodeToJSONIndex(node, header.getNames(), json_function_name);
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns,
    const String & json_function_name)
{
    auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns, json_function_name);

    /// Try CAST unwrapping: CAST(json.path, 'Type') or _CAST(json.path, 'Type')
    if (!json_info && node.isFunction())
    {
        auto func = node.toFunctionNode();
        auto fname = func.getFunctionName();
        if ((fname == "CAST" || fname == "_CAST") && func.getArgumentsSize() == 2)
            json_info = tryMatchJSONSubcolumnToIndex(
                func.getArgumentAt(0).getColumnName(), index_columns, json_function_name);
    }

    return json_info;
}

/// Parse a JSONValues index column name of the form:
///   JSONValues(col, 'path1', 'path2', ...)
/// Returns {col, [path1, path2, ...]} or nullopt if the name doesn't match.
static std::optional<std::pair<String, std::vector<String>>>
parseJSONValuesColumnName(const String & col_name)
{
    static constexpr std::string_view prefix = "JSONValues(";
    if (!col_name.starts_with(prefix))
        return std::nullopt;

    if (col_name.back() != ')')
        return std::nullopt;

    std::string_view rest(col_name);
    rest.remove_prefix(prefix.size());
    rest.remove_suffix(1); // remove trailing ')'

    /// First token is the unquoted JSON column name, followed by ", 'pathN'" args.
    auto comma = rest.find(", '");
    if (comma == std::string_view::npos)
        return std::nullopt;

    String json_col(rest.substr(0, comma));
    rest.remove_prefix(comma + 2); // skip ", "

    std::vector<String> paths;
    while (!rest.empty())
    {
        if (rest[0] != '\'')
            break;
        rest.remove_prefix(1);
        auto end = rest.find('\'');
        if (end == std::string_view::npos)
            break;
        paths.emplace_back(rest.substr(0, end));
        rest.remove_prefix(end + 1);
        if (rest.starts_with(", "))
            rest.remove_prefix(2);
    }

    if (paths.empty())
        return std::nullopt;

    return std::make_pair(std::move(json_col), std::move(paths));
}

static std::optional<JSONSubcolumnIndexInfo>
tryMatchColumnToJSONValuesIndex(const String & column_name, const Names & index_columns)
{
    for (auto [candidate_col, subcolumn_part] : Nested::getAllColumnAndSubcolumnPairs(column_name))
    {
        if (subcolumn_part.starts_with("^"))
            continue;

        String path = extractPathFromSubcolumn(subcolumn_part);
        if (path.empty())
            continue;

        for (size_t idx = 0; idx < index_columns.size(); ++idx)
        {
            auto parsed = parseJSONValuesColumnName(index_columns[idx]);
            if (!parsed)
                continue;

            const auto & [json_col, listed_paths] = *parsed;
            if (json_col != candidate_col)
                continue;

            if (std::find(listed_paths.begin(), listed_paths.end(), path) != listed_paths.end())
            {
                return JSONSubcolumnIndexInfo{
                    .json_column_name = String(candidate_col),
                    .path = std::move(path),
                    .header_position = idx,
                };
            }
        }
    }
    return std::nullopt;
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONValuesIndex(
    const RPNBuilderTreeNode & node,
    const Block & header)
{
    return tryMatchNodeToJSONValuesIndex(node, header.getNames());
}

std::optional<JSONSubcolumnIndexInfo> tryMatchNodeToJSONValuesIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns)
{
    auto json_info = tryMatchColumnToJSONValuesIndex(node.getColumnName(), index_columns);

    if (!json_info && node.isFunction())
    {
        auto func = node.toFunctionNode();
        auto fname = func.getFunctionName();
        if ((fname == "CAST" || fname == "_CAST") && func.getArgumentsSize() == 2)
            json_info = tryMatchColumnToJSONValuesIndex(
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
