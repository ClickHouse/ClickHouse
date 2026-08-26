#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <Common/Exception.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
}

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
    /// Scan the index columns, not the dot positions of the name: the name can embed a folded
    /// constant, so its length is unbounded while `index_columns` is not.
    const std::string_view name = column_name;
    const size_t json_column_offset = json_function_name.size() + 1;

    std::string_view matched_json_column;
    std::string_view matched_subcolumn;
    size_t matched_position = 0;
    bool matched = false;

    for (size_t position = 0; position < index_columns.size(); ++position)
    {
        const std::string_view entry = index_columns[position];

        /// Entry must be `json_function_name(X)` with a non-empty X.
        if (entry.size() < json_column_offset + 2 || entry.back() != ')' || !entry.starts_with(json_function_name)
            || entry[json_function_name.size()] != '(')
            continue;

        const std::string_view json_column = entry.substr(json_column_offset, entry.size() - json_column_offset - 1);

        /// The name must be `X.<non-empty subcolumn>`.
        if (json_column.size() + 1 >= name.size() || !name.starts_with(json_column) || name[json_column.size()] != '.')
            continue;

        /// Shortest X wins, ties resolve to the first entry: several entries can match one name.
        if (matched && json_column.size() >= matched_json_column.size())
            continue;

        matched_json_column = json_column;
        matched_subcolumn = name.substr(json_column.size() + 1);
        matched_position = position;
        matched = true;
    }

    if (!matched)
        return std::nullopt;

    /// Sub-object access (^ prefix) is not supported for index filtering
    if (matched_subcolumn.starts_with("^"))
        return std::nullopt;

    String path = extractPathFromSubcolumn(matched_subcolumn);
    if (path.empty())
        return std::nullopt;

    return JSONSubcolumnIndexInfo{
        .json_column_name = String(matched_json_column),
        .path = std::move(path),
        .header_position = matched_position,
    };
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
    Field converted;
    try
    {
        converted = convertFieldToType(value_field, *key_expression_type);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM)
            throw;
        return false;
    }
    if (converted == key_expression_type->getDefault())
        return false;

    return true;
}

}
