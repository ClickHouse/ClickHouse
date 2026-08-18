#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <fmt/format.h>

namespace DB
{

/// Extract the JSON path from a subcolumn name, stripping any `.:\`Type\`` suffix.
/// For example:
///   "a.b"            -> "a.b"
///   "a.b.:`Int64`"   -> "a.b"
///   "a.b.:`Array(Int64)`"  -> "a.b"
static std::optional<String> extractPathFromSubcolumn(std::string_view subcolumn_name, size_t & array_json_levels)
{
    if (subcolumn_name.empty()
        || subcolumn_name.starts_with("^")
        || subcolumn_name.starts_with("@`")
        || subcolumn_name.find(".^`") != std::string_view::npos
        || subcolumn_name.find(".@`") != std::string_view::npos)
        return std::nullopt;

    String path;
    std::string_view remaining = subcolumn_name;
    while (!remaining.empty())
    {
        const size_t type_hint_position = remaining.find(".:`");
        if (type_hint_position == std::string_view::npos)
        {
            path += remaining;
            break;
        }

        path += remaining.substr(0, type_hint_position);
        ReadBufferFromMemory buffer(remaining.substr(type_hint_position + 2));
        String type_hint;
        if (!tryReadBackQuotedString(type_hint, buffer))
            return std::nullopt;

        auto type = DataTypeFactory::instance().get(type_hint);
        size_t levels = 0;
        while (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        {
            ++levels;
            type = removeNullableOrLowCardinalityNullable(array_type->getNestedType());
        }

        std::string_view tail(buffer.position(), buffer.available());
        if (levels == 0 || removeLowCardinality(type)->getTypeId() != TypeIndex::Object)
        {
            if (!tail.empty())
                return std::nullopt;
            break;
        }

        array_json_levels += levels;
        for (size_t level = 0; level != levels; ++level)
            path += "[]";
        if (tail.starts_with('.'))
            tail.remove_prefix(1);
        if (!tail.empty())
            path += '.';
        remaining = tail;
    }

    if (path.empty())
        return std::nullopt;
    return path;
}

std::optional<JSONSubcolumnIndexInfo> tryMatchJSONSubcolumn(
    const String & column_name,
    const String & json_column_name,
    size_t header_position)
{
    for (auto [candidate_col, subcolumn_part] : Nested::getAllColumnAndSubcolumnPairs(column_name))
    {
        if (candidate_col != json_column_name)
            continue;

        size_t array_json_levels = 0;
        auto path = extractPathFromSubcolumn(subcolumn_part, array_json_levels);
        if (!path)
            return std::nullopt;

        return JSONSubcolumnIndexInfo{
            .json_column_name = String(candidate_col),
            .path = std::move(*path),
            .header_position = header_position,
            .array_json_levels = array_json_levels,
        };
    }

    return std::nullopt;
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

        size_t array_json_levels = 0;
        auto path = extractPathFromSubcolumn(subcolumn_part, array_json_levels);
        if (!path)
            return std::nullopt;

        size_t position = static_cast<size_t>(std::distance(index_columns.begin(), it));

        return JSONSubcolumnIndexInfo{
            .json_column_name = String(candidate_col),
            .path = std::move(*path),
            .header_position = position,
            .array_json_levels = array_json_levels,
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

String serializeFieldAsText(const Field & value, const DataTypePtr & type)
{
    auto column = type->createColumn();
    column->insert(value);
    WriteBufferFromOwnString buffer;
    type->getDefaultSerialization()->serializeText(*column, 0, buffer, {});
    return buffer.str();
}

}
