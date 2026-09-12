#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/JSONPathValues.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

/// Prefixed subcolumns look like "<prefix>`first_path_element`.rest": the back-quote distinguishes
/// them from an ordinary path starting with the prefix character, e.g. "@`a`" versus "@a".
static bool isPrefixedSubcolumn(std::string_view subcolumn_name, char prefix)
{
    return subcolumn_name.size() >= 2 && subcolumn_name[0] == prefix && subcolumn_name[1] == '`';
}

/// Extract the JSON path from a subcolumn name, stripping any `.:\`Type\`` suffix.
/// For example:
///   "a.b"            -> "a.b"
///   "a.b.:`Int64`"   -> "a.b"
///   "a.b.:`Array(Int64)`"  -> "a.b"
static std::optional<String> extractPathFromSubcolumn(
    std::string_view subcolumn_name,
    String & escaped_path,
    size_t & array_json_levels)
{
    if (subcolumn_name.empty()
        || isPrefixedSubcolumn(subcolumn_name, DataTypeObject::SUB_OBJECT_SUBCOLUMN_PREFIX)
        || isPrefixedSubcolumn(subcolumn_name, DataTypeObject::COMBINED_SUBCOLUMN_PREFIX)
        || subcolumn_name.contains(".^`")
        || subcolumn_name.contains(".@`"))
        return std::nullopt;

    String path;
    std::string_view remaining = subcolumn_name;
    while (!remaining.empty())
    {
        const size_t type_hint_position = remaining.find(".:`");
        if (type_hint_position == std::string_view::npos)
        {
            path += remaining;
            escaped_path += JSONPathValues::escapeLiteralPath(remaining);
            break;
        }

        path += remaining.substr(0, type_hint_position);
        escaped_path += JSONPathValues::escapeLiteralPath(remaining.substr(0, type_hint_position));
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
        {
            path += "[]";
            escaped_path += "[]";
        }
        if (tail.starts_with('.'))
            tail.remove_prefix(1);
        if (!tail.empty())
        {
            path += '.';
            escaped_path += '.';
        }
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
        String escaped_path;
        auto path = extractPathFromSubcolumn(subcolumn_part, escaped_path, array_json_levels);
        if (!path)
            return std::nullopt;

        return JSONSubcolumnIndexInfo{
            .json_column_name = String(candidate_col),
            .path = std::move(*path),
            .escaped_path = std::move(escaped_path),
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

    size_t array_json_levels = 0;
    String escaped_path;
    auto path = extractPathFromSubcolumn(matched_subcolumn, escaped_path, array_json_levels);
    if (!path)
        return std::nullopt;

    return JSONSubcolumnIndexInfo{
        .json_column_name = String(matched_json_column),
        .path = std::move(*path),
        .escaped_path = std::move(escaped_path),
        .header_position = matched_position,
        .array_json_levels = array_json_levels,
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
