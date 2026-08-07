#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <IO/WriteBufferFromString.h>
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

std::optional<JSONAllValuesIndexInfo> tryMatchNodeToJSONAllValuesIndex(
    const RPNBuilderTreeNode & node,
    const Block & header)
{
    return tryMatchNodeToJSONAllValuesIndex(node, header.getNames());
}

static bool isJSONAllValuesMatchSafe(
    const RPNBuilderTreeNode & node,
    JSONAllValuesMatchKind match_kind)
{
    if (match_kind == JSONAllValuesMatchKind::StringCast)
        return true;

    const auto * dag_node = node.getDAGNode();
    if (!dag_node)
        return false;

    const auto & result_type = *dag_node->result_type;
    if (isDynamic(result_type) || isVariant(result_type))
        return false;

    bool contains_dynamic_or_variant = false;
    result_type.forEachChild([&](const IDataType & child)
    {
        contains_dynamic_or_variant |= isDynamic(child) || isVariant(child);
    });

    return !contains_dynamic_or_variant;
}

std::optional<JSONAllValuesIndexInfo> tryMatchNodeToJSONAllValuesIndex(
    const RPNBuilderTreeNode & node,
    const Names & index_columns)
{
    if (node.isFunction())
    {
        auto function = node.toFunctionNode();
        const auto function_name = function.getFunctionName();
        if ((function_name == "CAST" || function_name == "_CAST") && function.getArgumentsSize() == 2)
        {
            auto argument = function.getArgumentAt(0);
            auto json_info = tryMatchJSONSubcolumnToIndex(argument.getColumnName(), index_columns, "JSONAllValues");
            if (!json_info)
                return std::nullopt;

            const auto * node_dag = node.getDAGNode();
            const auto * argument_dag = argument.getDAGNode();
            if (!node_dag || !argument_dag)
                return std::nullopt;

            const bool is_identity_cast = node_dag->result_type->equals(*argument_dag->result_type);
            if (!is_identity_cast && node_dag->result_type->getTypeId() != TypeIndex::String)
                return std::nullopt;

            const auto match_kind = is_identity_cast
                ? JSONAllValuesMatchKind::IdentityCast
                : JSONAllValuesMatchKind::StringCast;

            if (!isJSONAllValuesMatchSafe(node, match_kind))
                return std::nullopt;

            return JSONAllValuesIndexInfo{std::move(*json_info), match_kind, argument_dag->result_type};
        }
    }

    if (auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns, "JSONAllValues"))
    {
        constexpr auto match_kind = JSONAllValuesMatchKind::Direct;
        if (isJSONAllValuesMatchSafe(node, match_kind))
            return JSONAllValuesIndexInfo{std::move(*json_info), match_kind, node.getDAGNode()->result_type};
    }

    return std::nullopt;
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

Field tryConvertJSONValueToType(
    const Field & value,
    const DataTypePtr & source_type,
    const DataTypePtr & target_type)
{
    const auto * source_array = typeid_cast<const DataTypeArray *>(source_type.get());
    const auto * target_array = typeid_cast<const DataTypeArray *>(target_type.get());
    if (source_array && target_array && value.getType() == Field::Types::Array)
    {
        const auto & source_values = value.safeGet<Array>();
        Array converted_values;
        converted_values.reserve(source_values.size());

        for (const auto & source_value : source_values)
        {
            auto converted_value = tryConvertJSONValueToType(
                source_value, source_array->getNestedType(), target_array->getNestedType());
            if (converted_value.isNull() && !canContainNull(*target_array->getNestedType()))
                return {};

            converted_values.emplace_back(std::move(converted_value));
        }

        return converted_values;
    }

    return tryConvertFieldToType(value, *target_type, source_type.get());
}

String serializeJSONValueAsText(const Field & value, const DataTypePtr & type)
{
    auto column = type->createColumn();
    column->insert(value);
    WriteBufferFromOwnString buf;
    type->getDefaultSerialization()->serializeText(*column, 0, buf, {});
    return buf.str();
}

bool isJSONAllValuesStringCastSourceDefault(
    JSONAllValuesMatchKind match_kind,
    const DataTypePtr & source_type,
    const Field & value)
{
    if (match_kind != JSONAllValuesMatchKind::StringCast || canContainNull(*source_type))
        return false;

    return value == Field(serializeJSONValueAsText(source_type->getDefault(), source_type));
}

}
