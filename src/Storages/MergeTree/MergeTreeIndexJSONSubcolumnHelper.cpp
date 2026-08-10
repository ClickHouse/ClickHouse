#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
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
struct ExtractedJSONPath
{
    String path;
    bool has_type_hint;
    bool has_subcolumn_after_type_hint;
};

static ExtractedJSONPath extractPathFromSubcolumn(std::string_view subcolumn_name)
{
    /// Dynamic type subcolumn looks like "some.path.:`TypeName`..."
    /// Find the ".:`" pattern that marks the start of the type specifier.
    auto pos = subcolumn_name.find(".:`");
    if (pos == std::string_view::npos)
        return {String(subcolumn_name), false, false};

    return {String(subcolumn_name.substr(0, pos)), true, subcolumn_name.find("`.", pos + 3) != std::string_view::npos};
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

        auto extracted_path = extractPathFromSubcolumn(subcolumn_part);
        if (extracted_path.path.empty())
            return std::nullopt;

        size_t position = static_cast<size_t>(std::distance(index_columns.begin(), it));

        return JSONSubcolumnIndexInfo{
            .json_column_name = String(candidate_col),
            .path = std::move(extracted_path.path),
            .header_position = position,
            .has_type_hint = extracted_path.has_type_hint,
            .has_subcolumn_after_type_hint = extracted_path.has_subcolumn_after_type_hint,
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

static String serializeJSONValueAsText(const Field & value, const DataTypePtr & type, bool serialize_quoted = false)
{
    auto column = type->createColumn();
    column->insert(value);
    WriteBufferFromOwnString buf;
    const auto & serialization = type->getDefaultSerialization();
    if (serialize_quoted)
        serialization->serializeTextQuoted(*column, 0, buf, {});
    else
        serialization->serializeText(*column, 0, buf, {});
    return buf.str();
}

static bool hasSettingSensitiveStringSerialization(const IDataType & type, const FormatSettings & settings)
{
    const bool custom_bool = settings.bool_true_representation != "true" || settings.bool_false_representation != "false";
    const bool custom_datetime = settings.date_time_output_format != FormatSettings::DateTimeOutputFormat::Simple;
    const bool custom_decimal = settings.decimal_trailing_zeros || settings.always_write_decimal_point_in_float_and_decimal;
    const bool custom_float = settings.always_write_decimal_point_in_float_and_decimal;

    if (!custom_bool && !custom_datetime && !custom_decimal && !custom_float)
        return false;

    const auto is_sensitive = [&](const IDataType & child)
    {
        const WhichDataType which(child);
        return isDynamic(child)
            || (custom_bool && child.getName() == "Bool")
            || (custom_datetime && which.isDateTimeOrDateTime64())
            || (custom_decimal && which.isDecimal())
            || (custom_float && which.isFloat());
    };

    bool result = is_sensitive(type);
    type.forEachChild([&](const IDataType & child) { result |= is_sensitive(child); });
    return result;
}

static bool isJSONAllValuesMatchSafe(
    const RPNBuilderTreeNode & node,
    bool is_string_cast)
{
    if (is_string_cast)
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
            if (!json_info || json_info->has_subcolumn_after_type_hint)
                return std::nullopt;

            const auto * node_dag = node.getDAGNode();
            const auto * argument_dag = argument.getDAGNode();
            if (!node_dag || !argument_dag)
                return std::nullopt;

            const bool is_string_cast = !node_dag->result_type->equals(*argument_dag->result_type);
            if (is_string_cast && removeNullable(node_dag->result_type)->getTypeId() != TypeIndex::String)
                return std::nullopt;

            /// Casting a nullable typed path to non-nullable `String` throws on `NULL`. Do not let
            /// granule pruning suppress `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`.
            if (is_string_cast && isNullableOrLowCardinalityNullable(argument_dag->result_type)
                && !isNullableOrLowCardinalityNullable(node_dag->result_type))
                return std::nullopt;

            /// `JSONAllValues` stores text with fixed format settings, while `CAST(... AS String)`
            /// uses query settings. Do not build a probe when those representations can differ.
            if (is_string_cast
                && hasSettingSensitiveStringSerialization(
                    *argument_dag->result_type, getFormatSettings(node.getTreeContext().getQueryContext())))
                return std::nullopt;

            if (!isJSONAllValuesMatchSafe(node, is_string_cast))
                return std::nullopt;

            std::optional<Field> unindexed_value;
            if (json_info->has_type_hint && !canContainNull(*argument_dag->result_type))
            {
                if (is_string_cast)
                    unindexed_value = serializeJSONValueAsText(
                        argument_dag->result_type->getDefault(), argument_dag->result_type);
                else
                    unindexed_value = argument_dag->result_type->getDefault();
            }
            else if (is_string_cast
                && (isDynamic(*argument_dag->result_type) || isVariant(*argument_dag->result_type))
                && !canContainNull(*node_dag->result_type))
            {
                unindexed_value = node_dag->result_type->getDefault();
            }

            return JSONAllValuesIndexInfo{std::move(*json_info), is_string_cast, std::move(unindexed_value)};
        }
    }

    if (auto json_info = tryMatchJSONSubcolumnToIndex(node.getColumnName(), index_columns, "JSONAllValues");
        json_info && !json_info->has_subcolumn_after_type_hint)
    {
        if (isJSONAllValuesMatchSafe(node, false))
        {
            const auto & result_type = node.getDAGNode()->result_type;
            std::optional<Field> unindexed_value;
            if (json_info->has_type_hint && !canContainNull(*result_type))
                unindexed_value = result_type->getDefault();
            return JSONAllValuesIndexInfo{std::move(*json_info), false, std::move(unindexed_value)};
        }
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
    const DataTypePtr & target_type,
    const FormatSettings & format_settings)
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
                source_value, source_array->getNestedType(), target_array->getNestedType(), format_settings);
            if (converted_value.isNull() && !canContainNull(*target_array->getNestedType()))
                return {};

            converted_values.emplace_back(std::move(converted_value));
        }

        return converted_values;
    }

    return tryConvertFieldToType(value, *target_type, source_type.get(), format_settings);
}

std::optional<String> tryConvertAndSerializeJSONValueAsText(
    const Field & value,
    const DataTypePtr & source_type,
    const DataTypePtr & target_type,
    const FormatSettings & format_settings,
    const std::optional<Field> & unindexed_value,
    bool serialize_quoted)
{
    Field converted_value = value;
    DataTypePtr serialization_type = source_type;
    if (target_type)
    {
        converted_value = tryConvertJSONValueToType(value, source_type, target_type, format_settings);
        if (converted_value.isNull())
            return std::nullopt;

        serialization_type = target_type;
    }

    if (unindexed_value && converted_value == *unindexed_value)
        return std::nullopt;

    return serializeJSONValueAsText(converted_value, serialization_type, serialize_quoted);
}

}
