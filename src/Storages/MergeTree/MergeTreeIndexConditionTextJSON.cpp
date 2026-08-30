#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Common/StringUtils.h>
#include <Common/likePatternToRegexp.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/JSONPathValues.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>

#include <cmath>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 text_index_like_min_pattern_length;
    extern const SettingsBool use_text_index_like_evaluation_by_dictionary_scan;
    extern const SettingsBool dynamic_throw_on_type_mismatch;
}

static bool containsFloat(const IDataType & type)
{
    if (WhichDataType(type).isFloat())
        return true;

    bool result = false;
    type.forEachChild([&](const IDataType & child)
    {
        result |= containsFloat(child);
    });
    return result;
}

static bool hasStableJSONPathValuesSerialization(const IDataType & type)
{
    bool result = true;
    auto check = [&](const IDataType & current_type)
    {
        if (const auto * date_time = typeid_cast<const DataTypeDateTime *>(&current_type))
            result &= date_time->hasExplicitTimeZone();
        else if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(&current_type))
            result &= date_time64->hasExplicitTimeZone();
    };

    check(type);
    type.forEachChild(check);
    return result;
}

static String escapeLikePatternLiteral(std::string_view value)
{
    String result;
    result.reserve(value.size());
    for (const char byte : value)
    {
        if (byte == '\\' || byte == '%' || byte == '_')
            result += '\\';
        result += byte;
    }
    return result;
}

static OptimizedRegularExpression createJSONValuePattern(const String & value_pattern, bool case_insensitive)
{
    if (case_insensitive)
        return Regexps::createRegexp<true, true, true>(value_pattern);
    return Regexps::createRegexp<true, true, false>(value_pattern);
}

static String createJSONTokenPrefix(
    std::string_view path_type_prefix,
    JSONPathValues::Kind kind,
    std::string_view value_prefix)
{
    String prefix(path_type_prefix);
    prefix += static_cast<char>(kind);
    prefix += value_prefix;
    return prefix;
}

static const DataTypes & getJSONPathValuesDynamicNumberTypes()
{
    static const DataTypes types{
        DataTypeFactory::instance().get("Bool"),
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeFloat64>(),
    };
    return types;
}

static bool isSupportedJSONPathValuesMap(const DataTypePtr & type)
{
    if (!type)
        return false;

    const auto nested_type = removeNullableOrLowCardinalityNullable(type);
    const auto *const map_type = typeid_cast<const DataTypeMap *>(nested_type.get());
    if (!map_type
        || !WhichDataType(removeLowCardinality(map_type->getKeyType())).isString()
        || !WhichDataType(removeLowCardinality(map_type->getValueType())).isString())
        return false;
    return true;
}

static bool hasUnindexedJSONPathValuesTypedAncestor(const DataTypeObject & object_type, std::string_view path)
{
    for (const auto & [prefix, type] : object_type.getTypedPaths())
    {
        if (path.size() <= prefix.size() || !path.starts_with(prefix))
            continue;

        std::string_view remaining = path.substr(prefix.size());
        auto nested_type = removeLowCardinality(removeNullableOrLowCardinalityNullable(type));
        if (remaining.starts_with("[]"))
        {
            while (remaining.starts_with("[]"))
            {
                const auto * array_type = typeid_cast<const DataTypeArray *>(nested_type.get());
                if (!array_type)
                    return true;
                nested_type = removeLowCardinality(
                    removeNullableOrLowCardinalityNullable(array_type->getNestedType()));
                remaining.remove_prefix(2);
            }
        }
        else if (!remaining.starts_with('.'))
            continue;

        if (remaining.starts_with('.'))
            remaining.remove_prefix(1);
        if (remaining.empty())
            continue;

        const auto * nested_object_type = typeid_cast<const DataTypeObject *>(nested_type.get());
        if (!nested_object_type)
            return true;

        if (hasUnindexedJSONPathValuesTypedAncestor(*nested_object_type, remaining))
            return true;
    }

    return false;
}

static bool appendJSONPathValuesDynamicEqualityTokens(
    std::string_view path,
    const DataTypePtr & literal_type,
    const Field & literal,
    size_t max_token_bytes,
    VectorWithMemoryTracking<String> & tokens,
    VectorWithMemoryTracking<String> & validation_tokens)
{
    auto append_encoded = [&](std::optional<JSONPathValues::EncodedValue> encoded, bool always_validate = false)
    {
        if (!encoded)
            return false;
        if (!encoded->complete || always_validate)
            validation_tokens.emplace_back(encoded->token);
        tokens.emplace_back(std::move(encoded->token));
        return true;
    };

    auto append_validation_marker = [&]
    {
        auto marker = JSONPathValues::encodeDynamicValidation(path, max_token_bytes);
        if (!marker)
            return false;
        validation_tokens.emplace_back(*marker);
        tokens.emplace_back(std::move(*marker));
        return true;
    };

    const auto which_literal = WhichDataType(literal_type);
    if (which_literal.isArray())
        return false;

    /// Only plain String literals: FixedString values carry zero padding that is not
    /// part of the exact tokens the index stores.
    if (which_literal.isString())
    {
        const auto & value = literal.safeGet<String>();
        const auto string_type = std::make_shared<DataTypeString>();
        if (!append_encoded(JSONPathValues::encodeValue(path, string_type, value, max_token_bytes)))
            return false;

        for (const auto & target_type : getJSONPathValuesDynamicNumberTypes())
        {
            Field converted = tryConvertFieldToType(literal, *target_type, literal_type.get());
            if (converted.isNull())
                continue;

            const String converted_text = serializeFieldAsText(converted, target_type);
            if (converted_text != value)
                return false;

            auto append_converted = [&](const Field & converted_value)
            {
                return append_encoded(JSONPathValues::encodeValue(
                    path,
                    target_type,
                    serializeFieldAsText(converted_value, target_type),
                    max_token_bytes));
            };

            if (WhichDataType(target_type).isFloat64() && converted.safeGet<Float64>() == 0)
            {
                if (!append_converted(Field(0.0)) || !append_converted(Field(-0.0)))
                    return false;
            }
            else if (!append_converted(converted))
                return false;
        }

        return append_validation_marker();
    }

    if (!which_literal.isNativeNumber() && !isBool(literal_type))
        return false;

    const String literal_text = serializeFieldAsText(literal, literal_type);
    for (const auto & target_type : getJSONPathValuesDynamicNumberTypes())
    {
        if (isBool(target_type))
        {
            std::optional<std::string_view> boolean_text;
            if (literal_text == "true" || literal_text == "1")
                boolean_text = "true";
            else if (literal_text == "false" || literal_text == "0" || literal_text == "-0")
                boolean_text = "false";

            if (boolean_text
                && !append_encoded(JSONPathValues::encodeValue(
                    path, target_type, *boolean_text, max_token_bytes)))
                return false;
            continue;
        }

        Field converted = convertFieldToType(literal, *target_type, literal_type.get(), {}, true);
        if (converted.isNull())
            continue;

        Field round_trip = convertFieldToType(converted, *literal_type, target_type.get(), {}, true);
        if (round_trip.isNull())
            continue;
        const bool is_signed_zero_round_trip
            = WhichDataType(literal_type).isFloat()
            && literal.safeGet<Float64>() == 0
            && round_trip.safeGet<Float64>() == 0;
        if (!is_signed_zero_round_trip
            && serializeFieldAsText(round_trip, literal_type) != literal_text)
            continue;

        auto append_converted = [&](const Field & value)
        {
            auto encoded = JSONPathValues::encodeValue(
                path,
                target_type,
                serializeFieldAsText(value, target_type),
                max_token_bytes);
            return append_encoded(std::move(encoded));
        };

        if (WhichDataType(target_type).isFloat64() && converted.safeGet<Float64>() == 0)
        {
            if (!append_converted(Field(0.0)) || !append_converted(Field(-0.0)))
                return false;
        }
        else if (!append_converted(converted))
            return false;
    }

    return append_validation_marker();
}

bool MergeTreeIndexConditionText::traverseJSONPathValuesFunction(
    const RPNBuilderFunctionTreeNode & function_node,
    const RPNBuilderTreeNode & index_column_node,
    DataTypePtr value_type,
    Field value_field,
    RPNElement & out) const
{
    if (!json_path_values_configuration
        || tokenizer->getType() != ITokenizer::Type::JSONPathValues
        || (preprocessor && preprocessor->hasActions()))
        return false;

    auto json_node_info = tryMatchJSONPathValuesNode(index_column_node);
    if (!json_node_info)
        return false;
    const auto & json_info = json_node_info->subcolumn;
    out.json_path = json_info.path;

    const String function_name = function_node.getFunctionName();
    if ((function_name == "like" || function_name == "ilike") && function_node.getArgumentsSize() == 3)
        return false;

    const bool has_multiple_needles = function_name == "multiSearchAny" || function_name == "multiSearchAnyUTF8";
    if (function_name != "equals"
        && function_name != "has"
        && function_name != "mapContains"
        && function_name != "mapContainsKey"
        && function_name != "startsWith"
        && function_name != "endsWith"
        && function_name != "like"
        && function_name != "ilike"
        && function_name != "match"
        && !has_multiple_needles)
        return false;

    if (value_field.isNull())
        return false;

    /// `startsWith` uses a binary search over the ordered token dictionary instead of scanning it.
    if (function_name != "equals"
        && function_name != "has"
        && function_name != "mapContains"
        && function_name != "mapContainsKey"
        && function_name != "startsWith"
        && !getContext()->getSettingsRef()[Setting::use_text_index_like_evaluation_by_dictionary_scan])
        return false;

    DataTypePtr declared_type = json_path_values_configuration->json_type->tryGetSubcolumnType(json_info.path);
    if (declared_type)
        declared_type = removeNullableOrLowCardinalityNullable(declared_type);

    /// FixedString literals carry zero padding that participates in zero-pad-aware string
    /// comparison but not in the exact tokens the index stores. Equal-width FixedString
    /// representations have identical padding and are safe to compare through the index.
    if (value_type)
    {
        const auto * value_fixed_string = typeid_cast<const DataTypeFixedString *>(
            removeLowCardinalityAndNullable(value_type).get());
        if (value_fixed_string)
        {
            const auto * declared_fixed_string = typeid_cast<const DataTypeFixedString *>(declared_type.get());
            if (!declared_fixed_string || value_fixed_string->getN() != declared_fixed_string->getN())
                return false;
        }
    }

    const auto * map_type = declared_type && isSupportedJSONPathValuesMap(declared_type)
        ? typeid_cast<const DataTypeMap *>(declared_type.get())
        : nullptr;
    const size_t max_token_bytes = json_path_values_configuration->max_token_bytes;

    if (json_node_info->map_key)
    {
        if (function_name != "equals" || !map_type || value_field.getType() != Field::Types::String)
            return false;

        const auto & value = value_field.safeGet<String>();
        if (value.empty())
            return false;

        auto encoded = JSONPathValues::encodeMapEntry(
            JSONPathValues::encodePathTypePrefix(json_info.path, declared_type),
            *json_node_info->map_key,
            value,
            max_token_bytes);
        if (!encoded)
            return false;

        std::optional<JSONTextQueryPayload> payload;
        if (!encoded->complete)
            payload = JSONTextQueryPayload{
                .validation_tokens = VectorWithMemoryTracking<String>{encoded->token}};

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::All,
            TextIndexDirectReadMode::Exact,
            VectorWithMemoryTracking<String>{std::move(encoded->token)},
            std::vector<OptimizedRegularExpression>{},
            std::move(payload)));
        return true;
    }

    if (map_type
        && (function_name == "has" || function_name == "mapContains" || function_name == "mapContainsKey"))
    {
        if (value_field.getType() != Field::Types::String)
            return false;

        const String map_prefix = JSONPathValues::encodePathTypePrefix(json_info.path, declared_type);
        auto complete_prefix = JSONPathValues::encodeMapEntryPrefix(
            map_prefix, value_field.safeGet<String>(), JSONPathValues::Kind::MapEntryComplete, max_token_bytes);
        auto truncated_prefix = JSONPathValues::encodeMapEntryPrefix(
            map_prefix, value_field.safeGet<String>(), JSONPathValues::Kind::MapEntryTruncated, max_token_bytes);
        if (!complete_prefix || !truncated_prefix
            || complete_prefix->size() + JSONPathValues::VALUE_HASH_BYTES > max_token_bytes)
            return false;

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::Any,
            TextIndexDirectReadMode::Exact,
            VectorWithMemoryTracking<String>{},
            std::vector<OptimizedRegularExpression>{},
            JSONTextQueryPayload{
                .pattern_token_prefixes = {std::move(*complete_prefix), std::move(*truncated_prefix)},
                .match_patterns_by_prefix = true}));
        return true;
    }

    if (function_name == "has")
    {
        const auto * dag_node = index_column_node.getDAGNode();
        if (!dag_node)
            return false;

        const auto expression_type = removeNullableOrLowCardinalityNullable(dag_node->result_type);
        const auto * array_type = typeid_cast<const DataTypeArray *>(expression_type.get());
        if (!array_type || array_type->getNestedType()->hasDynamicStructure())
            return false;

        const auto & array_nested_type = array_type->getNestedType();
        const auto nested_value_type = removeLowCardinalityAndNullable(array_nested_type);
        const WhichDataType nested_which(*nested_value_type);
        if (nested_which.isArray()
            || nested_which.isMap()
            || nested_which.isTuple()
            || nested_which.isObject()
            || !hasStableJSONPathValuesSerialization(*array_nested_type))
            return false;

        DataTypePtr token_array_type = expression_type;
        if (json_node_info->array_json_levels != 0)
        {
            if (json_node_info->array_json_levels != 1)
                return false;
            const auto element_type = removeNullableOrLowCardinalityNullable(array_nested_type);
            const auto base_type = removeLowCardinality(element_type);
            const WhichDataType which(*base_type);
            if (which.isArray() || which.isMap() || which.isTuple() || which.isObject() || base_type->hasDynamicStructure())
                return false;
            token_array_type = std::make_shared<DataTypeArray>(element_type);
        }
        const String array_prefix = JSONPathValues::encodePathTypePrefix(json_info.path, token_array_type);
        std::optional<JSONTextQueryPayload> payload;
        Field converted = convertFieldToType(value_field, *array_nested_type, value_type.get(), {}, true);
        if (converted.isNull())
            return false;

        const bool nested_is_float = WhichDataType(nested_value_type).isFloat();
        if (containsFloat(*array_nested_type) && !nested_is_float)
            return false;

        VectorWithMemoryTracking<String> tokens;
        VectorWithMemoryTracking<String> validation_tokens;
        const auto complete_kind = json_node_info->array_json_levels == 0
            ? JSONPathValues::Kind::ArrayElementComplete
            : JSONPathValues::Kind::ScalarComplete;
        const auto truncated_kind = json_node_info->array_json_levels == 0
            ? JSONPathValues::Kind::ArrayElementTruncated
            : JSONPathValues::Kind::ScalarTruncated;
        auto append_token = [&](const Field & field)
        {
            const String value = WhichDataType(nested_value_type).isStringOrFixedString()
                ? field.safeGet<String>()
                : serializeFieldAsText(field, array_nested_type);
            if (value.empty()
                && WhichDataType(nested_value_type).isStringOrFixedString())
                return false;
            auto encoded = JSONPathValues::encodeValue(
                array_prefix,
                value,
                json_path_values_configuration->max_token_bytes,
                true,
                complete_kind,
                truncated_kind);
            if (!encoded)
                return false;

            if (!encoded->complete)
                validation_tokens.emplace_back(encoded->token);
            tokens.emplace_back(std::move(encoded->token));
            return true;
        };

        if (nested_is_float)
        {
            const auto float_value = converted.safeGet<Float64>();
            if (!std::isfinite(float_value))
                return false;

            if (float_value == 0)
            {
                if (!append_token(Field(0.0)) || !append_token(Field(-0.0)))
                    return false;
            }
            else if (!append_token(converted))
                return false;
        }
        else if (!append_token(converted))
            return false;

        out.function = RPNElement::FUNCTION_EQUALS;
        if (!validation_tokens.empty())
        {
            payload.emplace();
            payload->validation_tokens = std::move(validation_tokens);
        }
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            tokens.size() == 1 ? TextSearchMode::All : TextSearchMode::Any,
            TextIndexDirectReadMode::Exact,
            std::move(tokens),
            std::vector<OptimizedRegularExpression>{},
            std::move(payload)));
        return true;
    }

    if (json_node_info->array_json_levels != 0)
        return false;

    auto type = removeNullable(value_type);
    bool expression_is_dynamic = false;
    const auto * dag_node = index_column_node.getDAGNode();
    if (!dag_node)
        return false;

    auto expression_type = removeNullableOrLowCardinalityNullable(
        json_node_info->source_type ? json_node_info->source_type : dag_node->result_type);
    expression_is_dynamic = expression_type->getTypeId() == TypeIndex::Dynamic;
    if (!expression_is_dynamic)
    {
        if (WhichDataType(expression_type).isArray() || WhichDataType(expression_type).isMap())
            return false;

        if (function_name == "equals" && !hasStableJSONPathValuesSerialization(*expression_type))
            return false;

        if (function_name == "equals")
        {
            Field converted = convertFieldToType(value_field, *expression_type, value_type.get(), {}, true);
            if (converted.isNull())
                return false;
            value_field = std::move(converted);
        }
        type = std::move(expression_type);
    }

    if (expression_is_dynamic
        && getContext()->getSettingsRef()[Setting::dynamic_throw_on_type_mismatch])
        return false;

    if (expression_is_dynamic && has_multiple_needles)
        type = std::make_shared<DataTypeString>();

    if (WhichDataType(type).isObject())
        return false;

    if (has_multiple_needles && !WhichDataType(type).isStringOrFixedString())
        return false;

    if (function_name == "equals" && expression_is_dynamic)
    {
        if (WhichDataType(type).isStringOrFixedString()
            && value_field.safeGet<String>().empty())
            return false;

        VectorWithMemoryTracking<String> tokens;
        VectorWithMemoryTracking<String> validation_tokens;
        if (!appendJSONPathValuesDynamicEqualityTokens(
                json_info.path, type, value_field, max_token_bytes, tokens, validation_tokens))
            return false;

        JSONTextQueryPayload payload{
            .match_patterns_by_prefix = true,
            .validation_tokens = std::move(validation_tokens)};
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::Any,
            TextIndexDirectReadMode::Exact,
            std::move(tokens),
            std::vector<OptimizedRegularExpression>{},
            std::move(payload)));
        return true;
    }

    const bool type_is_string = WhichDataType(removeLowCardinalityAndNullable(type)).isString();
    const String path_type_prefix = JSONPathValues::encodePathTypePrefix(json_info.path, type);
    if (path_type_prefix.size() + 1 > max_token_bytes)
        return false;

    if (function_name == "equals" && containsFloat(*type))
    {
        if (!WhichDataType(removeLowCardinalityAndNullable(type)).isFloat())
            return false;

        const auto float_value = value_field.safeGet<Float64>();
        if (!std::isfinite(float_value))
            return false;

        if (float_value == 0)
        {
            VectorWithMemoryTracking<String> tokens;
            VectorWithMemoryTracking<String> validation_tokens;
            for (const auto value : {0.0, -0.0})
            {
                auto encoded = JSONPathValues::encodeValue(
                    json_info.path, type, serializeFieldAsText(Field(value), type), max_token_bytes);
                if (!encoded)
                    return false;
                if (!encoded->complete)
                    validation_tokens.emplace_back(encoded->token);
                tokens.emplace_back(std::move(encoded->token));
            }

            out.function = RPNElement::FUNCTION_EQUALS;
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
                function_name,
                TextSearchMode::Any,
                TextIndexDirectReadMode::Exact,
                std::move(tokens),
                std::vector<OptimizedRegularExpression>{},
                validation_tokens.empty()
                    ? std::nullopt
                    : std::optional<JSONTextQueryPayload>(JSONTextQueryPayload{
                        .validation_tokens = std::move(validation_tokens)})));
            return true;
        }
    }

    const String value = has_multiple_needles
        ? String{}
        : WhichDataType(removeLowCardinalityAndNullable(type)).isStringOrFixedString()
            ? value_field.safeGet<String>()
            : serializeFieldAsText(value_field, type);

    const size_t min_pattern_length = getContext()->getSettingsRef()[Setting::text_index_like_min_pattern_length];
    if ((function_name == "startsWith" || function_name == "endsWith")
        && value.size() < min_pattern_length)
        return false;

    if (function_name == "equals")
    {
        if (type_is_string && value.empty())
            return false;

        auto encoded = JSONPathValues::encodeValue(json_info.path, type, value, max_token_bytes);
        if (!encoded)
            return false;

        VectorWithMemoryTracking<String> tokens{encoded->token};
        std::optional<JSONTextQueryPayload> payload;
        if (!encoded->complete)
            payload = JSONTextQueryPayload{
                .validation_tokens = VectorWithMemoryTracking<String>{encoded->token}};
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::All,
            TextIndexDirectReadMode::Exact,
            std::move(tokens),
            std::vector<OptimizedRegularExpression>{},
            std::move(payload)));
        return true;
    }

    if (path_type_prefix.size() + 1 + JSONPathValues::VALUE_HASH_BYTES > max_token_bytes)
        return false;

    VectorWithMemoryTracking<String> tokens;
    std::vector<OptimizedRegularExpression> patterns;
    const String complete_prefix = createJSONTokenPrefix(
        path_type_prefix, JSONPathValues::Kind::ScalarComplete, {});
    const String truncated_prefix = createJSONTokenPrefix(
        path_type_prefix, JSONPathValues::Kind::ScalarTruncated, {});

    auto create_pattern_query = [&](const std::vector<String> & value_patterns, TextIndexDirectReadMode direct_read_mode)
    {
        patterns.reserve(value_patterns.size());
        for (const auto & value_pattern : value_patterns)
        {
            patterns.emplace_back(createJSONValuePattern(value_pattern, function_name == "ilike"));
        }

        out.function = RPNElement::FUNCTION_LIKE;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::Any,
            direct_read_mode,
            std::move(tokens),
            std::move(patterns),
            JSONTextQueryPayload{
                .pattern_token_prefixes = {complete_prefix, truncated_prefix},
                .validation_pattern_prefixes = {truncated_prefix}}));
        return true;
    };

    auto create_prefix_query = [&](const String & prefix)
    {
        if (prefix.empty())
            return false;

        std::vector<String> token_prefixes;
        token_prefixes.emplace_back(createJSONTokenPrefix(
            path_type_prefix,
            JSONPathValues::Kind::ScalarComplete,
            prefix));
        token_prefixes.emplace_back(truncated_prefix);

        out.function = RPNElement::FUNCTION_LIKE;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::Any,
            TextIndexDirectReadMode::Exact,
            std::move(tokens),
            std::vector<OptimizedRegularExpression>{},
            JSONTextQueryPayload{
                .pattern_token_prefixes = std::move(token_prefixes),
                .match_patterns_by_prefix = true,
                .validation_pattern_prefixes = {truncated_prefix}}));
        return true;
    };

    auto create_case_insensitive_prefix_query = [&](const String & prefix)
    {
        if (prefix.empty())
            return false;

        const String value_pattern = escapeLikePatternLiteral(prefix) + "%";
        patterns.emplace_back(createJSONValuePattern(value_pattern, true));

        out.function = RPNElement::FUNCTION_LIKE;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
            function_name,
            TextSearchMode::Any,
            TextIndexDirectReadMode::Exact,
            std::move(tokens),
            std::move(patterns),
            JSONTextQueryPayload{
                .pattern_token_prefixes = {complete_prefix, truncated_prefix},
                .validation_pattern_prefixes = {truncated_prefix}}));
        return true;
    };

    if (function_name == "startsWith")
        return create_prefix_query(value);

    if (function_name == "endsWith")
    {
        if (value.empty())
            return false;

        return create_pattern_query(
            {"%" + escapeLikePatternLiteral(value)},
            TextIndexDirectReadMode::Exact);
    }

    if (function_name == "like" || function_name == "ilike")
    {
        const bool case_insensitive = function_name == "ilike";
        if ((case_insensitive
                ? Regexps::createRegexp</*like=*/ true, /*no_capture=*/ true, /*case_insensitive=*/ true>(value).match("", 0)
                : Regexps::createRegexp</*like=*/ true, /*no_capture=*/ true, /*case_insensitive=*/ false>(value).match("", 0)))
            return false;

        auto prefix = extractFixedPrefixFromLikePattern(value, true);
        if (prefix.is_perfect && prefix.prefix.size() >= min_pattern_length
            && (case_insensitive ? create_case_insensitive_prefix_query(prefix.prefix) : create_prefix_query(prefix.prefix)))
            return true;
    }

    if (function_name == "match")
    {
        if (Regexps::createRegexp</*like=*/ false, /*no_capture=*/ true, /*case_insensitive=*/ false>(value).match("", 0))
            return false;

        const auto analysis = OptimizedRegularExpression::analyze(value);
        if (analysis.required_substring.size() < min_pattern_length)
            return false;

        return create_pattern_query(
            {"%" + escapeLikePatternLiteral(analysis.required_substring) + "%"},
            getHintOrNoneMode());
    }

    if (has_multiple_needles)
    {
        if (value_field.getType() != Field::Types::Array)
            return false;

        const auto & needles = value_field.safeGet<Array>();
        checkMultiSearchNeedlesLimit(function_name, needles.size());
        if (needles.empty())
        {
            out.function = RPNElement::ALWAYS_FALSE;
            return true;
        }

        std::vector<String> value_patterns;
        value_patterns.reserve(needles.size());
        for (const auto & needle : needles)
        {
            if (needle.getType() != Field::Types::String
                || needle.safeGet<String>().empty()
                || needle.safeGet<String>().size() < min_pattern_length)
                return false;

            value_patterns.emplace_back("%" + escapeLikePatternLiteral(needle.safeGet<String>()) + "%");
        }

        return create_pattern_query(value_patterns, TextIndexDirectReadMode::Exact);
    }

    String literal;
    if (!likePatternIsSubstring(value, literal) || literal.size() < min_pattern_length)
        return false;

    return create_pattern_query({value}, TextIndexDirectReadMode::Exact);
}

std::optional<MergeTreeIndexConditionText::JSONPathValuesNodeInfo>
MergeTreeIndexConditionText::tryMatchJSONPathValuesNode(const RPNBuilderTreeNode & node) const
{
    if (!json_path_values_configuration)
        return std::nullopt;

    const auto & object_type = assert_cast<const DataTypeObject &>(*json_path_values_configuration->json_type);

    auto make_map_element_info = [&](const String & map_column_name, String key)
        -> std::optional<JSONPathValuesNodeInfo>
    {
        auto map_json_info = tryMatchJSONSubcolumn(
            map_column_name, json_path_values_configuration->column_name);
        if (!map_json_info
            || !json_path_values_configuration->path_matcher->shouldIndex(map_json_info->path)
            || hasUnindexedJSONPathValuesTypedAncestor(object_type, map_json_info->path))
            return std::nullopt;
        const auto map_type = json_path_values_configuration->json_type->tryGetSubcolumnType(map_json_info->path);
        if (!isSupportedJSONPathValuesMap(map_type))
            return std::nullopt;
        const auto nested_map_type = removeNullableOrLowCardinalityNullable(map_type);
        const auto & value_type = assert_cast<const DataTypeMap &>(*nested_map_type).getValueType();
        return JSONPathValuesNodeInfo{
            .subcolumn = std::move(*map_json_info),
            .source_type = value_type,
            .map_key = std::move(key),
        };
    };

    if (node.isFunction())
    {
        const auto function = node.toFunctionNode();
        if ((function.getFunctionName() == "CAST" || function.getFunctionName() == "_CAST")
            && function.getArgumentsSize() == 2)
        {
            const auto argument = function.getArgumentAt(0);
            const auto * argument_dag = argument.getDAGNode();
            const auto * node_dag = node.getDAGNode();
            if (argument_dag && node_dag)
            {
                const auto source_type = removeNullable(argument_dag->result_type);
                const auto result_type = removeNullable(node_dag->result_type);
                const auto * low_cardinality_type
                    = typeid_cast<const DataTypeLowCardinality *>(source_type.get());
                if (low_cardinality_type
                    && low_cardinality_type->getDictionaryType()->equals(*result_type))
                    return tryMatchJSONPathValuesNode(argument);
            }
        }
    }

    if (auto map_subcolumn = tryParseMapSubcolumnName(node.getColumnName()))
    {
        auto & [map_column_name, serialized_key] = *map_subcolumn;
        if (auto info = make_map_element_info(map_column_name, std::move(serialized_key)))
            return info;
    }

    static constexpr std::string_view map_keys_suffix = ".keys";
    if (node.getColumnName().ends_with(map_keys_suffix))
    {
        String map_column_name = node.getColumnName().substr(
            0, node.getColumnName().size() - map_keys_suffix.size());
        auto map_json_info = tryMatchJSONSubcolumn(
            map_column_name, json_path_values_configuration->column_name);
        if (map_json_info
            && json_path_values_configuration->path_matcher->shouldIndex(map_json_info->path)
            && !hasUnindexedJSONPathValuesTypedAncestor(object_type, map_json_info->path))
        {
            const auto map_type = json_path_values_configuration->json_type->tryGetSubcolumnType(map_json_info->path);
            if (isSupportedJSONPathValuesMap(map_type))
                return JSONPathValuesNodeInfo{
                    .subcolumn = std::move(*map_json_info),
                    .source_type = map_type,
                    .map_key = std::nullopt,
                };
        }
    }

    if (node.isFunction())
    {
        const auto function = node.toFunctionNode();
        if (function.getFunctionName() == "arrayElement" && function.getArgumentsSize() == 2)
        {
            Field key;
            DataTypePtr key_type;
            if (function.getArgumentAt(1).tryGetConstant(key, key_type)
                && key.getType() == Field::Types::String)
            {
                if (auto info = make_map_element_info(
                        function.getArgumentAt(0).getColumnName(), key.safeGet<String>()))
                    return info;
            }
        }

        return std::nullopt;
    }

    auto json_info = tryMatchJSONSubcolumn(
        node.getColumnName(), json_path_values_configuration->column_name);
    if (json_info)
    {
        if (!json_path_values_configuration->path_matcher->shouldIndex(json_info->path)
            || hasUnindexedJSONPathValuesTypedAncestor(object_type, json_info->path))
            return std::nullopt;
        return JSONPathValuesNodeInfo{
            .subcolumn = std::move(*json_info),
            .source_type = node.getDAGNode() ? node.getDAGNode()->result_type : nullptr,
            .map_key = std::nullopt,
            .array_json_levels = json_info->array_json_levels,
        };
    }
    return std::nullopt;
}

bool MergeTreeIndexConditionText::tryPrepareJSONPathValuesSet(
    const RPNBuilderTreeNode & index_column_node,
    const String & function_name,
    const std::vector<String> & values,
    RPNElement & out,
    TextIndexDirectReadMode direct_read_mode) const
{
    auto json_index_info = tryMatchJSONPathValuesNode(index_column_node);
    if (!json_index_info
        || json_index_info->map_key
        || json_index_info->array_json_levels != 0
        || values.empty())
        return false;
    out.json_path = json_index_info->subcolumn.path;

    auto type = removeNullableOrLowCardinalityNullable(json_index_info->source_type);
    if (!type)
        return false;

    const size_t max_token_bytes = json_path_values_configuration->max_token_bytes;
    VectorWithMemoryTracking<String> tokens;
    VectorWithMemoryTracking<String> validation_tokens;

    if (!WhichDataType(removeLowCardinality(type)).isStringOrFixedString())
        return false;

    const String path_type_prefix = JSONPathValues::encodePathTypePrefix(json_index_info->subcolumn.path, type);
    if (path_type_prefix.size() + 1 > max_token_bytes)
        return false;

    tokens.reserve(values.size());
    for (const auto & value : values)
    {
        if (value.empty())
            return false;

        auto encoded = JSONPathValues::encodeValue(
            json_index_info->subcolumn.path, type, value, max_token_bytes);
        if (!encoded)
            return false;
        if (!encoded->complete)
            validation_tokens.emplace_back(encoded->token);
        tokens.emplace_back(std::move(encoded->token));
    }

    out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
        function_name,
        TextSearchMode::Any,
        direct_read_mode,
        std::move(tokens),
        std::vector<OptimizedRegularExpression>{},
        validation_tokens.empty()
            ? std::nullopt
            : std::optional<JSONTextQueryPayload>(JSONTextQueryPayload{
                .validation_tokens = std::move(validation_tokens)})));
    return true;
}

}
