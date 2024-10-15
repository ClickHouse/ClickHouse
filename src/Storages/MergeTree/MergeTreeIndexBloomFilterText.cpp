#include <Storages/MergeTree/MergeTreeIndexBloomFilterText.h>

#include <Columns/ColumnArray.h>
#include <Common/OptimizedRegularExpression.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexUtils.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}

MergeTreeIndexGranuleBloomFilterText::MergeTreeIndexGranuleBloomFilterText(
    const String & index_name_,
    size_t columns_number,
    const BloomFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , bloom_filters(
        columns_number, BloomFilter(params))
    , has_elems(false)
{
}

void MergeTreeIndexGranuleBloomFilterText::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty fulltext index {}.", backQuote(index_name));

    for (const auto & bloom_filter : bloom_filters)
        ostr.write(reinterpret_cast<const char *>(bloom_filter.getFilter().data()), params.filter_size);
}

void MergeTreeIndexGranuleBloomFilterText::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    for (auto & bloom_filter : bloom_filters)
    {
        istr.readStrict(reinterpret_cast<char *>(bloom_filter.getFilter().data()), params.filter_size);
    }
    has_elems = true;
}


MergeTreeIndexAggregatorBloomFilterText::MergeTreeIndexAggregatorBloomFilterText(
    const Names & index_columns_,
    const String & index_name_,
    const BloomFilterParameters & params_,
    TokenExtractorPtr token_extractor_)
    : index_columns(index_columns_)
    , index_name (index_name_)
    , params(params_)
    , token_extractor(token_extractor_)
    , granule(
        std::make_shared<MergeTreeIndexGranuleBloomFilterText>(
            index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBloomFilterText::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleBloomFilterText>(
        index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorBloomFilterText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column_with_type = block.getByName(index_columns[col]);
        const auto & column = column_with_type.column;
        size_t current_position = *pos;

        if (isArray(column_with_type.type))
        {
            const auto & column_array = assert_cast<const ColumnArray &>(*column);
            const auto & column_offsets = column_array.getOffsets();
            const auto & column_key = column_array.getData();

            for (size_t i = 0; i < rows_read; ++i)
            {
                size_t element_start_row = column_offsets[current_position - 1];
                size_t elements_size = column_offsets[current_position] - element_start_row;

                for (size_t row_num = 0; row_num < elements_size; ++row_num)
                {
                    auto ref = column_key.getDataAt(element_start_row + row_num);
                    token_extractor->stringPaddedToBloomFilter(ref.data, ref.size, granule->bloom_filters[col]);
                }

                current_position += 1;
            }
        }
        else
        {
            for (size_t i = 0; i < rows_read; ++i)
            {
                auto ref = column->getDataAt(current_position + i);
                token_extractor->stringPaddedToBloomFilter(ref.data, ref.size, granule->bloom_filters[col]);
            }
        }
    }

    granule->has_elems = true;
    *pos += rows_read;
}

MergeTreeConditionBloomFilterText::MergeTreeConditionBloomFilterText(
    const ActionsDAG * filter_actions_dag,
    ContextPtr context,
    const Block & index_sample_block,
    const BloomFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    : index_columns(index_sample_block.getNames())
    , index_data_types(index_sample_block.getNamesAndTypesList().getTypes())
    , params(params_)
    , token_extractor(token_extactor_)
{
    if (!filter_actions_dag)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        filter_actions_dag->getOutputs().at(0),
        context,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

/// Keep in-sync with MergeTreeConditionGinFilter::alwaysUnknownOrTrue
bool MergeTreeConditionBloomFilterText::alwaysUnknownOrTrue() const
{
    /// Check like in KeyCondition.
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS
             || element.function == RPNElement::FUNCTION_HAS
             || element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN
             || element.function == RPNElement::FUNCTION_MULTI_SEARCH
             || element.function == RPNElement::FUNCTION_MATCH
             || element.function == RPNElement::FUNCTION_HAS_ANY
             || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            // do nothing
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 && arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 || arg2;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    return rpn_stack[0];
}

/// Keep in-sync with MergeTreeIndexConditionGin::mayBeTrueOnTranuleInPart
bool MergeTreeConditionBloomFilterText::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleBloomFilterText> granule
            = std::dynamic_pointer_cast<MergeTreeIndexGranuleBloomFilterText>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BloomFilter index condition got a granule with the wrong type.");

    /// Check like in KeyCondition.
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS
             || element.function == RPNElement::FUNCTION_HAS)
        {
            rpn_stack.emplace_back(granule->bloom_filters[element.key_column].contains(*element.bloom_filter), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.set_bloom_filters.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const size_t key_idx = element.set_key_position[column];

                const auto & bloom_filters = element.set_bloom_filters[column];
                for (size_t row = 0; row < bloom_filters.size(); ++row)
                    result[row] = result[row] && granule->bloom_filters[key_idx].contains(bloom_filters[row]);
            }

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH
            || element.function == RPNElement::FUNCTION_HAS_ANY)
        {
            std::vector<bool> result(element.set_bloom_filters.back().size(), true);

            const auto & bloom_filters = element.set_bloom_filters[0];

            for (size_t row = 0; row < bloom_filters.size(); ++row)
                result[row] = result[row] && granule->bloom_filters[element.key_column].contains(bloom_filters[row]);

            rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
        }
        else if (element.function == RPNElement::FUNCTION_MATCH)
        {
            if (!element.set_bloom_filters.empty())
            {
                /// Alternative substrings
                std::vector<bool> result(element.set_bloom_filters.back().size(), true);

                const auto & bloom_filters = element.set_bloom_filters[0];

                for (size_t row = 0; row < bloom_filters.size(); ++row)
                    result[row] = result[row] && granule->bloom_filters[element.key_column].contains(bloom_filters[row]);

                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            }
            else if (element.bloom_filter)
            {
                /// Required substrings
                rpn_stack.emplace_back(granule->bloom_filters[element.key_column].contains(*element.bloom_filter), true);
            }
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in BloomFilterCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in BloomFilterCondition::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

std::optional<size_t> MergeTreeConditionBloomFilterText::getKeyIndex(const std::string & key_column_name)
{
    const auto it = std::ranges::find(index_columns, key_column_name);
    return it == index_columns.end() ? std::nullopt : std::make_optional<size_t>(std::ranges::distance(index_columns.cbegin(), it));
}

bool MergeTreeConditionBloomFilterText::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            /// Check constant like in KeyCondition

            if (const_value.getType() == Field::Types::UInt64)
            {
                out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Int64)
            {
                out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();
        auto function_name = function_node.getFunctionName();

        size_t arguments_size = function_node.getArgumentsSize();
        if (arguments_size != 2)
            return false;

        auto left_argument = function_node.getArgumentAt(0);
        auto right_argument = function_node.getArgumentAt(1);

        if (functionIsInOrGlobalInOperator(function_name))
        {
            if (tryPrepareSetBloomFilter(left_argument, right_argument, out))
            {
                if (function_name == "notIn")
                {
                    out.function = RPNElement::FUNCTION_NOT_IN;
                    return true;
                }
                if (function_name == "in")
                {
                    out.function = RPNElement::FUNCTION_IN;
                    return true;
                }
            }
        }
        else if (function_name == "equals" ||
                 function_name == "notEquals" ||
                 function_name == "has" ||
                 function_name == "mapContains" ||
                 function_name == "match" ||
                 function_name == "like" ||
                 function_name == "notLike" ||
                 function_name.starts_with("hasToken") ||
                 function_name == "startsWith" ||
                 function_name == "endsWith" ||
                 function_name == "multiSearchAny" ||
                 function_name == "hasAny")
        {
            Field const_value;
            DataTypePtr const_type;

            if (right_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseTreeEquals(function_name, left_argument, const_type, const_value, out))
                    return true;
            }
            else if (left_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
            {
                if (traverseTreeEquals(function_name, right_argument, const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeConditionBloomFilterText::traverseTreeEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    Field const_value = value_field;

    const auto column_name = key_node.getColumnName();
    auto key_index = getKeyIndex(column_name);
    const auto map_key_index = getKeyIndex(fmt::format("mapKeys({})", column_name));

    if (key_node.isFunction())
    {
        auto key_function_node = key_node.toFunctionNode();
        auto key_function_node_function_name = key_function_node.getFunctionName();

        if (key_function_node_function_name == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exist in the map
              * we return default the value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exist.
              */
            if (value_field == value_type->getDefault())
                return false;

            auto first_argument = key_function_node.getArgumentAt(0);
            const auto map_column_name = first_argument.getColumnName();
            if (const auto map_keys_index = getKeyIndex(fmt::format("mapKeys({})", map_column_name)))
            {
                auto second_argument = key_function_node.getArgumentAt(1);
                DataTypePtr const_type;

                if (second_argument.tryGetConstant(const_value, const_type))
                {
                    key_index = map_keys_index;

                    auto const_data_type = WhichDataType(const_type);
                    if (!const_data_type.isStringOrFixedString() && !const_data_type.isArray())
                        return false;
                }
                else
                {
                    return false;
                }
            }
            else if (const auto map_values_exists = getKeyIndex(fmt::format("mapValues({})", map_column_name)))
            {
                key_index = map_values_exists;
            }
            else
            {
                return false;
            }
        }
    }

    const auto lowercase_key_index = getKeyIndex(fmt::format("lower({})", column_name));
    const auto is_has_token_case_insensitive = function_name.starts_with("hasTokenCaseInsensitive");
    if (const auto is_case_insensitive_scenario = is_has_token_case_insensitive && lowercase_key_index;
        function_name.starts_with("hasToken") && ((!is_has_token_case_insensitive && key_index) || is_case_insensitive_scenario))
    {
        out.key_column = is_case_insensitive_scenario ? *lowercase_key_index : *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);

        auto value = const_value.safeGet<String>();
        if (is_case_insensitive_scenario)
            std::ranges::transform(value, value.begin(), [](const auto & c) { return static_cast<char>(std::tolower(c)); });

        token_extractor->stringToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }

    if (!key_index && !map_key_index)
        return false;

    if (map_key_index && (function_name == "has" || function_name == "mapContains"))
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_HAS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        auto & value = const_value.safeGet<String>();
        token_extractor->stringToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }

    if (function_name == "has")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_HAS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        auto & value = const_value.safeGet<String>();
        token_extractor->stringToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }

    if (function_name == "notEquals")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }
    if (function_name == "equals")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }
    if (function_name == "like")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }
    if (function_name == "notLike")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToBloomFilter(value.data(), value.size(), *out.bloom_filter);
        return true;
    }
    if (function_name == "startsWith")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToBloomFilter(value.data(), value.size(), *out.bloom_filter, true, false);
        return true;
    }
    if (function_name == "endsWith")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.bloom_filter = std::make_unique<BloomFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToBloomFilter(value.data(), value.size(), *out.bloom_filter, false, true);
        return true;
    }
    if (function_name == "multiSearchAny" || function_name == "hasAny")
    {
        out.key_column = *key_index;
        out.function = function_name == "multiSearchAny" ? RPNElement::FUNCTION_MULTI_SEARCH : RPNElement::FUNCTION_HAS_ANY;

        /// 2d vector is not needed here but is used because already exists for FUNCTION_IN
        std::vector<std::vector<BloomFilter>> bloom_filters;
        bloom_filters.emplace_back();
        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            bloom_filters.back().emplace_back(params);
            const auto & value = element.safeGet<String>();

            if (function_name == "multiSearchAny")
            {
                token_extractor->substringToBloomFilter(value.data(), value.size(), bloom_filters.back().back(), false, false);
            }
            else
            {
                token_extractor->stringToBloomFilter(value.data(), value.size(), bloom_filters.back().back());
            }
        }
        out.set_bloom_filters = std::move(bloom_filters);
        return true;
    }
    if (function_name == "match")
    {
        out.key_column = *key_index;
        out.function = RPNElement::FUNCTION_MATCH;
        out.bloom_filter = std::make_unique<BloomFilter>(params);

        auto & value = const_value.safeGet<String>();
        String required_substring;
        bool dummy_is_trivial, dummy_required_substring_is_prefix;
        std::vector<String> alternatives;
        OptimizedRegularExpression::analyze(value, required_substring, dummy_is_trivial, dummy_required_substring_is_prefix, alternatives);

        if (required_substring.empty() && alternatives.empty())
            return false;

        /// out.set_bloom_filters means alternatives exist
        /// out.bloom_filter means required_substring exists
        if (!alternatives.empty())
        {
            std::vector<std::vector<BloomFilter>> bloom_filters;
            bloom_filters.emplace_back();
            for (const auto & alternative : alternatives)
            {
                bloom_filters.back().emplace_back(params);
                token_extractor->substringToBloomFilter(alternative.data(), alternative.size(), bloom_filters.back().back(), false, false);
            }
            out.set_bloom_filters = std::move(bloom_filters);
        }
        else
            token_extractor->substringToBloomFilter(required_substring.data(), required_substring.size(), *out.bloom_filter, false, false);

        return true;
    }

    return false;
}

bool MergeTreeConditionBloomFilterText::tryPrepareSetBloomFilter(
    const RPNBuilderTreeNode & left_argument,
    const RPNBuilderTreeNode & right_argument,
    RPNElement & out)
{
    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    auto left_argument_function_node_optional = left_argument.toFunctionNodeOrNull();

    if (left_argument_function_node_optional && left_argument_function_node_optional->getFunctionName() == "tuple")
    {
        const auto & left_argument_function_node = *left_argument_function_node_optional;
        size_t left_argument_function_node_arguments_size = left_argument_function_node.getArgumentsSize();

        for (size_t i = 0; i < left_argument_function_node_arguments_size; ++i)
        {
            if (const auto key = getKeyIndex(left_argument_function_node.getArgumentAt(i).getColumnName()))
            {
                key_tuple_mapping.emplace_back(i, *key);
                data_types.push_back(index_data_types[*key]);
            }
        }
    }
    else if (const auto key = getKeyIndex(left_argument.getColumnName()))
    {
        key_tuple_mapping.emplace_back(0, *key);
        data_types.push_back(index_data_types[*key]);
    }

    if (key_tuple_mapping.empty())
        return false;

    auto future_set = right_argument.tryGetPreparedSet(data_types);
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(right_argument.getTreeContext().getQueryContext());
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & prepared_set_data_type : prepared_set->getDataTypes())
    {
        auto prepared_set_data_type_id = prepared_set_data_type->getTypeId();
        if (prepared_set_data_type_id != TypeIndex::String && prepared_set_data_type_id != TypeIndex::FixedString)
            return false;
    }

    std::vector<std::vector<BloomFilter>> bloom_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    size_t prepared_set_total_row_count = prepared_set->getTotalRowCount();

    for (const auto & elem : key_tuple_mapping)
    {
        bloom_filters.emplace_back();
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];

        for (size_t row = 0; row < prepared_set_total_row_count; ++row)
        {
            bloom_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            token_extractor->stringPaddedToBloomFilter(ref.data, ref.size, bloom_filters.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_bloom_filters = std::move(bloom_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilterText::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilterText>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomFilterText::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomFilterText>(index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilterText::createIndexCondition(
        const ActionsDAG * filter_dag, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionBloomFilterText>(filter_dag, context, index.sample_block, params, token_extractor.get());
}

MergeTreeIndexPtr bloomFilterIndexTextCreator(
    const IndexDescription & index)
{
    if (index.type == NgramTokenExtractor::getName())
    {
        size_t n = index.arguments[0].safeGet<size_t>();
        BloomFilterParameters params(
            index.arguments[1].safeGet<size_t>(),
            index.arguments[2].safeGet<size_t>(),
            index.arguments[3].safeGet<size_t>());

        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);

        return std::make_shared<MergeTreeIndexBloomFilterText>(index, params, std::move(tokenizer));
    }
    if (index.type == SplitTokenExtractor::getName())
    {
        BloomFilterParameters params(
            index.arguments[0].safeGet<size_t>(), index.arguments[1].safeGet<size_t>(), index.arguments[2].safeGet<size_t>());

        auto tokenizer = std::make_unique<SplitTokenExtractor>();

        return std::make_shared<MergeTreeIndexBloomFilterText>(index, params, std::move(tokenizer));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index type: {}", backQuote(index.name));
}

void bloomFilterIndexTextValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isArray())
        {
            const auto & array_type = assert_cast<const DataTypeArray &>(*index_data_type);
            data_type = WhichDataType(array_type.getNestedType());
        }
        else if (data_type.isLowCardinality())
        {
            const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index_data_type);
            data_type = WhichDataType(low_cardinality.getDictionaryType());
        }

        if (!data_type.isString() && !data_type.isFixedString() && !data_type.isIPv6())
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Ngram and token bloom filter indexes can only be used with column types `String`, `FixedString`, `LowCardinality(String)`, `LowCardinality(FixedString)`, `Array(String)` or `Array(FixedString)`");
    }

    if (index.type == NgramTokenExtractor::getName())
    {
        if (index.arguments.size() != 4)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "`ngrambf` index must have exactly 4 arguments.");
    }
    else if (index.type == SplitTokenExtractor::getName())
    {
        if (index.arguments.size() != 3)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "`tokenbf` index must have exactly 3 arguments.");
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index type: {}", backQuote(index.name));
    }

    assert(index.arguments.size() >= 3);

    for (const auto & arg : index.arguments)
        if (arg.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters to *bf_v1 index must be unsigned integers");

    /// Just validate
    BloomFilterParameters params(
        index.arguments[0].safeGet<size_t>(),
        index.arguments[1].safeGet<size_t>(),
        index.arguments[2].safeGet<size_t>());
}

}
