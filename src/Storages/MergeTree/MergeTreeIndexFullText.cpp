#include <Storages/MergeTree/MergeTreeIndexFullText.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Poco/Logger.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexUtils.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/OptimizedRegularExpression.h>
#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_INDEX;
    extern const int INCORRECT_QUERY;
}

MergeTreeIndexGranuleFullText::MergeTreeIndexGranuleFullText(
    const String & index_name_,
    size_t columns_number,
    const GinFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , gin_filters(columns_number, GinFilter(params))
    , has_elems(false)
{
}

void MergeTreeIndexGranuleFullText::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty fulltext index {}.", backQuote(index_name));

    const auto & size_type = std::make_shared<DataTypeUInt32>();
    auto size_serialization = size_type->getDefaultSerialization();

    for (const auto & gin_filter : gin_filters)
    {
        size_t filter_size = gin_filter.getFilter().size();
        size_serialization->serializeBinary(filter_size, ostr, {});
        ostr.write(reinterpret_cast<const char *>(gin_filter.getFilter().data()), filter_size * sizeof(GinSegmentWithRowIdRangeVector::value_type));
    }
}

void MergeTreeIndexGranuleFullText::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_rows;
    const auto & size_type = std::make_shared<DataTypeUInt32>();

    auto size_serialization = size_type->getDefaultSerialization();
    for (auto & gin_filter : gin_filters)
    {
        size_serialization->deserializeBinary(field_rows, istr, {});
        size_t filter_size = field_rows.safeGet<size_t>();
        gin_filter.getFilter().resize(filter_size);

        if (filter_size == 0)
            continue;

        istr.readStrict(reinterpret_cast<char *>(gin_filter.getFilter().data()), filter_size * sizeof(GinSegmentWithRowIdRangeVector::value_type));
    }
    has_elems = true;
}


MergeTreeIndexAggregatorFullText::MergeTreeIndexAggregatorFullText(
    GinIndexStorePtr store_,
    const Names & index_columns_,
    const String & index_name_,
    const GinFilterParameters & params_,
    TokenExtractorPtr token_extractor_)
    : store(store_)
    , index_columns(index_columns_)
    , index_name (index_name_)
    , params(params_)
    , token_extractor(token_extractor_)
    , granule(
        std::make_shared<MergeTreeIndexGranuleFullText>(
            index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorFullText::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleFullText>(
        index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorFullText::addToGinFilter(UInt32 rowID, const char * data, size_t length, GinFilter & gin_filter)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && token_extractor->nextInStringPadded(data, length, &cur, &token_start, &token_len))
        gin_filter.add(data + token_start, token_len, rowID, store);
}

void MergeTreeIndexAggregatorFullText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);
    auto start_row_id = store->getNextRowIDRange(rows_read);

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column_with_type = block.getByName(index_columns[col]);
        const auto & column = column_with_type.column;
        size_t current_position = *pos;
        auto row_id = start_row_id;

        bool need_to_write = false;
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
                    addToGinFilter(row_id, ref.data, ref.size, granule->gin_filters[col]);
                    store->incrementCurrentSizeBy(ref.size);
                }
                current_position += 1;
                row_id++;

                if (store->needToWrite())
                    need_to_write = true;
            }
        }
        else
        {
            for (size_t i = 0; i < rows_read; ++i)
            {
                auto ref = column->getDataAt(current_position + i);
                addToGinFilter(row_id, ref.data, ref.size, granule->gin_filters[col]);
                store->incrementCurrentSizeBy(ref.size);
                row_id++;
                if (store->needToWrite())
                    need_to_write = true;
            }
        }
        granule->gin_filters[col].addRowRangeToGinFilter(store->getCurrentSegmentID(), start_row_id, static_cast<UInt32>(start_row_id + rows_read - 1));
        if (need_to_write)
        {
            store->writeSegment();
        }
    }

    granule->has_elems = true;
    *pos += rows_read;
}

MergeTreeConditionFullText::MergeTreeConditionFullText(
    const ActionsDAG * filter_actions_dag,
    ContextPtr context_,
    const Block & index_sample_block,
    const GinFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    :  WithContext(context_), header(index_sample_block)
    , params(params_)
    , token_extractor(token_extactor_)
{
    if (!filter_actions_dag)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    rpn = std::move(
            RPNBuilder<RPNElement>(
                    filter_actions_dag->getOutputs().at(0), context_,
                    [&](const RPNBuilderTreeNode & node, RPNElement & out)
                    {
                        return this->traverseAtomAST(node, out);
                    }).extractRPN());
}

/// Keep in-sync with MergeTreeConditionFullText::alwaysUnknownOrTrue
bool MergeTreeConditionFullText::alwaysUnknownOrTrue() const
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

bool MergeTreeConditionFullText::mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule,[[maybe_unused]] PostingsCacheForStore & cache_store) const
{
    std::shared_ptr<MergeTreeIndexGranuleFullText> granule
            = std::dynamic_pointer_cast<MergeTreeIndexGranuleFullText>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GinFilter index condition got a granule with the wrong type.");

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
            rpn_stack.emplace_back(granule->gin_filters[element.key_column].contains(*element.gin_filter, cache_store), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.set_gin_filters.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const size_t key_idx = element.set_key_position[column];

                const auto & gin_filters = element.set_gin_filters[column];
                for (size_t row = 0; row < gin_filters.size(); ++row)
                    result[row] = result[row] && granule->gin_filters[key_idx].contains(gin_filters[row], cache_store);
            }

            rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
        {
            std::vector<bool> result(element.set_gin_filters.back().size(), true);

            const auto & gin_filters = element.set_gin_filters[0];

            for (size_t row = 0; row < gin_filters.size(); ++row)
                result[row] = result[row] && granule->gin_filters[element.key_column].contains(gin_filters[row], cache_store);

            rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
        }
        else if (element.function == RPNElement::FUNCTION_MATCH)
        {
            if (!element.set_gin_filters.empty())
            {
                /// Alternative substrings
                std::vector<bool> result(element.set_gin_filters.back().size(), true);

                const auto & gin_filters = element.set_gin_filters[0];

                for (size_t row = 0; row < gin_filters.size(); ++row)
                    result[row] = result[row] && granule->gin_filters[element.key_column].contains(gin_filters[row], cache_store);

                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            }
            else if (element.gin_filter)
            {
                rpn_stack.emplace_back(granule->gin_filters[element.key_column].contains(*element.gin_filter, cache_store), true);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in GinFilterCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in GinFilterCondition::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

bool MergeTreeConditionFullText::traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out)
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
                out.function = const_value.safeGet<Float64>() != 0.00 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    if (node.isFunction())
    {
        const auto function = node.toFunctionNode();
        // auto arguments_size = function.getArgumentsSize();
        auto function_name = function.getFunctionName();

        size_t function_arguments_size = function.getArgumentsSize();
        if (function_arguments_size != 2)
            return false;
        auto lhs_argument = function.getArgumentAt(0);
        auto rhs_argument = function.getArgumentAt(1);

        if (functionIsInOrGlobalInOperator(function_name))
        {
            if (tryPrepareSetGinFilter(lhs_argument, rhs_argument, out))
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
                 function_name == "like" ||
                 function_name == "notLike" ||
                 function_name == "hasToken" ||
                 function_name == "hasTokenOrNull" ||
                 function_name == "startsWith" ||
                 function_name == "endsWith" ||
                 function_name == "multiSearchAny" ||
                 function_name == "match")
        {
            Field const_value;
            DataTypePtr const_type;
            if (rhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseASTEquals(function_name, lhs_argument, const_type, const_value, out))
                    return true;
            }
            else if (lhs_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
            {
                if (traverseASTEquals(function_name, rhs_argument, const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeConditionFullText::traverseASTEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_ast,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    Field const_value = value_field;
    size_t key_column_num = 0;
    bool key_exists = header.has(key_ast.getColumnName());
    bool map_key_exists = header.has(fmt::format("mapKeys({})", key_ast.getColumnName()));

    if (key_ast.isFunction())
    {
        const auto function = key_ast.toFunctionNode();
        if (function.getFunctionName() == "arrayElement")
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

            auto first_argument = function.getArgumentAt(0);
            const auto map_column_name = first_argument.getColumnName();
            auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", map_column_name);

            if (header.has(map_keys_index_column_name))
            {
                auto argument = function.getArgumentAt(1);
                DataTypePtr const_type;
                if (argument.tryGetConstant(const_value, const_type))
                {
                    auto const_data_type = WhichDataType(const_type);
                    if (!const_data_type.isStringOrFixedString() && !const_data_type.isArray())
                        return false;

                    key_column_num = header.getPositionByName(map_keys_index_column_name);
                    key_exists = true;
                }
                else
                {
                    return false;
                }
            }
            else if (header.has(map_values_index_column_name))
            {
                key_column_num = header.getPositionByName(map_values_index_column_name);
                key_exists = true;
            }
            else
            {
                return false;
            }
        }
    }

    if (!key_exists && !map_key_exists)
        return false;

    if (map_key_exists && (function_name == "has" || function_name == "mapContains"))
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    if (function_name == "has")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }

    if (function_name == "notEquals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    if (function_name == "equals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    if (function_name == "like")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    if (function_name == "notLike")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    if (function_name == "startsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToGinFilter(value.data(), value.size(), *out.gin_filter, true, false);
        return true;
    }
    if (function_name == "endsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToGinFilter(value.data(), value.size(), *out.gin_filter, false, true);
        return true;
    }
    if (function_name == "multiSearchAny")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_MULTI_SEARCH;

        /// 2d vector is not needed here but is used because already exists for FUNCTION_IN
        std::vector<GinFilters> gin_filters;
        gin_filters.emplace_back();
        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            gin_filters.back().emplace_back(params);
            const auto & value = element.safeGet<String>();
            token_extractor->substringToGinFilter(value.data(), value.size(), gin_filters.back().back(), false, false);
        }
        out.set_gin_filters = std::move(gin_filters);
        return true;
    }
    if (function_name == "match")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_MATCH;

        auto & value = const_value.safeGet<String>();
        String required_substring;
        bool dummy_is_trivial, dummy_required_substring_is_prefix;
        std::vector<String> alternatives;
        OptimizedRegularExpression::analyze(value, required_substring, dummy_is_trivial, dummy_required_substring_is_prefix, alternatives);

        if (required_substring.empty() && alternatives.empty())
            return false;

        /// out.set_gin_filters means alternatives exist
        /// out.gin_filter means required_substring exists
        if (!alternatives.empty())
        {
            std::vector<GinFilters> gin_filters;
            gin_filters.emplace_back();
            for (const auto & alternative : alternatives)
            {
                gin_filters.back().emplace_back(params);
                token_extractor->substringToGinFilter(alternative.data(), alternative.size(), gin_filters.back().back(), false, false);
            }
            out.set_gin_filters = std::move(gin_filters);
        }
        else
        {
            out.gin_filter = std::make_unique<GinFilter>(params);
            token_extractor->substringToGinFilter(required_substring.data(), required_substring.size(), *out.gin_filter, false, false);
        }

        return true;
    }

    return false;
}

bool MergeTreeConditionFullText::tryPrepareSetGinFilter(
    const RPNBuilderTreeNode & lhs,
    const RPNBuilderTreeNode & rhs,
    RPNElement & out)
{
    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    if (lhs.isFunction() && lhs.toFunctionNode().getFunctionName() == "tuple")
    {
        const auto function = lhs.toFunctionNode();
        auto arguments_size = function.getArgumentsSize();
        for (size_t i = 0; i < arguments_size; ++i)
        {
            if (header.has(function.getArgumentAt(i).getColumnName()))
            {
                auto key = header.getPositionByName(function.getArgumentAt(i).getColumnName());
                key_tuple_mapping.emplace_back(i, key);
                data_types.push_back(header.getByPosition(key).type);
            }
        }
    }
    else
    {
        if (header.has(lhs.getColumnName()))
        {
            auto key = header.getPositionByName(lhs.getColumnName());
            key_tuple_mapping.emplace_back(0, key);
            data_types.push_back(header.getByPosition(key).type);
        }
    }

    if (key_tuple_mapping.empty())
        return false;

    auto future_set = rhs.tryGetPreparedSet();
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(rhs.getTreeContext().getQueryContext());
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & data_type : prepared_set->getDataTypes())
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
            return false;

    std::vector<GinFilters> gin_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    for (const auto & elem : key_tuple_mapping)
    {
        gin_filters.emplace_back();
        gin_filters.back().reserve(prepared_set->getTotalRowCount());
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            gin_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            token_extractor->stringToGinFilter(ref.data, ref.size, gin_filters.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_gin_filters = std::move(gin_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexFullText::createIndexGranule() const
{
    /// ------
    /// Index type 'inverted' was renamed to 'full_text' in May 2024.
    /// Tables with old indexes can be loaded during a transition period. We still want let users know that they should drop existing
    /// indexes and re-create them. Function `createIndexGranule` is called whenever the index is used by queries. Reject the query if we
    /// have an old index.
    /// TODO: remove this at the end of 2024.
    if (index.type == INVERTED_INDEX_NAME)
        throw Exception(ErrorCodes::ILLEGAL_INDEX, "Indexes of type 'inverted' are no longer supported. Please drop and recreate the index as type 'full-text'");
    /// ------

    return std::make_shared<MergeTreeIndexGranuleFullText>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexFullText::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    /// should not be called: createIndexAggregatorForPart should be used
    assert(false);
    return nullptr;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexFullText::createIndexAggregatorForPart(const GinIndexStorePtr & store, const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorFullText>(store, index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexFullText::createIndexCondition(
        const ActionsDAG * filter_actions_dag, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionFullText>(filter_actions_dag, context, index.sample_block, params, token_extractor.get());
};

MergeTreeIndexPtr fullTextIndexCreator(
    const IndexDescription & index)
{
    size_t n = index.arguments.empty() ? 0 : index.arguments[0].safeGet<size_t>();
    UInt64 max_rows = index.arguments.size() < 2 ? DEFAULT_MAX_ROWS_PER_POSTINGS_LIST : index.arguments[1].safeGet<UInt64>();
    GinFilterParameters params(n, max_rows);

    /// Use SplitTokenExtractor when n is 0, otherwise use NgramTokenExtractor
    if (n > 0)
    {
        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);
        return std::make_shared<MergeTreeIndexFullText>(index, params, std::move(tokenizer));
    }

    auto tokenizer = std::make_unique<SplitTokenExtractor>();
    return std::make_shared<MergeTreeIndexFullText>(index, params, std::move(tokenizer));
}

void fullTextIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isArray())
        {
            const auto & gin_type = assert_cast<const DataTypeArray &>(*index_data_type);
            data_type = WhichDataType(gin_type.getNestedType());
        }
        else if (data_type.isLowCardinality())
        {
            const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index_data_type);
            data_type = WhichDataType(low_cardinality.getDictionaryType());
        }

        if (!data_type.isString() && !data_type.isFixedString())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Full text index can be used only with `String`, `FixedString`,"
                            "`LowCardinality(String)`, `LowCardinality(FixedString)` "
                            "column or Array with `String` or `FixedString` values column.");
    }

    if (index.arguments.size() > 2)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Full text index must have less than two arguments.");

    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The first full text index argument must be positive integer.");

    if (index.arguments.size() == 2)
    {
        if (index.arguments[1].getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "The second full text index argument must be UInt64");
        if (index.arguments[1].safeGet<UInt64>() != UNLIMITED_ROWS_PER_POSTINGS_LIST && index.arguments[1].safeGet<UInt64>() < MIN_ROWS_PER_POSTINGS_LIST)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "The maximum rows per postings list must be no less than {}", MIN_ROWS_PER_POSTINGS_LIST);
    }
    /// Just validate
    size_t ngrams = index.arguments.empty() ? 0 : index.arguments[0].safeGet<size_t>();
    UInt64 max_rows_per_postings_list = index.arguments.size() < 2 ? DEFAULT_MAX_ROWS_PER_POSTINGS_LIST : index.arguments[1].safeGet<UInt64>();
    GinFilterParameters params(ngrams, max_rows_per_postings_list);
}
}
