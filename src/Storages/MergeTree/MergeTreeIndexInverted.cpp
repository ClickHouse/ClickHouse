#include <Storages/MergeTree/MergeTreeIndexInverted.h>

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
#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

MergeTreeIndexGranuleInverted::MergeTreeIndexGranuleInverted(
    const String & index_name_,
    size_t columns_number,
    const GinFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , gin_filters(columns_number, GinFilter(params))
    , has_elems(false)
{
}

void MergeTreeIndexGranuleInverted::serializeBinary(WriteBuffer & ostr) const
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

void MergeTreeIndexGranuleInverted::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_rows;
    const auto & size_type = std::make_shared<DataTypeUInt32>();

    auto size_serialization = size_type->getDefaultSerialization();
    for (auto & gin_filter : gin_filters)
    {
        size_serialization->deserializeBinary(field_rows, istr, {});
        size_t filter_size = field_rows.get<size_t>();

        if (filter_size == 0)
            continue;

        gin_filter.getFilter().assign(filter_size, {});
        istr.readStrict(reinterpret_cast<char *>(gin_filter.getFilter().data()), filter_size * sizeof(GinSegmentWithRowIdRangeVector::value_type));
    }
    has_elems = true;
}


MergeTreeIndexAggregatorInverted::MergeTreeIndexAggregatorInverted(
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
        std::make_shared<MergeTreeIndexGranuleInverted>(
            index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorInverted::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleInverted>(
        index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorInverted::addToGinFilter(UInt32 rowID, const char * data, size_t length, GinFilter & gin_filter, UInt64 limit)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && token_extractor->nextInStringPadded(data, length, &cur, &token_start, &token_len))
        gin_filter.add(data + token_start, token_len, rowID, store, limit);
}

void MergeTreeIndexAggregatorInverted::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);
    auto row_id = store->getNextRowIDRange(rows_read);
    auto start_row_id = row_id;

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column_with_type = block.getByName(index_columns[col]);
        const auto & column = column_with_type.column;
        size_t current_position = *pos;

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
                    addToGinFilter(row_id, ref.data, ref.size, granule->gin_filters[col], rows_read);
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
                addToGinFilter(row_id, ref.data, ref.size, granule->gin_filters[col], rows_read);
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

MergeTreeConditionInverted::MergeTreeConditionInverted(
    const SelectQueryInfo & query_info,
    ContextPtr context_,
    const Block & index_sample_block,
    const GinFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    :  WithContext(context_), header(index_sample_block)
    , params(params_)
    , token_extractor(token_extactor_)
    , prepared_sets(query_info.prepared_sets)
{
    if (context_->getSettingsRef().allow_experimental_analyzer)
    {
        if (!query_info.filter_actions_dag)
        {
            rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
            return;
        }

        rpn = std::move(
                RPNBuilder<RPNElement>(
                        query_info.filter_actions_dag->getOutputs().at(0), context_,
                        [&](const RPNBuilderTreeNode & node, RPNElement & out)
                        {
                            return this->traverseAtomAST(node, out);
                        }).extractRPN());
        return;
    }

    ASTPtr filter_node = buildFilterNode(query_info.query);
    if (!filter_node)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    auto block_with_constants = KeyCondition::getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context_);
    RPNBuilder<RPNElement> builder(
        filter_node,
        context_,
        std::move(block_with_constants),
        query_info.prepared_sets,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return traverseAtomAST(node, out); });
    rpn = std::move(builder).extractRPN();
}

/// Keep in-sync with MergeTreeConditionFullText::alwaysUnknownOrTrue
bool MergeTreeConditionInverted::alwaysUnknownOrTrue() const
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

bool MergeTreeConditionInverted::mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule,[[maybe_unused]] PostingsCacheForStore & cache_store) const
{
    std::shared_ptr<MergeTreeIndexGranuleInverted> granule
            = std::dynamic_pointer_cast<MergeTreeIndexGranuleInverted>(idx_granule);
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

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
        {
            std::vector<bool> result(element.set_gin_filters.back().size(), true);

            const auto & gin_filters = element.set_gin_filters[0];

            for (size_t row = 0; row < gin_filters.size(); ++row)
                result[row] = result[row] && granule->gin_filters[element.key_column].contains(gin_filters[row], cache_store);

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
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

bool MergeTreeConditionInverted::traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            /// Check constant like in KeyCondition
            if (const_value.getType() == Field::Types::UInt64
                || const_value.getType() == Field::Types::Int64
                || const_value.getType() == Field::Types::Float64)
            {
                /// Zero in all types is represented in memory the same way as in UInt64.
                out.function = const_value.get<UInt64>()
                            ? RPNElement::ALWAYS_TRUE
                            : RPNElement::ALWAYS_FALSE;

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
                else if (function_name == "in")
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
                 function_name == "multiSearchAny")
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

bool MergeTreeConditionInverted::traverseASTEquals(
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
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exists in map
              * we return default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exists.
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
        auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "has")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_HAS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }

    if (function_name == "notEquals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "equals")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "like")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "notLike")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "startsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "endsWith")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_filter = std::make_unique<GinFilter>(params);
        const auto & value = const_value.get<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_filter);
        return true;
    }
    else if (function_name == "multiSearchAny")
    {
        out.key_column = key_column_num;
        out.function = RPNElement::FUNCTION_MULTI_SEARCH;

        /// 2d vector is not needed here but is used because already exists for FUNCTION_IN
        std::vector<GinFilters> gin_filters;
        gin_filters.emplace_back();
        for (const auto & element : const_value.get<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            gin_filters.back().emplace_back(params);
            const auto & value = element.get<String>();
            token_extractor->stringToGinFilter(value.data(), value.size(), gin_filters.back().back());
        }
        out.set_gin_filters = std::move(gin_filters);
        return true;
    }

    return false;
}

bool MergeTreeConditionInverted::tryPrepareSetGinFilter(
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

MergeTreeIndexGranulePtr MergeTreeIndexInverted::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleInverted>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexInverted::createIndexAggregator() const
{
    /// should not be called: createIndexAggregatorForPart should be used
    assert(false);
    return nullptr;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexInverted::createIndexAggregatorForPart(const GinIndexStorePtr & store) const
{
    return std::make_shared<MergeTreeIndexAggregatorInverted>(store, index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexInverted::createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionInverted>(query, context, index.sample_block, params, token_extractor.get());
};

bool MergeTreeIndexInverted::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    return std::find(std::cbegin(index.column_names), std::cend(index.column_names), node->getColumnName()) != std::cend(index.column_names);
}

MergeTreeIndexPtr invertedIndexCreator(
    const IndexDescription & index)
{
    size_t n = index.arguments.empty() ? 0 : index.arguments[0].get<size_t>();
    Float64 density = index.arguments.size() < 2 ? 1.0 : index.arguments[1].get<Float64>();
    GinFilterParameters params(n, density);

    /// Use SplitTokenExtractor when n is 0, otherwise use NgramTokenExtractor
    if (n > 0)
    {
        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);
        return std::make_shared<MergeTreeIndexInverted>(index, params, std::move(tokenizer));
    }
    else
    {
        auto tokenizer = std::make_unique<SplitTokenExtractor>();
        return std::make_shared<MergeTreeIndexInverted>(index, params, std::move(tokenizer));
    }
}

void invertedIndexValidator(const IndexDescription & index, bool /*attach*/)
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
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Inverted index can be used only with `String`, `FixedString`,"
                            "`LowCardinality(String)`, `LowCardinality(FixedString)` "
                            "column or Array with `String` or `FixedString` values column.");
    }

    if (index.arguments.size() > 2)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Inverted index must have less than two arguments.");

    if (!index.arguments.empty() && index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The first Inverted index argument must be positive integer.");

    if (index.arguments.size() == 2 && (index.arguments[1].getType() != Field::Types::Float64 || index.arguments[1].get<Float64>() <= 0 || index.arguments[1].get<Float64>() > 1))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The second Inverted index argument must be a float between 0 and 1.");

    /// Just validate
    size_t ngrams = index.arguments.empty() ? 0 : index.arguments[0].get<size_t>();
    Float64 density = index.arguments.size() < 2 ? 1.0 : index.arguments[1].get<Float64>();
    GinFilterParameters params(ngrams, density);
}

}
