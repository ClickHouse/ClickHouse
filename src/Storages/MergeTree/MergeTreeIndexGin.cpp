
#include <algorithm>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Core/Defines.h>

#include <Poco/Logger.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

MergeTreeIndexGranuleGinFilter::MergeTreeIndexGranuleGinFilter(
    const String & index_name_,
    size_t columns_number,
    const GinFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , gin_filters(
        columns_number, GinFilter(params))
    , has_elems(false)
{
}

void MergeTreeIndexGranuleGinFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty fulltext index {}.", backQuote(index_name));

    const auto & size_type = std::make_shared<DataTypeUInt32>();
    auto size_serialization = size_type->getDefaultSerialization();

    for (const auto & gin_filter : gin_filters)
    {
        size_t filter_size = gin_filter.getFilter().size();
        size_serialization->serializeBinary(filter_size, ostr);
        ostr.write(reinterpret_cast<const char*>(gin_filter.getFilter().data()), filter_size * sizeof(GinFilter::RowIDRangeContainer::value_type));
    }
}

void MergeTreeIndexGranuleGinFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_rows;
    const auto & size_type = std::make_shared<DataTypeUInt32>();

    auto size_serialization = size_type->getDefaultSerialization();
    for (auto & gin_filter : gin_filters)
    {
        size_serialization->deserializeBinary(field_rows, istr);
        size_t filter_size = field_rows.get<size_t>();

        if (filter_size == 0)
            continue;

        gin_filter.getFilter().assign(filter_size, {});
        istr.read(reinterpret_cast<char*>(gin_filter.getFilter().data()), filter_size * sizeof(GinFilter::RowIDRangeContainer::value_type));
    }
    has_elems = true;
}


MergeTreeIndexAggregatorGinFilter::MergeTreeIndexAggregatorGinFilter(
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
        std::make_shared<MergeTreeIndexGranuleGinFilter>(
            index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorGinFilter::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleGinFilter>(
        index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorGinFilter::addToGinFilter(UInt32 rowID, const char* data, size_t length, GinFilter& gin_filter)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && token_extractor->nextInStringPadded(data, length, &cur, &token_start, &token_len))
    {
        gin_filter.add(data + token_start, token_len, rowID, store);
    }
}

void MergeTreeIndexAggregatorGinFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

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
                    addToGinFilter(row_id, ref.data, ref.size, granule->gin_filters[col]);
                    store->addSize(ref.size);
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
                store->addSize(ref.size);
                row_id++;
                if (store->needToWrite())
                    need_to_write = true;
            }
        }
        granule->gin_filters[col].addRowRangeToGinFilter(store->getCurrentSegmentID(), start_row_id, start_row_id + rows_read - 1);
        if (need_to_write)
        {
            store->writeSegment();
        }
    }

    granule->has_elems = true;
    *pos += rows_read;
}

MergeTreeConditionGinFilter::MergeTreeConditionGinFilter(
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const Block & index_sample_block,
    const GinFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    : index_columns(index_sample_block.getNames())
    , index_data_types(index_sample_block.getNamesAndTypesList().getTypes())
    , params(params_)
    , token_extractor(token_extactor_)
    , prepared_sets(query_info.prepared_sets)
{
    rpn = std::move(
            RPNBuilder<RPNElement>(
                    query_info, context,
                    [this] (const ASTPtr & node, ContextPtr /* context */, Block & block_with_constants, RPNElement & out) -> bool
                    {
                        return this->traverseAtomAST(node, block_with_constants, out);
                    }).extractRPN());
}

bool MergeTreeConditionGinFilter::alwaysUnknownOrTrue() const
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
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    return rpn_stack[0];
}

bool MergeTreeConditionGinFilter::mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule,[[maybe_unused]] PostingsCacheForStore &cache_store) const
{
    std::shared_ptr<MergeTreeIndexGranuleGinFilter> granule
            = std::dynamic_pointer_cast<MergeTreeIndexGranuleGinFilter>(idx_granule);
    if (!granule)
        throw Exception(
                "GinFilter index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

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
            throw Exception("Unexpected function type in GinFilterCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in GinFilterCondition::mayBeTrueOnGranule", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}

bool MergeTreeConditionGinFilter::getKey(const std::string & key_column_name, size_t & key_column_num)
{
    auto it = std::find(index_columns.begin(), index_columns.end(), key_column_name);
    if (it == index_columns.end())
        return false;

    key_column_num = static_cast<size_t>(it - index_columns.begin());
    return true;
}

bool MergeTreeConditionGinFilter::traverseAtomAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
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

    if (const auto * function = node->as<ASTFunction>())
    {
        if (!function->arguments)
            return false;

        const ASTs & arguments = function->arguments->children;

        if (arguments.size() != 2)
            return false;

        if (functionIsInOrGlobalInOperator(function->name))
        {
            if (tryPrepareSetGinFilter(arguments, out))
            {
                if (function->name == "notIn")
                {
                    out.function = RPNElement::FUNCTION_NOT_IN;
                    return true;
                }
                else if (function->name == "in")
                {
                    out.function = RPNElement::FUNCTION_IN;
                    return true;
                }
            }
        }
        else if (function->name == "equals" ||
                 function->name == "notEquals" ||
                 function->name == "has" ||
                 function->name == "mapContains" ||
                 function->name == "like" ||
                 function->name == "notLike" ||
                 function->name == "hasToken" ||
                 function->name == "startsWith" ||
                 function->name == "endsWith" ||
                 function->name == "multiSearchAny")
        {
            Field const_value;
            DataTypePtr const_type;
            if (KeyCondition::getConstant(arguments[1], block_with_constants, const_value, const_type))
            {
                if (traverseASTEquals(function->name, arguments[0], const_type, const_value, out))
                    return true;
            }
            else if (KeyCondition::getConstant(arguments[0], block_with_constants, const_value, const_type) && (function->name == "equals" || function->name == "notEquals"))
            {
                if (traverseASTEquals(function->name, arguments[1], const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeConditionGinFilter::traverseASTEquals(
    const String & function_name,
    const ASTPtr & key_ast,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    Field const_value = value_field;

    size_t key_column_num = 0;
    bool key_exists = getKey(key_ast->getColumnName(), key_column_num);
    bool map_key_exists = getKey(fmt::format("mapKeys({})", key_ast->getColumnName()), key_column_num);

    if (const auto * function = key_ast->as<ASTFunction>())
    {
        if (function->name == "arrayElement")
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

            const auto * column_ast_identifier = function->arguments.get()->children[0].get()->as<ASTIdentifier>();
            if (!column_ast_identifier)
                return false;

            const auto & map_column_name = column_ast_identifier->name();

            size_t map_keys_key_column_num = 0;
            auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
            bool map_keys_exists = getKey(map_keys_index_column_name, map_keys_key_column_num);

            size_t map_values_key_column_num = 0;
            auto map_values_index_column_name = fmt::format("mapValues({})", map_column_name);
            bool map_values_exists = getKey(map_values_index_column_name, map_values_key_column_num);

            if (map_keys_exists)
            {
                auto & argument = function->arguments.get()->children[1];

                if (const auto * literal = argument->as<ASTLiteral>())
                {
                    auto element_key = literal->value;
                    const_value = element_key;
                    key_column_num = map_keys_key_column_num;
                    key_exists = true;
                }
                else
                {
                    return false;
                }
            }
            else if (map_values_exists)
            {
                key_column_num = map_values_key_column_num;
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
    else if (function_name == "hasToken")
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
        std::vector<std::vector<GinFilter>> gin_filters;
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

bool MergeTreeConditionGinFilter::tryPrepareSetGinFilter(
    const ASTs & args,
    RPNElement & out)
{
    const ASTPtr & left_arg = args[0];
    const ASTPtr & right_arg = args[1];

    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    const auto * left_arg_tuple = typeid_cast<const ASTFunction *>(left_arg.get());
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        const auto & tuple_elements = left_arg_tuple->arguments->children;
        for (size_t i = 0; i < tuple_elements.size(); ++i)
        {
            size_t key = 0;
            if (getKey(tuple_elements[i]->getColumnName(), key))
            {
                key_tuple_mapping.emplace_back(i, key);
                data_types.push_back(index_data_types[key]);
            }
        }
    }
    else
    {
        size_t key = 0;
        if (getKey(left_arg->getColumnName(), key))
        {
            key_tuple_mapping.emplace_back(0, key);
            data_types.push_back(index_data_types[key]);
        }
    }

    if (key_tuple_mapping.empty())
        return false;

    PreparedSetKey set_key;
    if (typeid_cast<const ASTSubquery *>(right_arg.get()) || typeid_cast<const ASTIdentifier *>(right_arg.get()))
        set_key = PreparedSetKey::forSubquery(*right_arg);
    else
        set_key = PreparedSetKey::forLiteral(*right_arg, data_types);

    const SetPtr & prepared_set = prepared_sets->get(set_key);
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & data_type : prepared_set->getDataTypes())
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
            return false;

    std::vector<std::vector<GinFilter>> gin_filters;
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
            gin_filters.back().back().setQueryString(ref.data, ref.size);
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_gin_filters = std::move(gin_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexGinFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleGinFilter>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexGinFilter::createIndexAggregator() const
{
    /// should not be called: createIndexAggregatorForPart should be used
    assert(false);
    return nullptr;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexGinFilter::createIndexAggregatorForPart(const GinIndexStorePtr &store) const
{
    return std::make_shared<MergeTreeIndexAggregatorGinFilter>(store, index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexGinFilter::createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionGinFilter>(query, context, index.sample_block, params, token_extractor.get());
};

bool MergeTreeIndexGinFilter::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    return std::find(std::cbegin(index.column_names), std::cend(index.column_names), node->getColumnName()) != std::cend(index.column_names);
}

MergeTreeIndexPtr ginIndexCreator(
    const IndexDescription & index)
{
    if (index.type == GinFilter::getName())
    {
        size_t n = index.arguments.empty() ? 0 : index.arguments[0].get<size_t>();
        GinFilterParameters params(n);

        /// Use SplitTokenExtractor when n is 0, otherwise use NgramTokenExtractor
        if (n > 0)
        {
            auto tokenizer = std::make_unique<NgramTokenExtractor>(n);
            return std::make_shared<MergeTreeIndexGinFilter>(index, params, std::move(tokenizer));
        }
        else
        {
            auto tokenizer = std::make_unique<SplitTokenExtractor>();
            return std::make_shared<MergeTreeIndexGinFilter>(index, params, std::move(tokenizer));
        }
    }
    else
    {
        throw Exception("Unknown index type: " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);
    }
}

void ginIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & index_data_type : index.data_types)
    {
        WhichDataType data_type(index_data_type);

        if (data_type.isArray())
        {
            const auto & gin_type = assert_cast<const DataTypeArray &>(*index_data_type);
            data_type = WhichDataType(gin_type.getNestedType());
        }
        else if (data_type.isLowCarnality())
        {
            const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index_data_type);
            data_type = WhichDataType(low_cardinality.getDictionaryType());
        }

        if (!data_type.isString() && !data_type.isFixedString())
            throw Exception("Gin filter index can be used only with `String`, `FixedString`, `LowCardinality(String)`, `LowCardinality(FixedString)` column or Array with `String` or `FixedString` values column.", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.type != GinFilter::getName())
        throw Exception("Unknown index type: " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);

    if (index.arguments.size() > 1)
        throw Exception("Gin index must have zero or one argument.", ErrorCodes::INCORRECT_QUERY);

    if (index.arguments.size() == 1 && index.arguments[0].getType() != Field::Types::UInt64)
        throw Exception("Gin index argument must be positive integer.", ErrorCodes::INCORRECT_QUERY);

    size_t ngrams = index.arguments.empty() ? 0 : index.arguments[0].get<size_t>();

    /// Just validate
    GinFilterParameters params(ngrams);
}

}
