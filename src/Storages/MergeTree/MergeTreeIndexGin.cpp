#include <Storages/MergeTree/MergeTreeIndexGin.h>

#include <Columns/ColumnLowCardinality.h>
#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/searchAnyAll.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <base/types.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

static const String ARGUMENT_TOKENIZER = "tokenizer";
static const String ARGUMENT_NGRAM_SIZE = "ngram_size";
static const String ARGUMENT_SEPARATORS = "separators";
static const String ARGUMENT_SEGMENT_DIGESTION_THRESHOLD_BYTES = "segment_digestion_threshold_bytes";
static const String ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE = "bloom_filter_false_positive_rate";

MergeTreeIndexGranuleGin::MergeTreeIndexGranuleGin(const String & index_name_)
    : index_name(index_name_)
    , gin_filter()
    , initialized(false)
{
}

void MergeTreeIndexGranuleGin::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty text index {}.", backQuote(index_name));

    const auto & size_type = std::make_shared<DataTypeUInt32>();
    auto size_serialization = size_type->getDefaultSerialization();

    size_t filter_size = gin_filter.getSegmentsWithRowIdRange().size();
    size_serialization->serializeBinary(filter_size, ostr, {});
    ostr.write(reinterpret_cast<const char *>(gin_filter.getSegmentsWithRowIdRange().data()), filter_size * sizeof(GinSegmentsWithRowIdRange::value_type));
}

void MergeTreeIndexGranuleGin::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_rows;
    const auto & size_type = std::make_shared<DataTypeUInt32>();

    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->deserializeBinary(field_rows, istr, {});

    size_t filter_size = field_rows.safeGet<size_t>();
    gin_filter.getSegmentsWithRowIdRange().resize(filter_size);

    if (filter_size != 0)
        istr.readStrict(reinterpret_cast<char *>(gin_filter.getSegmentsWithRowIdRange().data()), filter_size * sizeof(GinSegmentsWithRowIdRange::value_type));

    initialized = true;
}

size_t MergeTreeIndexGranuleGin::memoryUsageBytes() const
{
    return gin_filter.memoryUsageBytes();
}

MergeTreeIndexAggregatorGin::MergeTreeIndexAggregatorGin(
    GinIndexStorePtr store_,
    const Names & index_columns_,
    const String & index_name_,
    TokenExtractorPtr token_extractor_)
    : store(store_)
    , index_columns(index_columns_)
    , index_name (index_name_)
    , token_extractor(token_extractor_)
    , granule(std::make_shared<MergeTreeIndexGranuleGin>(index_name))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorGin::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleGin>(index_name);
    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorGin::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read == 0)
        return;

    const auto & index_column_name = index_columns[0];
    const auto & index_column = block.getByName(index_column_name);

    UInt32 start_row_id = store->getNextRowIdRange(rows_read);

    size_t current_position = *pos;
    for (size_t i = 0; i < rows_read; ++i)
    {
        std::string_view value = index_column.column->getDataAt(current_position + i).toView();
        for (const auto & token : token_extractor->getTokensView(value.data(), value.length()))
            granule->gin_filter.add(String(token), start_row_id + i, store);
        store->incrementCurrentSizeBy(value.size());
    }
    granule->gin_filter.addRowIdRangeToGinFilter(store->getCurrentSegmentId(), start_row_id, static_cast<UInt32>(start_row_id + rows_read - 1));

    if (store->needToWriteCurrentSegment())
        store->writeSegment();

    granule->initialized = true;
    *pos += rows_read;
}

MergeTreeIndexConditionGin::MergeTreeIndexConditionGin(
    const ActionsDAG::Node * predicate,
    ContextPtr context_,
    const Block & index_sample_block,
    const GinFilter::Parameters & gin_filter_params_,
    TokenExtractorPtr token_extactor_)
    : WithContext(context_)
    , header(index_sample_block)
    , gin_filter_params(gin_filter_params_)
    , token_extractor(token_extactor_)
{
    if (!predicate)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    rpn = std::move(
            RPNBuilder<RPNElement>(
                    predicate, context_,
                    [&](const RPNBuilderTreeNode & node, RPNElement & out)
                    {
                        return this->traverseAtomAST(node, out);
                    }).extractRPN());
}

/// Keep in-sync with MergeTreeIndexConditionGin::alwaysUnknownOrTrue
bool MergeTreeIndexConditionGin::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn,
        {RPNElement::FUNCTION_EQUALS,
         RPNElement::FUNCTION_NOT_EQUALS,
         RPNElement::FUNCTION_SEARCH_ANY,
         RPNElement::FUNCTION_SEARCH_ALL,
         RPNElement::FUNCTION_IN,
         RPNElement::FUNCTION_NOT_IN,
         RPNElement::FUNCTION_MATCH});
}

bool MergeTreeIndexConditionGin::mayBeTrueOnGranule([[maybe_unused]] MergeTreeIndexGranulePtr idx_granule) const
{
    /// should call mayBeTrueOnGranuleInPart instead
    assert(false);
    return false;
}

bool MergeTreeIndexConditionGin::mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule, GinPostingsListsCacheForStore & postings_lists_cache_for_store) const
{
    std::shared_ptr<MergeTreeIndexGranuleGin> granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleGin>(idx_granule);
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
        else if (element.function == RPNElement::FUNCTION_SEARCH_ANY)
        {
            chassert(element.query_strings_for_set.size() == 1);
            const std::vector<GinQueryString> & query_strings = element.query_strings_for_set.front();
            bool exists_in_gin_filter = query_strings.empty() ? true : false;
            for (const GinQueryString & query_string : query_strings)
            {
                if (granule->gin_filter.contains(query_string, postings_lists_cache_for_store, GinSearchMode::Any))
                {
                    exists_in_gin_filter = true;
                    break;
                }
            }
            rpn_stack.emplace_back(exists_in_gin_filter, true);
        }
        else if (element.function == RPNElement::FUNCTION_SEARCH_ALL)
        {
            chassert(element.query_strings_for_set.size() == 1);
            const std::vector<GinQueryString> & query_strings = element.query_strings_for_set.front();
            bool exists_in_gin_filter = true;
            for (const GinQueryString & query_string : query_strings)
            {
                if (!granule->gin_filter.contains(query_string, postings_lists_cache_for_store, GinSearchMode::All))
                {
                    exists_in_gin_filter = false;
                    break;
                }
            }
            rpn_stack.emplace_back(exists_in_gin_filter, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            rpn_stack.emplace_back(granule->gin_filter.contains(*element.query_string, postings_lists_cache_for_store), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.query_strings_for_set.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const auto & query_strings = element.query_strings_for_set[column];
                for (size_t row = 0; row < query_strings.size(); ++row)
                    result[row] = result[row] && granule->gin_filter.contains(query_strings[row], postings_lists_cache_for_store);
            }

            rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MATCH)
        {
            if (!element.query_strings_for_set.empty())
            {
                /// Alternative substrings
                std::vector<bool> result(element.query_strings_for_set.back().size(), true);

                const auto & query_strings = element.query_strings_for_set[0];

                for (size_t row = 0; row < query_strings.size(); ++row)
                    result[row] = granule->gin_filter.contains(query_strings[row], postings_lists_cache_for_store);

                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            }
            else if (element.query_string)
            {
                rpn_stack.emplace_back(granule->gin_filter.contains(*element.query_string, postings_lists_cache_for_store), true);
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

bool MergeTreeIndexConditionGin::traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out)
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
                 function_name == "like" ||
                 function_name == "notLike" ||
                 function_name == "hasToken" ||
                 function_name == "hasTokenOrNull" ||
                 function_name == "startsWith" ||
                 function_name == "endsWith" ||
                 function_name == "searchAny" ||
                 function_name == "searchAll" ||
                 function_name == "match")
        {
            Field const_value;
            DataTypePtr const_type;
            if (rhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseASTEquals(function, lhs_argument, const_type, const_value, out))
                    return true;
            }
            else if (lhs_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
            {
                if (traverseASTEquals(function, rhs_argument, const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

bool MergeTreeIndexConditionGin::traverseASTEquals(
    const RPNBuilderFunctionTreeNode & function_node,
    const RPNBuilderTreeNode & index_column_ast,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out)
{
    bool index_column_exists = header.has(index_column_ast.getColumnName());
    if (!index_column_exists)
        return false;

    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    const Field & const_value = value_field;
    const String & function_name = function_node.getFunctionName();

    if (function_name == "notEquals")
    {
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.query_string);
        return true;
    }
    if (function_name == "equals")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.query_string);
        return true;
    }
    if (function_name == "searchAny" || function_name == "searchAll")
    {
        std::vector<GinQueryString> query_strings;
        std::vector<String> search_tokens;
        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;
            const auto & value = element.safeGet<String>();
            query_strings.emplace_back(GinQueryString(value, {value}));
            search_tokens.push_back(value);
        }
        out.function = function_name == "searchAny" ? RPNElement::FUNCTION_SEARCH_ANY : RPNElement::FUNCTION_SEARCH_ALL;
        out.query_strings_for_set = std::vector<std::vector<GinQueryString>>{std::move(query_strings)};

        {
            /// TODO(ahmadov): move this block to another place, e.g. optimizations or query tree re-write.
            const auto * function_dag_node = function_node.getDAGNode();
            chassert(function_dag_node != nullptr && function_dag_node->function_base != nullptr);
            const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(function_dag_node->function_base.get());
            chassert(adaptor != nullptr);
            if (function_name == "searchAny")
            {
                auto * search_function = typeid_cast<FunctionSearchImpl<traits::SearchAnyTraits> *>(adaptor->getFunction().get());
                chassert(search_function != nullptr);
                search_function->trySetGinFilterParameters(gin_filter_params);
                search_function->trySetSearchTokens(search_tokens);
            }
            else
            {
                auto * search_function = typeid_cast<FunctionSearchImpl<traits::SearchAllTraits> *>(adaptor->getFunction().get());
                chassert(search_function != nullptr);
                search_function->trySetGinFilterParameters(gin_filter_params);
                search_function->trySetSearchTokens(search_tokens);
            }
        }
        return true;
    }
    if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.query_string);
        return true;
    }
    if (function_name == "startsWith")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToGinFilter(value.data(), value.size(), *out.query_string, true, false);
        return true;
    }
    if (function_name == "endsWith")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToGinFilter(value.data(), value.size(), *out.query_string, false, true);
        return true;
    }
    /// Currently, not all token extractors support LIKE-style matching.
    if (function_name == "like" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.query_string);
        return true;
    }
    if (function_name == "notLike" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.query_string);
        return true;
    }
    if (function_name == "match" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_MATCH;

        const auto & value = const_value.safeGet<String>();
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(value);
        if (result.required_substring.empty() && result.alternatives.empty())
            return false;

        /// out.query_strings_for_set means alternatives exist
        /// out.gin_filter means required_substring exists
        if (!result.alternatives.empty())
        {
            std::vector<std::vector<GinQueryString>> query_strings;
            query_strings.emplace_back();
            for (const auto & alternative : result.alternatives)
            {
                query_strings.back().emplace_back();
                token_extractor->substringToGinFilter(alternative.data(), alternative.size(), query_strings.back().back(), false, false);
            }
            out.query_strings_for_set = std::move(query_strings);
        }
        else
        {
            out.query_string = std::make_unique<GinQueryString>();
            token_extractor->substringToGinFilter(result.required_substring.data(), result.required_substring.size(), *out.query_string, false, false);
        }

        return true;
    }

    return false;
}

bool MergeTreeIndexConditionGin::tryPrepareSetGinFilter(
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

    std::vector<std::vector<GinQueryString>> gin_query_infos;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    for (const auto & elem : key_tuple_mapping)
    {
        gin_query_infos.emplace_back();
        gin_query_infos.back().reserve(prepared_set->getTotalRowCount());
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            gin_query_infos.back().emplace_back();
            std::string_view value = column->getDataAt(row).toView();
            token_extractor->stringToGinFilter(value.data(), value.size(), gin_query_infos.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.query_strings_for_set = std::move(gin_query_infos);

    return true;
}

MergeTreeIndexGin::MergeTreeIndexGin(
    const IndexDescription & index_,
    const GinFilter::Parameters & gin_filter_params_,
    std::unique_ptr<ITokenExtractor> && token_extractor_)
    : IMergeTreeIndex(index_)
    , gin_filter_params(gin_filter_params_)
    , token_extractor(std::move(token_extractor_))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexGin::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleGin>(index.name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexGin::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    /// should not be called: createIndexAggregatorForPart should be used
    chassert(false);
    return nullptr;
}

MergeTreeIndexAggregatorPtr MergeTreeIndexGin::createIndexAggregatorForPart(const GinIndexStorePtr & store, const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorGin>(store, index.column_names, index.name, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexGin::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionGin>(predicate, context, index.sample_block, gin_filter_params, token_extractor.get());
}

namespace
{

std::unordered_map<String, Field> convertArgumentsToOptionsMap(const FieldVector & arguments)
{
    std::unordered_map<String, Field> options;
    for (const Field & argument : arguments)
    {
        if (argument.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Arguments of text index must be key-value pair (identifier = literal)");
        Tuple tuple = argument.template safeGet<Tuple>();
        String key = tuple[0].safeGet<String>();
        if (options.contains(key))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Text index '{}' argument is specified more than once", key);
        options[key] = tuple[1];
    }
    return options;
}

template <typename Type>
std::optional<Type> getOption(const std::unordered_map<String, Field> & options, const String & option)
{
    if (auto it = options.find(option); it != options.end())
    {
        const Field & value = it->second;
        Field::Types::Which expected_type = Field::TypeToEnum<NearestFieldType<Type>>::value;
        if (value.getType() != expected_type)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Text index argument '{}' expected to be {}, but got {}",
                option,
                fieldTypeToString(expected_type),
                value.getTypeName());
        return value.safeGet<Type>();
    }
    return {};
}

template <typename... Args>
std::optional<std::vector<String>> getOptionAsStringArray(Args &&... args)
{
    auto array = getOption<Array>(std::forward<Args>(args)...);
    if (array.has_value())
    {
        std::vector<String> values;
        for (const auto & entry : array.value())
            values.emplace_back(entry.template safeGet<String>());
        return values;
    }
    return {};
}
}

MergeTreeIndexPtr ginIndexCreator(const IndexDescription & index)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);

    String tokenizer = getOption<String>(options, ARGUMENT_TOKENIZER).value();

    std::unique_ptr<ITokenExtractor> token_extractor;
    std::optional<UInt64> ngram_size;
    std::optional<std::vector<String>> separators;
    if (tokenizer == DefaultTokenExtractor::getExternalName())
        token_extractor = std::make_unique<DefaultTokenExtractor>();
    else if (tokenizer == NgramTokenExtractor::getExternalName())
    {
        ngram_size = getOption<UInt64>(options, ARGUMENT_NGRAM_SIZE);
        token_extractor = std::make_unique<NgramTokenExtractor>(ngram_size.value_or(DEFAULT_NGRAM_SIZE));
    }
    else if (tokenizer == SplitTokenExtractor::getExternalName())
    {
        separators = getOptionAsStringArray(options, ARGUMENT_SEPARATORS).value_or(std::vector<String>{" "});
        token_extractor = std::make_unique<SplitTokenExtractor>(separators.value());
    }
    else if (tokenizer == NoOpTokenExtractor::getExternalName())
        token_extractor = std::make_unique<NoOpTokenExtractor>();
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tokenizer {} not supported", tokenizer);

    UInt64 segment_digestion_threshold_bytes
        = getOption<UInt64>(options, ARGUMENT_SEGMENT_DIGESTION_THRESHOLD_BYTES).value_or(UNLIMITED_SEGMENT_DIGESTION_THRESHOLD_BYTES);

    double bloom_filter_false_positive_rate
        = getOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);

    GinFilter::Parameters params(tokenizer, segment_digestion_threshold_bytes, bloom_filter_false_positive_rate, ngram_size, separators);
    return std::make_shared<MergeTreeIndexGin>(index, params, std::move(token_extractor));
}

void ginIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    std::unordered_map<String, Field> options = convertArgumentsToOptionsMap(index.arguments);

    /// Check that tokenizer is present and supported
    std::optional<String> tokenizer = getOption<String>(options, ARGUMENT_TOKENIZER);
    if (!tokenizer)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Text index must have an '{}' argument", ARGUMENT_TOKENIZER);

    const bool is_supported_tokenizer = (tokenizer.value() == DefaultTokenExtractor::getExternalName()
                                      || tokenizer.value() == NgramTokenExtractor::getExternalName()
                                      || tokenizer.value() == SplitTokenExtractor::getExternalName()
                                      || tokenizer.value() == NoOpTokenExtractor::getExternalName());
    if (!is_supported_tokenizer)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index argument '{}' supports only 'default', 'ngram', 'split', and 'no_op', but got {}",
            ARGUMENT_TOKENIZER,
            tokenizer.value());

    std::optional<UInt64> ngram_size;
    if (tokenizer.value() == NgramTokenExtractor::getExternalName())
    {
        ngram_size = getOption<UInt64>(options, ARGUMENT_NGRAM_SIZE);
        if (ngram_size.has_value() && (*ngram_size < 2 || *ngram_size > 8))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Text index argument '{}' must be between 2 and 8, but got {}",
                ARGUMENT_NGRAM_SIZE,
                *ngram_size);
    }
    else if (tokenizer.value() == SplitTokenExtractor::getExternalName())
    {
        std::optional<DB::FieldVector> separators = getOption<Array>(options, ARGUMENT_SEPARATORS);
        if (separators.has_value())
        {
            for (const auto & separator : separators.value())
                if (separator.getType() != Field::Types::String)
                    throw Exception(
                        ErrorCodes::INCORRECT_QUERY,
                        "Element of text index argument '{}' expected to be String, but got {}",
                        ARGUMENT_SEPARATORS,
                        separator.getTypeName());
        }
    }

    UInt64 segment_digestion_threshold_bytes
        = getOption<UInt64>(options, ARGUMENT_SEGMENT_DIGESTION_THRESHOLD_BYTES).value_or(UNLIMITED_SEGMENT_DIGESTION_THRESHOLD_BYTES);

    double bloom_filter_false_positive_rate
        = getOption<double>(options, ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE).value_or(DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);

    if (!std::isfinite(bloom_filter_false_positive_rate)
            || bloom_filter_false_positive_rate <= 0.0 || bloom_filter_false_positive_rate >= 1.0)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index argument '{}' must be between 0.0 and 1.0, but got {}",
            ARGUMENT_BLOOM_FILTER_FALSE_POSITIVE_RATE,
            bloom_filter_false_positive_rate);

    GinFilter::Parameters gin_filter_params(
        tokenizer.value(),
        segment_digestion_threshold_bytes,
        bloom_filter_false_positive_rate,
        ngram_size,
        getOptionAsStringArray(options, ARGUMENT_SEPARATORS).value_or(std::vector<String>{" "})); /// Just validate

    /// Check that the index is created on a single column
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Text index must be created on a single column");

    WhichDataType data_type(index.data_types[0]);
    if (data_type.isLowCardinality())
    {
        /// TODO Consider removing support for LowCardinality. The index exists for high-cardinality cases.
        const auto & low_cardinality = assert_cast<const DataTypeLowCardinality &>(*index.data_types[0]);
        data_type = WhichDataType(low_cardinality.getDictionaryType());
    }

    if (!data_type.isString() && !data_type.isFixedString())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Text index must be created on columns of type `String`, `FixedString`, `LowCardinality(String)`, `LowCardinality(FixedString)`");
}

}
