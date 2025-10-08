#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/misc.h>
#include <Functions/searchAnyAll.h>
#include <Common/OptimizedRegularExpression.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace Setting
{
    extern const SettingsBool text_index_use_bloom_filter;
}

UInt128 TextSearchQuery::getHash() const
{
    SipHash hash;
    hash.update(function_name);
    hash.update(mode);
    hash.update(tokens.size());

    for (const auto & token : tokens)
        hash.update(token);

    return hash.get128();
}

MergeTreeIndexConditionText::MergeTreeIndexConditionText(
    const ActionsDAG::Node * predicate,
    ContextPtr context_,
    const Block & index_sample_block,
    TokenExtractorPtr token_extactor_)
    : WithContext(context_)
    , header(index_sample_block)
    , token_extractor(token_extactor_)
    , use_bloom_filter(context_->getSettingsRef()[Setting::text_index_use_bloom_filter])
{
    if (!predicate)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    rpn = std::move(RPNBuilder<RPNElement>(
        predicate,
        context_,
        [&](const RPNBuilderTreeNode & node, RPNElement & out)
        {
            return this->traverseAtomNode(node, out);
        }).extractRPN());

    NameSet all_search_tokens_set;

    for (const auto & element : rpn)
    {
        for (const auto & search_query : element.text_search_queries)
        {
            all_search_tokens_set.insert(search_query->tokens.begin(), search_query->tokens.end());
            all_search_queries[search_query->getHash()] = search_query;
        }

        if (getTextSearchMode(element) == TextSearchMode::Any)
            global_search_mode = TextSearchMode::Any;
    }

    all_search_tokens = Names(all_search_tokens_set.begin(), all_search_tokens_set.end());
    std::ranges::sort(all_search_tokens); /// Technically not necessary but leads to nicer read patterns on sorted dictionary blocks
}

TextSearchMode MergeTreeIndexConditionText::getTextSearchMode(const RPNElement & element)
{
    if (element.function == RPNElement::FUNCTION_SEARCH_ALL
        || element.function == RPNElement::FUNCTION_AND
        || element.function == RPNElement::FUNCTION_EQUALS
        || (element.function == RPNElement::FUNCTION_MATCH && element.text_search_queries.size() == 1))
        return TextSearchMode::All;

    return TextSearchMode::Any;
}

bool MergeTreeIndexConditionText::isSupportedFunctionForDirectRead(const String & function_name)
{
    return function_name == "hasToken"
        || function_name == "searchAny"
        || function_name == "searchAll";
}

bool MergeTreeIndexConditionText::isSupportedFunction(const String & function_name)
{
    return isSupportedFunctionForDirectRead(function_name)
        || function_name == "equals"
        || function_name == "notEquals"
        || function_name == "like"
        || function_name == "notLike"
        || function_name == "hasTokenOrNull"
        || function_name == "startsWith"
        || function_name == "endsWith"
        || function_name == "match";
}

TextSearchQueryPtr MergeTreeIndexConditionText::createTextSearchQuery(const ActionsDAG::Node & node) const
{
    RPNElement rpn_element;
    RPNBuilderTreeContext rpn_tree_context(getContext());
    RPNBuilderTreeNode rpn_node(&node, rpn_tree_context);

    if (!traverseAtomNode(rpn_node, rpn_element))
        return nullptr;

    if (rpn_element.text_search_queries.size() != 1)
        return nullptr;

    return rpn_element.text_search_queries.front();
}

std::optional<String> MergeTreeIndexConditionText::replaceToVirtualColumn(const TextSearchQuery & query, const String & index_name)
{
    auto query_hash = query.getHash();
    auto it = all_search_queries.find(query_hash);

    if (it == all_search_queries.end())
        return std::nullopt;

    size_t counter = function_name_to_index[query.function_name]++;
    String virtual_column_name = fmt::format("{}{}_{}_{}", TEXT_INDEX_VIRTUAL_COLUMN_PREFIX, index_name, query.function_name, counter);

    virtual_column_to_search_query[virtual_column_name] = it->second;
    return virtual_column_name;
}

TextSearchQueryPtr MergeTreeIndexConditionText::getSearchQueryForVirtualColumn(const String & column_name) const
{
    auto it = virtual_column_to_search_query.find(column_name);
    if (it == virtual_column_to_search_query.end())
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Virtual column {} not found in MergeTreeIndexConditionText", column_name);

    return it->second;
}

bool MergeTreeIndexConditionText::alwaysUnknownOrTrue() const
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

bool MergeTreeIndexConditionText::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    const auto * granule = typeid_cast<const MergeTreeIndexGranuleText *>(idx_granule.get());
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index condition got a granule with the wrong type.");

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
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAnyTokenFromQuery(*text_search_query);
            rpn_stack.emplace_back(exists_in_granule, true);
        }
        else if (element.function == RPNElement::FUNCTION_SEARCH_ALL
            || element.function == RPNElement::FUNCTION_EQUALS
            || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAllTokensFromQuery(*text_search_query);
            rpn_stack.emplace_back(exists_in_granule, true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MATCH
            || element.function == RPNElement::FUNCTION_IN
            || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            bool exists_in_granule = false;

            for (const auto & text_search_query : element.text_search_queries)
            {
                if (granule->hasAllTokensFromQuery(*text_search_query))
                {
                    exists_in_granule = true;
                    break;
                }
            }

            rpn_stack.emplace_back(exists_in_granule, true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
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
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type {} in MergeTreeIndexConditionText::RPNElement", element.function);
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in MergeTreeIndexConditionText::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

bool MergeTreeIndexConditionText::traverseAtomNode(const RPNBuilderTreeNode & node, RPNElement & out) const
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
        auto function_name = function.getFunctionName();
        size_t function_arguments_size = function.getArgumentsSize();

        if (function_arguments_size != 2)
            return false;

        auto lhs_argument = function.getArgumentAt(0);
        auto rhs_argument = function.getArgumentAt(1);

        if (functionIsInOrGlobalInOperator(function_name))
        {
            if (tryPrepareSetForTextSearch(lhs_argument, rhs_argument, function_name, out))
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
        else if (isSupportedFunction(function_name))
        {
            Field const_value;
            DataTypePtr const_type;

            if (rhs_argument.tryGetConstant(const_value, const_type))
            {
                if (traverseFunctionNode(function, lhs_argument, const_type, const_value, out))
                    return true;
            }
            else if (lhs_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
            {
                if (traverseFunctionNode(function, rhs_argument, const_type, const_value, out))
                    return true;
            }
        }
    }

    return false;
}

static std::vector<String> stringToTokens(const Field & field, const ITokenExtractor & token_extractor)
{
    std::vector<String> tokens;
    const auto & value = field.safeGet<String>();
    token_extractor.stringToTokens(value.data(), value.size(), tokens);
    return tokens;
}

static std::vector<String> substringToTokens(const Field & field, const ITokenExtractor & token_extractor, bool is_prefix, bool is_suffix)
{
    std::vector<String> tokens;
    const auto & value = field.safeGet<String>();
    token_extractor.substringToTokens(value.data(), value.size(), tokens, is_prefix, is_suffix);
    return tokens;
}

bool MergeTreeIndexConditionText::traverseFunctionNode(
    const RPNBuilderFunctionTreeNode & function_node,
    const RPNBuilderTreeNode & index_column_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out) const
{
    bool index_column_exists = header.has(index_column_node.getColumnName());
    if (!index_column_exists)
        return false;

    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    const Field & const_value = value_field;
    const String & function_name = function_node.getFunctionName();

    if (function_name == "notEquals")
    {
        auto tokens = stringToTokens(const_value, *token_extractor);
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    if (function_name == "equals")
    {
        auto tokens = stringToTokens(const_value, *token_extractor);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    if (function_name == "searchAny" || function_name == "searchAll")
    {
        std::vector<String> search_tokens;

        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            search_tokens.push_back(element.safeGet<String>());
        }

        /// TODO(ahmadov): move this block to another place, e.g. optimizations or query tree re-write.
        const auto * function_dag_node = function_node.getDAGNode();
        chassert(function_dag_node != nullptr && function_dag_node->function_base != nullptr);

        const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(function_dag_node->function_base.get());
        chassert(adaptor != nullptr);

        if (function_name == "searchAny")
        {
            out.function = RPNElement::FUNCTION_SEARCH_ANY;
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::Any, search_tokens));

            auto & search_function = typeid_cast<FunctionSearchImpl<traits::SearchAnyTraits> &>(*adaptor->getFunction());
            search_function.setTokenExtractor(token_extractor->clone());
            search_function.setSearchTokens(search_tokens);
        }
        else
        {
            out.function = RPNElement::FUNCTION_SEARCH_ALL;
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, search_tokens));

            auto & search_function = typeid_cast<FunctionSearchImpl<traits::SearchAllTraits> &>(*adaptor->getFunction());
            search_function.setTokenExtractor(token_extractor->clone());
            search_function.setSearchTokens(search_tokens);
        }

        return true;
    }
    if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        auto tokens = stringToTokens(const_value, *token_extractor);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    if (function_name == "startsWith")
    {
        auto tokens = substringToTokens(const_value, *token_extractor, true, false);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    if (function_name == "endsWith")
    {
        auto tokens = substringToTokens(const_value, *token_extractor, false, true);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    /// Currently, not all token extractors support LIKE-style matching.
    if (function_name == "like" && token_extractor->supportsStringLike())
    {
        std::vector<String> tokens;
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToTokens(value.data(), value.size(), tokens);

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    if (function_name == "notLike" && token_extractor->supportsStringLike())
    {
        std::vector<String> tokens;
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToTokens(value.data(), value.size(), tokens);

        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
        return true;
    }
    if (function_name == "match" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_MATCH;

        const auto & value = const_value.safeGet<String>();
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(value);

        if (!result.alternatives.empty())
        {
            for (const auto & alternative : result.alternatives)
            {
                auto tokens = substringToTokens(alternative, *token_extractor, false, false);
                out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
            }
            return true;
        }
        if (!result.required_substring.empty())
        {
            auto tokens = substringToTokens(result.required_substring, *token_extractor, false, false);
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
            return true;
        }

        return false;
    }

    return false;
}

bool MergeTreeIndexConditionText::tryPrepareSetForTextSearch(
    const RPNBuilderTreeNode & lhs,
    const RPNBuilderTreeNode & rhs,
    const String & function_name,
    RPNElement & out) const
{
    std::optional<size_t> set_key_position;

    if (lhs.isFunction() && lhs.toFunctionNode().getFunctionName() == "tuple")
    {
        const auto function = lhs.toFunctionNode();
        auto arguments_size = function.getArgumentsSize();

        for (size_t i = 0; i < arguments_size; ++i)
        {
            if (header.has(function.getArgumentAt(i).getColumnName()))
            {
                /// Text index support only one index column.
                if (set_key_position.has_value())
                    return false;

                set_key_position = i;
            }
        }
    }
    else
    {
        if (header.has(lhs.getColumnName()))
            set_key_position = 0;
    }

    if (!set_key_position.has_value())
        return false;

    auto future_set = rhs.tryGetPreparedSet();
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(rhs.getTreeContext().getQueryContext());
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return false;

    Columns columns = prepared_set->getSetElements();
    const auto & set_column = *columns[*set_key_position];

    if (!WhichDataType(set_column.getDataType()).isStringOrFixedString())
        return false;

    size_t total_row_count = prepared_set->getTotalRowCount();

    for (size_t row = 0; row < total_row_count; ++row)
    {
        auto ref = set_column.getDataAt(row);

        std::vector<String> tokens;
        token_extractor->stringToTokens(ref.data, ref.size, tokens);
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, std::move(tokens)));
    }

    return true;
}

}
