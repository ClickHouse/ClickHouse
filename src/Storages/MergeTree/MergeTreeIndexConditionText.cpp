#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/misc.h>
#include <Functions/hasAnyAllTokens.h>
#include <Common/OptimizedRegularExpression.h>
#include <Interpreters/ExpressionActions.h>
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
    extern const SettingsBool query_plan_text_index_add_hint;
    extern const SettingsBool use_text_index_dictionary_cache;
    extern const SettingsBool use_text_index_header_cache;
    extern const SettingsBool use_text_index_postings_cache;
    extern const SettingsUInt64 max_memory_usage;
}

TextSearchQuery::TextSearchQuery(String function_name_, TextSearchMode search_mode_, TextIndexDirectReadMode direct_read_mode_, std::vector<String> tokens_)
    : function_name(std::move(function_name_))
    , search_mode(search_mode_)
    , direct_read_mode(direct_read_mode_)
    , tokens(std::move(tokens_))
{
    std::sort(tokens.begin(), tokens.end());
}

SipHash TextSearchQuery::getHash() const
{
    SipHash hash;
    hash.update(function_name);
    hash.update(search_mode);
    hash.update(direct_read_mode);
    hash.update(tokens.size());

    for (const auto & token : tokens)
        hash.update(token);

    return hash;
}

MergeTreeIndexConditionText::MergeTreeIndexConditionText(
    const ActionsDAG::Node * predicate,
    ContextPtr context_,
    const Block & index_sample_block,
    TokenizerPtr tokenizer_,
    MergeTreeIndexTextPreprocessorPtr preprocessor_)
    : WithContext(context_)
    , header(index_sample_block)
    , tokenizer(tokenizer_)
    , preprocessor(preprocessor_)
{
    if (!predicate)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    const auto & settings = context_->getSettingsRef();
    static constexpr auto cache_policy = "SLRU";
    size_t max_memory_usage = std::min(settings[Setting::max_memory_usage] / 10ULL, 100ULL * 1024 * 1024);

    /// If usage of global text index caches is disabled, create local
    /// one to share them between threads that read the same data parts.
    if (settings[Setting::use_text_index_dictionary_cache])
        dictionary_block_cache = context_->getTextIndexDictionaryBlockCache();
    else
        dictionary_block_cache = std::make_shared<TextIndexDictionaryBlockCache>(cache_policy, max_memory_usage, 0, 1.0);

    if (settings[Setting::use_text_index_header_cache])
        header_cache = context_->getTextIndexHeaderCache();
    else
        header_cache = std::make_shared<TextIndexHeaderCache>(cache_policy, max_memory_usage, 0, 1.0);

    if (settings[Setting::use_text_index_postings_cache])
        postings_cache = context_->getTextIndexPostingsCache();
    else
        postings_cache = std::make_shared<TextIndexPostingsCache>(cache_policy, max_memory_usage, 0, 1.0);

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
            all_search_queries[search_query->getHash().get128()] = search_query;
        }

        if (requiresReadingAllTokens(element))
            global_search_mode = TextSearchMode::Any;
    }

    all_search_tokens = Names(all_search_tokens_set.begin(), all_search_tokens_set.end());
    std::ranges::sort(all_search_tokens); /// Technically not necessary but leads to nicer read patterns on sorted dictionary blocks
}

bool MergeTreeIndexConditionText::requiresReadingAllTokens(const RPNElement & element)
{
    switch (element.function)
    {
        case RPNElement::FUNCTION_OR:
        case RPNElement::FUNCTION_NOT:
        case RPNElement::FUNCTION_NOT_IN:
        case RPNElement::FUNCTION_NOT_EQUALS:
        case RPNElement::FUNCTION_HAS_ANY_TOKENS:
        {
            return true;
        }
        case RPNElement::FUNCTION_AND:
        case RPNElement::FUNCTION_EQUALS:
        case RPNElement::FUNCTION_HAS_ALL_TOKENS:
        case RPNElement::FUNCTION_UNKNOWN:
        case RPNElement::ALWAYS_TRUE:
        case RPNElement::ALWAYS_FALSE:
        {
            return false;
        }
        case RPNElement::FUNCTION_IN:
        case RPNElement::FUNCTION_MATCH:
        {
            return element.text_search_queries.size() != 1;
        }
    }
}

bool MergeTreeIndexConditionText::isSupportedFunction(const String & function_name)
{
    return function_name == "hasToken"
        || function_name == "hasAnyTokens"
        || function_name == "hasAllTokens"
        || function_name == "equals"
        || function_name == "notEquals"
        || function_name == "mapContainsKey"
        || function_name == "mapContainsKeyLike"
        || function_name == "mapContainsValue"
        || function_name == "mapContainsValueLike"
        || function_name == "has"
        || function_name == "like"
        || function_name == "notLike"
        || function_name == "hasTokenOrNull"
        || function_name == "startsWith"
        || function_name == "endsWith"
        || function_name == "match";
}

TextIndexDirectReadMode MergeTreeIndexConditionText::getHintOrNoneMode() const
{
    const auto & settings = getContext()->getSettingsRef();
    return settings[Setting::query_plan_text_index_add_hint] ? TextIndexDirectReadMode::Hint : TextIndexDirectReadMode::None;
}

TextIndexDirectReadMode MergeTreeIndexConditionText::getDirectReadMode(const String & function_name) const
{
    if (function_name == "hasToken"
        || function_name == "hasAnyTokens"
        || function_name == "hasAllTokens")
    {
        return TextIndexDirectReadMode::Exact;
    }

    if (function_name == "equals"
        || function_name == "has"
        || function_name == "mapContainsKey"
        || function_name == "mapContainsValue")
    {
        /// These functions compare the searched token as a whole and therefore
        /// exact direct read is only possible with array token extractor, that doesn't
        /// split documents into tokens. Otherwise we can only use direct read as a hint.
        bool is_array_tokenizer = typeid_cast<const ArrayTokenizer *>(tokenizer);
        bool has_preprocessor = preprocessor && preprocessor->hasActions();
        return is_array_tokenizer && !has_preprocessor ? TextIndexDirectReadMode::Exact : getHintOrNoneMode();
    }


    if (function_name == "like"
        || function_name == "startsWith"
        || function_name == "endsWith"
        || function_name == "mapContainsKeyLike"
        || function_name == "mapContainsValueLike")
    {
        return getHintOrNoneMode();
    }

    return TextIndexDirectReadMode::None;
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
    if (query.tokens.empty() && query.direct_read_mode == TextIndexDirectReadMode::Hint)
        return std::nullopt;

    auto query_hash = query.getHash();
    auto it = all_search_queries.find(query_hash.get128());

    if (it == all_search_queries.end())
        return std::nullopt;

    auto hash_str = getSipHash128AsHexString(query_hash);
    String virtual_column_name = fmt::format("{}{}_{}_{}", TEXT_INDEX_VIRTUAL_COLUMN_PREFIX, index_name, query.function_name, hash_str);

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
         RPNElement::FUNCTION_HAS_ANY_TOKENS,
         RPNElement::FUNCTION_HAS_ALL_TOKENS,
         RPNElement::FUNCTION_IN,
         RPNElement::FUNCTION_NOT_IN,
         RPNElement::FUNCTION_MATCH});
}

bool MergeTreeIndexConditionText::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const
{
    const auto * granule = typeid_cast<const MergeTreeIndexGranuleText *>(idx_granule.get());
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index condition got a granule with the wrong type.");

    /// Check like in KeyCondition.
    std::vector<BoolMask> rpn_stack;
    size_t element_idx = 0;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_HAS_ANY_TOKENS)
        {
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAnyQueryTokens(*text_search_query);
            rpn_stack.emplace_back(exists_in_granule, true);
        }
        else if (element.function == RPNElement::FUNCTION_HAS_ALL_TOKENS)
        {
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAllQueryTokens(*text_search_query);
            rpn_stack.emplace_back(exists_in_granule, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAllQueryTokensOrEmpty(*text_search_query);
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
                if (granule->hasAllQueryTokensOrEmpty(*text_search_query))
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

        if (update_partial_disjunction_result_fn)
        {
            update_partial_disjunction_result_fn(element_idx, rpn_stack.back().can_be_true, element.function == RPNElement::FUNCTION_UNKNOWN);
            ++element_idx;
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in MergeTreeIndexConditionText::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

std::string MergeTreeIndexConditionText::getDescription() const
{
    std::string description = fmt::format("(mode: {}; tokens: [", global_search_mode);

    if (all_search_tokens.size() > 50)
    {
        description += fmt::format("... {} tokens ...", all_search_tokens.size());
    }
    else
    {
        for (size_t i = 0; i < all_search_tokens.size(); ++i)
        {
            if (i > 0)
                description += ", ";

            description += fmt::format("\"{}\"", all_search_tokens[i]);
        }
    }

    description += "])";
    return description;
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

        if (traverseMapElementKeyNode(function, out))
            return true;

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

std::vector<String> MergeTreeIndexConditionText::stringToTokens(const Field & field) const
{
    std::vector<String> tokens;
    const String value = preprocessor->processConstant(field.safeGet<String>());
    tokenizer->stringToTokens(value.data(), value.size(), tokens);
    return tokenizer->compactTokens(tokens);
}

std::vector<String> MergeTreeIndexConditionText::substringToTokens(const Field & field, bool is_prefix, bool is_suffix) const
{
    std::vector<String> tokens;
    const String value = preprocessor->processConstant(field.safeGet<String>());
    tokenizer->substringToTokens(value.data(), value.size(), tokens, is_prefix, is_suffix);
    return tokenizer->compactTokens(tokens);
}

std::vector<String> MergeTreeIndexConditionText::stringLikeToTokens(const Field & field) const
{
    std::vector<String> tokens;
    const String value = preprocessor->processConstant(field.safeGet<String>());
    tokenizer->stringLikeToTokens(value.data(), value.size(), tokens);
    return tokenizer->compactTokens(tokens);
}

bool MergeTreeIndexConditionText::traverseFunctionNode(
    const RPNBuilderFunctionTreeNode & function_node,
    const RPNBuilderTreeNode & index_column_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out) const
{
    const String function_name = function_node.getFunctionName();
    auto direct_read_mode = getDirectReadMode(function_name);

    auto index_column_name = index_column_node.getColumnName();
    bool has_index_column = header.has(index_column_name);
    bool has_map_keys_column = header.has(fmt::format("mapKeys({})", index_column_name));
    bool has_map_values_column = header.has(fmt::format("mapValues({})", index_column_name));

    if (traverseMapElementValueNode(index_column_node, value_field))
    {
        has_index_column = true;

        /// If we use index on `mapValues(m)` for `func(m['key'], 'value')`, we can use direct read only as a hint
        /// because we have to match the specific key to the value and therefore execute a real filter.
        direct_read_mode = getHintOrNoneMode();
    }

    if (!has_index_column && !has_map_keys_column && !has_map_values_column)
        return false;

    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    if (has_map_keys_column || has_map_values_column)
    {
        if (!value_data_type.isStringOrFixedString())
            return false;

        auto make_map_function = [&](auto tokens)
        {
            out.function = RPNElement::FUNCTION_EQUALS;
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
            return true;
        };

        /// mapContainsKey* can be used only with an index defined as `mapKeys(Map(String, ...))`
        if (has_map_keys_column)
        {
            if (function_name == "mapContainsKey" || function_name == "has")
                return make_map_function(stringToTokens(value_field));
            if (function_name == "mapContainsKeyLike" && tokenizer->supportsStringLike())
                return make_map_function(stringLikeToTokens(value_field));
        }

        /// mapContainsValue* can be used only with an index defined as `mapValues(Map(String, ...))`
        if (has_map_values_column)
        {
            if (function_name == "mapContainsValue")
                return make_map_function(stringToTokens(value_field));
            if (function_name == "mapContainsValueLike" && tokenizer->supportsStringLike())
                return make_map_function(stringLikeToTokens(value_field));
        }

        return false;
    }
    if (function_name == "notEquals")
    {
        auto tokens = stringToTokens(value_field);
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "equals")
    {
        auto tokens = stringToTokens(value_field);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "hasAnyTokens" || function_name == "hasAllTokens")
    {
        std::vector<String> search_tokens;

        // hasAny/AllTokens funcs accept either string which will be tokenized or array of strings to be used as-is
        if (value_data_type.isString())
        {
            search_tokens = stringToTokens(value_field);
        }
        else
        {
            for (const auto & element : value_field.safeGet<Array>())
            {
                if (element.getType() != Field::Types::String)
                    return false;

                search_tokens.push_back(element.safeGet<String>());
            }
        }

        if (function_name == "hasAnyTokens")
        {
            out.function = RPNElement::FUNCTION_HAS_ANY_TOKENS;
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::Any, direct_read_mode, search_tokens));
        }
        else
        {
            out.function = RPNElement::FUNCTION_HAS_ALL_TOKENS;
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, search_tokens));
        }

        return true;
    }
    if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        auto tokens = stringToTokens(value_field);
        if (tokens.empty())
            tokens.push_back("");

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "startsWith")
    {
        auto tokens = substringToTokens(value_field, true, false);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "endsWith")
    {
        auto tokens = substringToTokens(value_field, false, true);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    /// Currently, not all token extractors support LIKE-style matching.
    if (function_name == "like" && tokenizer->supportsStringLike())
    {
        std::vector<String> tokens = stringLikeToTokens(value_field);

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "notLike" && tokenizer->supportsStringLike())
    {
        std::vector<String> tokens = stringLikeToTokens(value_field);

        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "match" && tokenizer->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_MATCH;

        const auto & value = value_field.safeGet<String>();
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(value);

        if (!result.alternatives.empty())
        {
            for (const auto & alternative : result.alternatives)
            {
                auto tokens = substringToTokens(alternative, false, false);
                out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
            }
            return true;
        }
        if (!result.required_substring.empty())
        {
            auto tokens = substringToTokens(result.required_substring, false, false);
            out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
            return true;
        }
        return false;
    }
    if (function_name == "has")
    {
        auto tokens = stringToTokens(value_field);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }

    return false;
}

bool MergeTreeIndexConditionText::traverseMapElementKeyNode(const RPNBuilderFunctionTreeNode & function_node, RPNElement & out) const
{
    /// Here we check whether we can use index defined for `mapKeys(m)` for functions like `func(arrayElement(m, 'const_key'), ...)`.
    /// It can be an arbitrary function that returns 0 for the default value of the map value type.
    /// It is true because `arrayElement` return default value if key doesn't exist in the map,
    /// therefore we can use index to skip granules and use direct read as a hint for the original condition.

    const auto * dag_node = function_node.getDAGNode();
    if (!dag_node || !dag_node->function_base || !dag_node->isDeterministic() || !WhichDataType(dag_node->result_type).isUInt8())
        return false;

    auto subdag = ActionsDAG::cloneSubDAG({dag_node}, true);
    auto required_columns = subdag.getRequiredColumns();
    const auto & outputs = subdag.getOutputs();

    if (required_columns.size() != 1 || outputs.size() != 1)
        return false;

    auto required_column = required_columns.front();
    auto output_column_name = outputs.front()->result_name;

    if (!isMap(required_column.type) || !header.has(fmt::format("mapKeys({})", required_column.name)))
        return false;

    std::optional<String> key_const_value;
    std::vector<const ActionsDAG::Node *> stack;
    stack.push_back(outputs.front());

    /// Try to find the `arrayElement` function in the DAG and extract the constant string key.
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (node->type == ActionsDAG::ActionType::FUNCTION
            && node->children.size() == 2
            && node->function_base
            && node->function_base->getName() == "arrayElement")
        {
            if (key_const_value.has_value())
                return false;

            const auto & map_argument = node->children[0];
            const auto & const_key_argument = node->children[1];

            if (map_argument->type != ActionsDAG::ActionType::INPUT || map_argument->result_name != required_column.name)
                return false;

            if (const_key_argument->type != ActionsDAG::ActionType::COLUMN || !isStringOrFixedString(const_key_argument->result_type))
                return false;

            key_const_value = std::string{const_key_argument->column->getDataAt(0)};
        }
        else
        {
            for (const auto & child : node->children)
                stack.push_back(child);
        }
    }

    if (!key_const_value.has_value())
        return false;

    /// Evaluate function on the empty map. Empty map will return default value for any key.
    Block block{{required_column.type->createColumnConstWithDefaultValue(1), required_column.type, required_column.name}};
    ExpressionActions actions(std::move(subdag));
    actions.execute(block);
    const auto & result_column = block.getByName(output_column_name).column;

    /// If the function returns true for the empty map, we cannot use index.
    if (result_column->getBool(0))
        return false;

    auto tokens = stringToTokens(*key_const_value);
    out.function = RPNElement::FUNCTION_EQUALS;
    out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>("mapContainsKey", TextSearchMode::All, getHintOrNoneMode(), std::move(tokens)));
    return true;
}

bool MergeTreeIndexConditionText::traverseMapElementValueNode(const RPNBuilderTreeNode & index_column_node, const Field & const_value) const
{
    /// Here we check whether we can use index defined for `mapValues(m)`
    /// for functions like `func(arrayElement(m, 'const_key'), ...)`.
    /// If index can be used, than we can analyze the index as for scalar string column
    /// because `arrayElement(m, 'const_key')` projects Array(String) to String.

    if (!index_column_node.isFunction())
        return false;

    const auto function = index_column_node.toFunctionNode();
    const auto column_name = function.getArgumentAt(0).getColumnName();

    if (function.getArgumentsSize() != 2 || function.getFunctionName() != "arrayElement")
        return false;

    if (const_value.getType() != Field::Types::String || const_value.safeGet<String>().empty())
        return false;

    return header.has(fmt::format("mapValues({})", column_name));
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
        tokenizer->stringToTokens(ref.data(), ref.size(), tokens);
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, TextIndexDirectReadMode::None, std::move(tokens)));
    }

    return true;
}

bool isTextIndexVirtualColumn(const String & column_name)
{
    return column_name.starts_with(TEXT_INDEX_VIRTUAL_COLUMN_PREFIX);
}

}
