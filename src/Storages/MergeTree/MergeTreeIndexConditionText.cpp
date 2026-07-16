#include <Storages/MergeTree/MergeTreeIndexConditionText.h>

#include <Common/StringUtils.h>
#include <Common/OptimizedRegularExpression.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/hasAnyAllTokens.h>
#include <IO/WriteBufferFromString.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSet.h>

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
    extern const SettingsBool use_text_index_tokens_cache;
    extern const SettingsBool use_text_index_header_cache;
    extern const SettingsBool use_text_index_postings_cache;
    extern const SettingsUInt64 max_memory_usage;
    extern const SettingsBool use_text_index_like_evaluation_by_dictionary_scan;
    extern const SettingsUInt64 text_index_like_min_pattern_length;
}

TextSearchQuery::TextSearchQuery(String function_name_, TextSearchMode search_mode_, TextIndexDirectReadMode direct_read_mode_, std::vector<String> tokens_, std::vector<OptimizedRegularExpression> patterns_)
    : function_name(std::move(function_name_))
    , search_mode(search_mode_)
    , direct_read_mode(direct_read_mode_)
    , tokens(std::move(tokens_))
    , patterns(std::move(patterns_))
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
    {
        hash.update(token.size());
        hash.update(token);
    }

    if (!patterns.empty())
    {
        hash.update(patterns.size());
        for (const auto & pattern : patterns)
        {
            if (const auto & re2 = pattern.getRE2())
            {
                hash.update(re2->pattern());
            }
            else
            {
                std::string required_substring;
                bool is_trivial;
                bool required_substring_is_prefix;
                pattern.getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);
                hash.update(required_substring);
                hash.update(is_trivial);
                hash.update(required_substring_is_prefix);
            }
        }
    }

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
    if (settings[Setting::use_text_index_tokens_cache])
        tokens_cache = context_->getTextIndexTokensCache();
    else
        tokens_cache = std::make_shared<TextIndexTokensCache>(cache_policy, max_memory_usage, 0, 1.0);

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

            for (const auto & pattern : search_query->patterns)
                all_search_patterns.push_back(&pattern);
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
        case RPNElement::FUNCTION_HAS_ANY_TOKENS:
        {
            return true;
        }
        case RPNElement::FUNCTION_AND:
        case RPNElement::FUNCTION_EQUALS:
        case RPNElement::FUNCTION_LIKE:
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
        || function_name == "hasPhrase"
        || function_name == "equals"
        || function_name == "mapContainsKey"
        || function_name == "mapContainsKeyLike"
        || function_name == "mapContainsValue"
        || function_name == "mapContainsValueLike"
        || function_name == "has"
        || function_name == "like"
        || function_name == "ilike"
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
        || function_name == "hasPhrase"
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
    if (query.tokens.empty() && query.patterns.empty() && query.direct_read_mode == TextIndexDirectReadMode::Hint)
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
         RPNElement::FUNCTION_HAS_ANY_TOKENS,
         RPNElement::FUNCTION_HAS_ALL_TOKENS,
         RPNElement::FUNCTION_LIKE,
         RPNElement::FUNCTION_IN,
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
        else if (element.function == RPNElement::FUNCTION_LIKE)
        {
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAnyQueryPatterns(*text_search_query);
            rpn_stack.emplace_back(exists_in_granule, true);

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
        else if (element.function == RPNElement::FUNCTION_EQUALS)
        {
            chassert(element.text_search_queries.size() == 1);
            const auto & text_search_query = element.text_search_queries.front();
            bool exists_in_granule = granule->hasAllQueryTokensOrEmpty(*text_search_query);
            rpn_stack.emplace_back(exists_in_granule, true);

        }
        else if (element.function == RPNElement::FUNCTION_MATCH || element.function == RPNElement::FUNCTION_IN)
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

        if (traverseJSONSubcolumnKeyNode(function, out))
            return true;

        if (function_arguments_size != 2)
            return false;

        auto lhs_argument = function.getArgumentAt(0);
        auto rhs_argument = function.getArgumentAt(1);

        if ((function_name == "in" || function_name == "globalIn")
            && tryPrepareSetForTextSearch(lhs_argument, rhs_argument, function_name, out))
        {
            out.function = RPNElement::FUNCTION_IN;
            return true;
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
            else if (lhs_argument.tryGetConstant(const_value, const_type) && function_name == "equals")
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

std::vector<OptimizedRegularExpression> MergeTreeIndexConditionText::stringLikeToPatterns(const Field & field, bool case_insensitive) const
{
    /// Only handles the pure '%value%' form: one leading '%', a non-empty alphanumeric token immediately following,
    /// then one trailing '%' immediately after the token, and nothing else.
    /// Returns a single-element vector on success, empty on anything more complex.
    /// Only this form is eligible for direct read mode.

    const String value = preprocessor->processConstant(field.safeGet<String>());
    if (value.empty())
        return {};

    const char * data = value.data();
    const size_t length = value.size();
    size_t pos = 0;

    const auto is_token_char = [](unsigned char c) { return isASCII(c) && isAlphaNumericASCII(static_cast<char>(c)); };

    const size_t min_pattern_length = getContext()->getSettingsRef()[Setting::text_index_like_min_pattern_length];

    /// Must start with at least one '%'.
    if (data[pos] != '%')
        return {};

    while (pos < length && data[pos] == '%')
        ++pos;

    /// Alphanumeric content must follow immediately.
    if (pos >= length || !is_token_char(static_cast<unsigned char>(data[pos])))
        return {};

    const size_t start = pos;

    while (pos < length && is_token_char(static_cast<unsigned char>(data[pos])))
        ++pos;

    const size_t end = pos;

    /// Reject short needles: it might match too many dictionary tokens.
    if (end - start < min_pattern_length)
        return {};

    /// Trailing '%' must follow immediately after the content.
    if (pos >= length || data[pos] != '%')
        return {};

    while (pos < length && data[pos] == '%')
        ++pos;

    /// Nothing else may follow.
    if (pos < length)
        return {};

    String pattern;
    pattern += '%';
    pattern.append(data + start, end - start);
    pattern += '%';

    std::vector<OptimizedRegularExpression> patterns;
    if (case_insensitive)
        patterns.emplace_back(Regexps::createRegexp<true, true, true>(pattern));
    else
        patterns.emplace_back(Regexps::createRegexp<true, true, false>(pattern));
    return patterns;
}

/// Converts a Field value to its text representation using `serializeText`,
/// matching the format produced by `JSONAllValues`.
static String serializeFieldAsText(const Field & value, const DataTypePtr & type)
{
    auto column = type->createColumn();
    column->insert(value);
    WriteBufferFromOwnString buf;
    type->getDefaultSerialization()->serializeText(*column, 0, buf, {});
    return buf.str();
}

bool MergeTreeIndexConditionText::traverseFunctionNode(
    const RPNBuilderFunctionTreeNode & function_node,
    const RPNBuilderTreeNode & index_column_node,
    DataTypePtr value_type,
    Field value_field,
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
    else if (tryMatchNodeToJSONIndex(index_column_node, header, "JSONAllValues"))
    {
        has_index_column = true;
        direct_read_mode = getHintOrNoneMode();
        bool is_special_text_index_function = function_name == "hasAnyTokens" || function_name == "hasAllTokens";

        /// Convert non-string values to their text representation to match the format produced by `JSONAllValues`.
        /// Keep array values as-is for special text index functions.
        if (!is_special_text_index_function && !WhichDataType(value_type).isStringOrFixedString())
        {
            value_field = serializeFieldAsText(value_field, value_type);
            value_type = std::make_shared<DataTypeString>();
        }
    }

    /// Try to parse map subcolumn reference like `map.key_<serialized_key>` for `mapValues` index.
    if (!has_index_column && !has_map_keys_column && !has_map_values_column)
    {
        if (auto parsed = tryParseMapSubcolumnName(index_column_name))
        {
            auto & [map_column_name, _] = *parsed;
            if (header.has(fmt::format("mapValues({})", map_column_name))
                && value_field.getType() == Field::Types::String
                && !value_field.safeGet<String>().empty())
            {
                has_index_column = true;
                direct_read_mode = getHintOrNoneMode();
            }
        }
    }

    if (!has_index_column && !has_map_keys_column && !has_map_values_column)
        return false;

    auto value_data_type = WhichDataType(value_type);
    if (!value_data_type.isStringOrFixedString() && !value_data_type.isArray())
        return false;

    const auto & settings = getContext()->getSettingsRef();

    /// like/ilike optimization is only supported for splitByNonAlpha and array tokenizers.
    static const std::unordered_set<ITokenizer::Type> like_optimization_supported_tokenizers = {
        ITokenizer::Type::SplitByNonAlpha,
        ITokenizer::Type::Array
    };

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
        {
            const String & string_needle = value_field.safeGet<String>();
            if (!string_needle.empty())
            {
                /// hasToken uses splitByNonAlpha as its tokenizer, so:
                ///  - A needle without any word character (alphanumeric or non-ASCII) is invalid.
                ///  - Bypass the index in that case so the row-level evaluation throws BAD_ARGUMENTS (or returns NULL for hasTokenOrNull)
                ///  -- Consistnt with the no-index behaviour.
                /// If the needle does contain word characters (e.g. "abc" with ngrams(4)):
                ///  - It is valid but too short for the index's tokenizer:
                ///  -- Fall through to push "" so all granules are pruned and the query returns 0 rows.
                if (std::ranges::none_of(string_needle, [](unsigned char c) { return !isASCII(c) || isAlphaNumericASCII(c); }))
                    return false;
            }
            tokens.push_back("");
        }

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "hasPhrase")
    {
        /// Only splitByNonAlpha, splitByString, ngrams, and asciiCJK tokenizers are supported with the `hasPhrase` function.
        static const std::unordered_set<std::string_view> supported_tokenizers = {
            SplitByNonAlphaTokenizer::getExternalName(),
            SplitByStringTokenizer::getExternalName(),
            AsciiCJKTokenizer::getExternalName(),
            NgramsTokenizer::getExternalName(),
        };
        if (!supported_tokenizers.contains(tokenizer->getTokenizerExternalName()))
            return false;

        auto tokens = stringToTokens(value_field);
        out.function = RPNElement::FUNCTION_HAS_ALL_TOKENS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "startsWith" && tokenizer->supportsStringLike())
    {
        auto tokens = substringToTokens(value_field, true, false);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    if (function_name == "endsWith" && tokenizer->supportsStringLike())
    {
        auto tokens = substringToTokens(value_field, false, true);
        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(tokens)));
        return true;
    }
    /// Currently, not all token extractors support LIKE-style matching.
    if (function_name == "like")
    {
        const bool has_preprocessor = preprocessor && preprocessor->hasActions();
        /// Requires explicit opt-in via use_text_index_like_evaluation_by_dictionary_scan because scanning
        /// the index dictionary for pattern-matching tokens has non-trivial overhead.
        if (like_optimization_supported_tokenizers.contains(tokenizer->getType()) && !has_preprocessor
            && settings[Setting::use_text_index_like_evaluation_by_dictionary_scan])
        {
            /// TODO(ahmadov): Only '%foo%' pattern is eligible for direct read mode. An empty vector means the pattern is too complex.
            /// Add support for multiple patterns later with hint mode:
            /// 1. Handle multiple patterns e.g. %foo bar% -> postings_pattern(%foo) && postings_pattern(bar%) && regex(%foo bar%)
            /// 2. Handle exact tokens and patterns e.g. %foo bar baz% -> postings_exact(bar) && postings_pattern(%foo) && postings_pattern(bar%)
            /// 3. Fall-back to the brute-force search for short patterns e.g. %a% -> reading postings for tokens containing 'a' is expensive.
            /// 4. Fall-back to the brute-force search for other cases for now.
            /// Follow-up:
            /// 1. Handle more complex patterns e.g. %foo%bar% -> (postings_pattern(%foo%) && postings_pattern(%bar%)) || postings_pattern(%foo%bar%)
            auto patterns = stringLikeToPatterns(value_field, false);
            if (patterns.size() == 1)
            {
                out.function = RPNElement::FUNCTION_LIKE;
                out.text_search_queries.emplace_back(
                    std::make_shared<TextSearchQuery>(
                        function_name, TextSearchMode::All, TextIndexDirectReadMode::Exact, std::vector<String>(), std::move(patterns)));
                return true;
            }
        }

        /// When the like optimization is not enabled, we fallback to the HINT mode.
        if (!tokenizer->supportsStringLike())
            return false;

        std::vector<String> exact_tokens = stringLikeToTokens(value_field);

        out.function = RPNElement::FUNCTION_EQUALS;
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, direct_read_mode, std::move(exact_tokens)));
        return true;
    }
    if (function_name == "ilike" && like_optimization_supported_tokenizers.contains(tokenizer->getType())
        && settings[Setting::use_text_index_like_evaluation_by_dictionary_scan])
    {
        const bool has_preprocessor = preprocessor && preprocessor->hasActions();
        if (has_preprocessor && !preprocessor->isLowerOrUpper())
            return false;

        auto patterns = stringLikeToPatterns(value_field, true);
        if (patterns.size() == 1)
        {
            out.function = RPNElement::FUNCTION_LIKE;
            out.text_search_queries.emplace_back(
                std::make_shared<TextSearchQuery>(
                    function_name, TextSearchMode::All, TextIndexDirectReadMode::Exact, std::vector<String>(), std::move(patterns)));
            return true;
        }
        return false;
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
    /// Also handles map subcolumn references like `func(m.key_<serialized_key>, ...)`.
    /// It can be an arbitrary function that returns 0 for the default value of the map value type.
    /// It is true because `arrayElement` (and the equivalent subcolumn access) returns default value if key doesn't exist in the map,
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

    std::optional<String> key_const_value;

    if (isMap(required_column.type) && header.has(fmt::format("mapKeys({})", required_column.name)))
    {
        /// Try to find the `arrayElement` function in the DAG and extract the constant string key.
        std::vector<const ActionsDAG::Node *> stack;
        stack.push_back(outputs.front());

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
    }
    else
    {
        /// Try to parse map subcolumn reference like `map.key_<serialized_key>`.
        auto parsed = tryParseMapSubcolumnName(required_column.name);
        if (!parsed)
            return false;

        auto & [map_column_name, serialized_key] = *parsed;
        if (!header.has(fmt::format("mapKeys({})", map_column_name)))
            return false;

        key_const_value = std::move(serialized_key);
    }

    if (!key_const_value.has_value())
        return false;

    /// If the DAG contains a Set (e.g. from an IN subquery), try to build it before execution.
    /// The Set may not be ready yet because it is built later during query execution.
    for (const auto & node : subdag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::COLUMN)
            continue;

        const auto * column_set = checkAndGetColumn<ColumnSet>(node.column.get());
        if (!column_set)
            continue;

        auto future_set = column_set->getData();
        if (!future_set)
            return false;

        auto prepared_set = future_set->buildOrderedSetInplace(getContext());
        if (!prepared_set || !prepared_set->hasExplicitSetElements())
            return false;
    }

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

bool MergeTreeIndexConditionText::hasIndexForMapElementValue(const RPNBuilderTreeNode & node) const
{
    /// Handle `arrayElement(map_col, 'key')` form (i.e., `map['key']`).
    if (node.isFunction())
    {
        const auto function = node.toFunctionNode();
        if (function.getArgumentsSize() == 2 && function.getFunctionName() == "arrayElement")
        {
            const auto column_name = function.getArgumentAt(0).getColumnName();
            return header.has(fmt::format("mapValues({})", column_name));
        }
        return false;
    }

    /// Handle `map.key_<serialized_key>` subcolumn form.
    auto parsed = tryParseMapSubcolumnName(node.getColumnName());
    if (!parsed)
        return false;
    auto & [map_column_name, serialized_key] = *parsed;
    return header.has(fmt::format("mapValues({})", map_column_name));
}

bool MergeTreeIndexConditionText::traverseMapElementValueNode(const RPNBuilderTreeNode & index_column_node, const Field & const_value) const
{
    /// Here we check whether we can use index defined for `mapValues(m)`
    /// for functions like `func(arrayElement(m, 'const_key'), ...)`.
    /// If index can be used, than we can analyze the index as for scalar string column
    /// because `arrayElement(m, 'const_key')` projects Array(String) to String.
    if (const_value.getType() != Field::Types::String || const_value.safeGet<String>().empty())
        return false;

    return hasIndexForMapElementValue(index_column_node);
}

bool MergeTreeIndexConditionText::traverseJSONSubcolumnKeyNode(
    const RPNBuilderFunctionTreeNode & function_node, RPNElement & out) const
{
    /// Handle functions like `func(json.some.path, ...)` where `json.some.path`
    /// is a plain INPUT node referencing a JSON subcolumn.
    /// Similar to traverseMapElementKeyNode but for JSON subcolumns.

    const auto * dag_node = function_node.getDAGNode();
    if (!dag_node || !dag_node->function_base || !dag_node->isDeterministic()
        || !WhichDataType(removeNullable(dag_node->result_type)).isUInt8())
        return false;

    auto subdag = ActionsDAG::cloneSubDAG({dag_node}, true);
    auto required_columns = subdag.getRequiredColumns();
    const auto & outputs = subdag.getOutputs();

    if (required_columns.size() != 1 || outputs.size() != 1)
        return false;

    auto required_column = required_columns.front();
    auto output_column_name = outputs.front()->result_name;

    /// Try to match the required column to a JSON subcolumn with JSONAllPaths index.
    auto json_info = tryMatchJSONSubcolumnToIndex(required_column.name, header, "JSONAllPaths");
    if (!json_info)
        return false;

    /// If the DAG contains a Set (e.g. from an IN subquery), try to build it before execution.
    /// The Set may not be ready yet because it is built later during query execution.
    for (const auto & node : subdag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::COLUMN)
            continue;

        const auto * column_set = checkAndGetColumn<ColumnSet>(node.column.get());
        if (!column_set)
            continue;

        auto future_set = column_set->getData();
        if (!future_set)
            return false;

        auto prepared_set = future_set->buildOrderedSetInplace(getContext());
        if (!prepared_set || !prepared_set->hasExplicitSetElements())
            return false;
    }

    /// Evaluate the function on a default column value.
    /// If the function returns true for the default value (what we'd get when the path is missing),
    /// we cannot safely skip the granule.
    Block block{{required_column.type->createColumnConstWithDefaultValue(1),
                 required_column.type, required_column.name}};
    ExpressionActions actions(std::move(subdag));
    actions.execute(block);
    const auto & result_column = block.getByName(output_column_name).column;

    if (result_column->getBool(0))
        return false;

    auto tokens = stringToTokens(Field(json_info->path));
    out.function = RPNElement::FUNCTION_EQUALS;
    out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(
        "JSONPathExists", TextSearchMode::All, getHintOrNoneMode(), std::move(tokens)));
    return true;
}

bool MergeTreeIndexConditionText::tryPrepareSetForTextSearch(
    const RPNBuilderTreeNode & lhs,
    const RPNBuilderTreeNode & rhs,
    const String & function_name,
    RPNElement & out) const
{
    std::optional<size_t> set_key_position;

    auto has_index = [&](const RPNBuilderTreeNode & node)
    {
        return header.has(node.getColumnName())
            || hasIndexForMapElementValue(node)
            || tryMatchNodeToJSONIndex(node, header, "JSONAllValues");
    };

    if (lhs.isFunction() && lhs.toFunctionNode().getFunctionName() == "tuple")
    {
        const auto function = lhs.toFunctionNode();
        auto arguments_size = function.getArgumentsSize();

        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function.getArgumentAt(i);

            if (has_index(argument))
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
        if (has_index(lhs))
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
    /// Set columns with tuple may be unpacked. Unpack them here to get the correct column index.
    if (columns.size() == 1 && isTuple(columns.front()->getDataType()))
        columns = typeid_cast<const ColumnTuple &>(*columns.front()).getColumnsCopy();

    if (*set_key_position >= columns.size())
        return false;

    const auto & set_column = *columns[*set_key_position];
    if (!WhichDataType(set_column.getDataType()).isStringOrFixedString())
        return false;

    size_t total_row_count = prepared_set->getTotalRowCount();

    for (size_t row = 0; row < total_row_count; ++row)
    {
        auto ref = set_column.getDataAt(row);

        /// Reject the index usage when there is an empty string in the set.
        /// The condition with such a predicate will be always true on granule.
        /// See MergeTreeIndexGranuleText::hasAllQueryTokensOrEmpty.
        if (ref.empty())
        {
            out.text_search_queries.clear();
            return false;
        }

        const String value = preprocessor->processConstant(String(ref));
        std::vector<String> tokens;
        tokenizer->stringToTokens(value.data(), value.size(), tokens);
        out.text_search_queries.emplace_back(std::make_shared<TextSearchQuery>(function_name, TextSearchMode::All, TextIndexDirectReadMode::None, std::move(tokens)));
    }

    return true;
}

bool isTextIndexVirtualColumn(const String & column_name)
{
    return column_name.starts_with(TEXT_INDEX_VIRTUAL_COLUMN_PREFIX);
}

}
