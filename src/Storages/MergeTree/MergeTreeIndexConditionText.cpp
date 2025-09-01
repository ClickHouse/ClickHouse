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

namespace Setting
{
    extern const SettingsBool text_index_use_bloom_filter;
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

    rpn = std::move(RPNBuilder<RPNElement>(predicate, context_,
        [&](const RPNBuilderTreeNode & node, RPNElement & out)
        {
            return this->traverseAtomAST(node, out);
        }).extractRPN());

    NameSet all_search_tokens_set;

    auto collect_tokens = [&](const auto & gin_query_string)
    {
        for (const auto & token : gin_query_string.getTokens())
            all_search_tokens_set.insert(token);
    };

    for (const auto & element : rpn)
    {
        if (element.gin_query_string)
        {
            collect_tokens(*element.gin_query_string);
        }

        for (const auto & gin_query_strings : element.gin_query_strings_for_set)
        {
            for (const auto & gin_query_string : gin_query_strings)
                collect_tokens(gin_query_string);
        }

        if (getTextSearchMode(element) == TextSearchMode::Any)
        {
            global_search_mode = TextSearchMode::Any;
        }
    }

    all_search_tokens = Names(all_search_tokens_set.begin(), all_search_tokens_set.end());
    std::sort(all_search_tokens.begin(), all_search_tokens.end());
}

TextSearchMode MergeTreeIndexConditionText::getTextSearchMode(const RPNElement & element)
{
    if (element.function == RPNElement::FUNCTION_SEARCH_ANY
        || element.function == RPNElement::FUNCTION_OR
        || element.function == RPNElement::FUNCTION_IN
        || element.function == RPNElement::FUNCTION_NOT_IN)
    {
        return TextSearchMode::Any;
    }

    if (element.function == RPNElement::FUNCTION_MATCH)
    {
        return element.gin_query_strings_for_set.empty() ? TextSearchMode::All : TextSearchMode::Any;
    }

    return TextSearchMode::All;
}

/// Keep in-sync with MergeTreeIndexConditionText::alwaysUnknownOrTrue
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
            chassert(element.gin_query_strings_for_set.size() == 1);
            const auto & gin_query_strings = element.gin_query_strings_for_set.front();
            bool exists_in_granule = false;

            for (const auto & gin_query : gin_query_strings)
            {
                if (granule->hasAllTokensFromQuery(gin_query))
                {
                    exists_in_granule = true;
                    break;
                }
            }

            rpn_stack.emplace_back(exists_in_granule, true);
        }
        else if (element.function == RPNElement::FUNCTION_SEARCH_ALL)
        {
            chassert(element.gin_query_strings_for_set.size() == 1);
            const auto & gin_query_strings = element.gin_query_strings_for_set.front();
            bool exists_in_granule = true;

            for (const auto & gin_query : gin_query_strings)
            {
                if (!granule->hasAllTokensFromQuery(gin_query))
                {
                    exists_in_granule = false;
                    break;
                }
            }

            rpn_stack.emplace_back(exists_in_granule, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            rpn_stack.emplace_back(granule->hasAllTokensFromQuery(*element.gin_query_string), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.gin_query_strings_for_set.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const auto & gin_query_strings = element.gin_query_strings_for_set[column];
                for (size_t row = 0; row < gin_query_strings.size(); ++row)
                    result[row] = result[row] && granule->hasAllTokensFromQuery(gin_query_strings[row]);
            }

            rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MATCH)
        {
            if (!element.gin_query_strings_for_set.empty())
            {
                /// Alternative substrings
                std::vector<bool> result(element.gin_query_strings_for_set.back().size(), true);

                const auto & gin_query_strings = element.gin_query_strings_for_set[0];

                for (size_t row = 0; row < gin_query_strings.size(); ++row)
                    result[row] = granule->hasAllTokensFromQuery(gin_query_strings[row]);

                rpn_stack.emplace_back(std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            }
            else if (element.gin_query_string)
            {
                rpn_stack.emplace_back(granule->hasAllTokensFromQuery(*element.gin_query_string), true);
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

bool MergeTreeIndexConditionText::traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out)
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

bool MergeTreeIndexConditionText::traverseASTEquals(
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
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_query_string);
        return true;
    }
    if (function_name == "equals")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_query_string);
        return true;
    }
    if (function_name == "searchAny" || function_name == "searchAll")
    {
        std::vector<GinQueryString> gin_query_strings;
        std::vector<String> search_tokens;

        for (const auto & element : const_value.safeGet<Array>())
        {
            if (element.getType() != Field::Types::String)
                return false;

            const auto & value = element.safeGet<String>();
            gin_query_strings.emplace_back(GinQueryString(value, {value}));
            search_tokens.push_back(value);
        }

        out.function = function_name == "searchAny" ? RPNElement::FUNCTION_SEARCH_ANY : RPNElement::FUNCTION_SEARCH_ALL;
        out.gin_query_strings_for_set = std::vector<std::vector<GinQueryString>>{std::move(gin_query_strings)};

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
                search_function->setTokenExtractor(token_extractor->clone());
                search_function->setSearchTokens(search_tokens);
            }
            else
            {
                auto * search_function = typeid_cast<FunctionSearchImpl<traits::SearchAllTraits> *>(adaptor->getFunction().get());
                chassert(search_function != nullptr);
                search_function->setTokenExtractor(token_extractor->clone());
                search_function->setSearchTokens(search_tokens);
            }
        }

        return true;
    }
    if (function_name == "hasToken" || function_name == "hasTokenOrNull")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringToGinFilter(value.data(), value.size(), *out.gin_query_string);
        return true;
    }
    if (function_name == "startsWith")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToGinFilter(value.data(), value.size(), *out.gin_query_string, true, false);
        return true;
    }
    if (function_name == "endsWith")
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->substringToGinFilter(value.data(), value.size(), *out.gin_query_string, false, true);
        return true;
    }
    /// Currently, not all token extractors support LIKE-style matching.
    if (function_name == "like" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_EQUALS;
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.gin_query_string);
        return true;
    }
    if (function_name == "notLike" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_NOT_EQUALS;
        out.gin_query_string = std::make_unique<GinQueryString>();
        const auto & value = const_value.safeGet<String>();
        token_extractor->stringLikeToGinFilter(value.data(), value.size(), *out.gin_query_string);
        return true;
    }
    if (function_name == "match" && token_extractor->supportsStringLike())
    {
        out.function = RPNElement::FUNCTION_MATCH;

        const auto & value = const_value.safeGet<String>();
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(value);
        if (result.required_substring.empty() && result.alternatives.empty())
            return false;

        /// out.gin_query_strings_for_set means alternatives exist
        /// out.gin_filter means required_substring exists
        if (!result.alternatives.empty())
        {
            std::vector<std::vector<GinQueryString>> gin_query_strings;
            gin_query_strings.emplace_back();
            for (const auto & alternative : result.alternatives)
            {
                gin_query_strings.back().emplace_back();
                token_extractor->substringToGinFilter(alternative.data(), alternative.size(), gin_query_strings.back().back(), false, false);
            }
            out.gin_query_strings_for_set = std::move(gin_query_strings);
        }
        else
        {
            out.gin_query_string = std::make_unique<GinQueryString>();
            token_extractor->substringToGinFilter(result.required_substring.data(), result.required_substring.size(), *out.gin_query_string, false, false);
        }

        return true;
    }

    return false;
}

bool MergeTreeIndexConditionText::tryPrepareSetGinFilter(
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
            auto ref = column->getDataAt(row);
            token_extractor->stringToGinFilter(ref.data, ref.size, gin_query_infos.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.gin_query_strings_for_set = std::move(gin_query_infos);

    return true;
}

}
