#include <Analyzer/Passes/CoalesceHasPhrasePass.h>

#include <memory>
#include <optional>
#include <vector>

#include <Core/Field.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/logical.h>

#include <Interpreters/Context.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_has_phrase_or_chain;
    extern const SettingsBool optimize_rewrite_has_phrase_and_chain;
}

namespace
{

/// A `hasPhrase` call that is eligible to be folded into `hasAnyPhrases`/`hasAllPhrases`.
struct ParsedHasPhrase
{
    QueryTreeNodePtr column;             /// 1st argument (the input column expression).
    Field phrase;                        /// 2nd argument (a constant String phrase).
    std::optional<String> tokenizer;     /// 3rd argument value, if present (nullopt == no explicit tokenizer).
    QueryTreeNodePtr tokenizer_node;      /// 3rd argument node, reused verbatim when emitting the folded call.
};

std::optional<ParsedHasPhrase> tryParseHasPhrase(const QueryTreeNodePtr & node)
{
    const auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        return {};

    const auto & function_name = function_node->getFunctionName();
    if (function_name != "hasPhrase" && function_name != "matchPhrase")
        return {};

    const auto & arguments = function_node->getArguments().getNodes();
    if (arguments.size() < 2 || arguments.size() > 3)
        return {};

    const auto * phrase_constant = arguments[1]->as<ConstantNode>();
    if (!phrase_constant || !isString(phrase_constant->getResultType()))
        return {};

    ParsedHasPhrase result;
    result.column = arguments[0];
    result.phrase = phrase_constant->getValue();

    if (arguments.size() == 3)
    {
        const auto * tokenizer_constant = arguments[2]->as<ConstantNode>();
        if (!tokenizer_constant || !isString(tokenizer_constant->getResultType()))
            return {};
        result.tokenizer = tokenizer_constant->getValue().safeGet<String>();
        result.tokenizer_node = arguments[2];
    }

    return result;
}

/// Accumulates all `hasPhrase` calls that share the same (column, tokenizer) key within one OR/AND node.
struct PhraseGroup
{
    QueryTreeNodePtr column;
    std::optional<String> tokenizer;
    QueryTreeNodePtr tokenizer_node;
    Array phrases;
};

class CoalesceHasPhraseVisitor : public InDepthQueryTreeVisitorWithContext<CoalesceHasPhraseVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CoalesceHasPhraseVisitor>;
    using Base::Base;

    CoalesceHasPhraseVisitor(
        FunctionOverloadResolverPtr or_function_resolver_,
        FunctionOverloadResolverPtr and_function_resolver_,
        FunctionOverloadResolverPtr has_phrase_any_resolver_,
        FunctionOverloadResolverPtr has_phrase_all_resolver_,
        ContextPtr context)
        : Base(std::move(context))
        , or_function_resolver(std::move(or_function_resolver_))
        , and_function_resolver(std::move(and_function_resolver_))
        , has_phrase_any_resolver(std::move(has_phrase_any_resolver_))
        , has_phrase_all_resolver(std::move(has_phrase_all_resolver_))
    {
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & function_name = function_node->getFunctionName();
        const bool is_or = function_name == "or";
        const bool is_and = function_name == "and";
        if (!is_or && !is_and)
            return;

        /// `OR` and `AND` coalescing are gated separately. `AND` -> `hasAllPhrases` is off by default
        /// because, unlike `OR`, an `AND` chain short-circuits on the first absent phrase, so coalescing
        /// it into a single full-column scan can regress selective `AND` filters (see the settings docs).
        if (is_or && !getSettings()[Setting::optimize_rewrite_has_phrase_or_chain])
            return;
        if (is_and && !getSettings()[Setting::optimize_rewrite_has_phrase_and_chain])
            return;

        const auto & arguments = function_node->getArguments().getNodes();

        /// Group eligible `hasPhrase` arguments by (column, tokenizer); remember each argument's group.
        /// `groups` keeps insertion order (used when rebuilding the argument list). The lookup is keyed by
        /// the column's tree hash via `QueryTreeNodePtrWithHashMap` (O(1) average, `isEqual` only runs on a
        /// hash collision); the per-column inner scan only walks that column's tokenizer variants (usually one).
        std::vector<PhraseGroup> groups;
        QueryTreeNodePtrWithHashMap<std::vector<size_t>> column_to_group_indices;
        std::vector<int> argument_group(arguments.size(), -1);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto parsed = tryParseHasPhrase(arguments[i]);
            if (!parsed)
                continue;

            auto & group_indices = column_to_group_indices[parsed->column];

            int group_index = -1;
            for (size_t index : group_indices)
            {
                if (groups[index].tokenizer == parsed->tokenizer)
                {
                    group_index = static_cast<int>(index);
                    break;
                }
            }

            if (group_index == -1)
            {
                group_index = static_cast<int>(groups.size());
                groups.push_back(PhraseGroup{parsed->column, parsed->tokenizer, parsed->tokenizer_node, Array{}});
                group_indices.push_back(group_index);
            }

            groups[group_index].phrases.push_back(parsed->phrase);
            argument_group[i] = group_index;
        }

        /// Only groups with at least two phrases are worth folding (one phrase has nothing to coalesce).
        bool has_foldable_group = false;
        for (const auto & group : groups)
            if (group.phrases.size() >= 2)
                has_foldable_group = true;

        if (!has_foldable_group)
            return;

        const auto & target_resolver = is_or ? has_phrase_any_resolver : has_phrase_all_resolver;
        const auto * target_function_name = is_or ? "hasAnyPhrases" : "hasAllPhrases";

        std::vector<QueryTreeNodePtr> folded_nodes(groups.size());
        for (size_t g = 0; g < groups.size(); ++g)
            if (groups[g].phrases.size() >= 2)
                folded_nodes[g] = buildFoldedNode(groups[g], target_function_name, target_resolver);

        /// Rebuild the argument list: keep non-foldable arguments in place, emit each folded node once
        /// at the position of the group's first argument and drop the rest.
        QueryTreeNodes new_arguments;
        new_arguments.reserve(arguments.size());
        std::vector<bool> emitted(groups.size(), false);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const int group_index = argument_group[i];
            if (group_index == -1 || !folded_nodes[group_index])
            {
                new_arguments.push_back(arguments[i]);
                continue;
            }

            if (!emitted[group_index])
            {
                new_arguments.push_back(folded_nodes[group_index]);
                emitted[group_index] = true;
            }
        }

        /// `or`/`and` require at least two arguments. If everything collapsed into a single folded node,
        /// append the operator's identity element (`OR 0` / `AND 1`), which leaves the result unchanged.
        if (new_arguments.size() == 1)
            new_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(is_or ? 0 : 1)));

        function_node->getArguments().getNodes() = std::move(new_arguments);
        function_node->resolveAsFunction(is_or ? or_function_resolver : and_function_resolver);
    }

private:
    QueryTreeNodePtr buildFoldedNode(
        const PhraseGroup & group, const String & target_function_name, const FunctionOverloadResolverPtr & target_resolver) const
    {
        auto folded = std::make_shared<FunctionNode>(target_function_name);
        auto & folded_arguments = folded->getArguments().getNodes();

        folded_arguments.push_back(group.column);

        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        folded_arguments.push_back(std::make_shared<ConstantNode>(Field{group.phrases}, array_type));

        if (group.tokenizer.has_value())
            folded_arguments.push_back(group.tokenizer_node);

        folded->resolveAsFunction(target_resolver);
        return folded;
    }

    const FunctionOverloadResolverPtr or_function_resolver;
    const FunctionOverloadResolverPtr and_function_resolver;
    const FunctionOverloadResolverPtr has_phrase_any_resolver;
    const FunctionOverloadResolverPtr has_phrase_all_resolver;
};

}

void CoalesceHasPhrasePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto or_function_resolver = createInternalFunctionOrOverloadResolver();
    auto and_function_resolver = createInternalFunctionAndOverloadResolver();
    auto has_phrase_any_resolver = FunctionFactory::instance().get("hasAnyPhrases", context);
    auto has_phrase_all_resolver = FunctionFactory::instance().get("hasAllPhrases", context);

    CoalesceHasPhraseVisitor visitor(
        std::move(or_function_resolver),
        std::move(and_function_resolver),
        std::move(has_phrase_any_resolver),
        std::move(has_phrase_all_resolver),
        std::move(context));
    visitor.visit(query_tree_node);
}

}
