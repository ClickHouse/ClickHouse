#include <Analyzer/Passes/OptimizeGroupByInjectiveFunctionsPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool force_grouping_standard_compatibility;
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_injective_functions_in_group_by;
    extern const SettingsBool allow_suspicious_types_in_group_by;
}

namespace
{

enum class GroupByKind : uint8_t
{
    ROLLUP,
    CUBE,
    GROUPING_SETS
};

/// A key f(g) that was eliminated from the GROUP BY key list together with the single non-constant
/// argument g it was rewritten to. Under a GROUP BY modifier the output projection still recomputes
/// f(g); for rows where g is absent from the set being aggregated (CUBE/ROLLUP subtotals, GROUPING SETS
/// non-member sets) g takes its column default, so f(g) becomes f(default) instead of the required
/// defaultOf(typeOf(f(g))). We keep the pair here to guard each occurrence of f(g) with a grouping
/// conditional after the keys are rewritten. See #110715.
struct EliminatedKey
{
    QueryTreeNodePtr original_key; /// f(g), the node still present in the projection/ORDER BY/HAVING
    QueryTreeNodePtr unwrapped_key; /// g, now a real GROUP BY key
};

/// The leaves an injective key would be rewritten to, reusing the shared unwrap logic (which also
/// enforces allow_suspicious_types). Operating on a single-key list lets us require the rewrite to be
/// cardinality-preserving (exactly one non-constant leaf) before applying it under a modifier.
QueryTreeNodes collectUnwrappedLeaves(const QueryTreeNodePtr & key, bool allow_suspicious_types)
{
    return unwrapInjectiveFunctionsInKeys({key}, allow_suspicious_types);
}

using AggredationKeyIndexMap = std::unordered_map<QueryTreeNodePtrWithHashIgnoreAliases, size_t>;

/// Rewrites, in an output subtree (projection / ORDER BY / HAVING), every whole occurrence of an
/// eliminated injective key f(g) into a grouping conditional if(present, f(g), default). This is a
/// manual recursion (NOT an InDepthQueryTreeVisitor): a matched node is replaced by the prebuilt
/// conditional and we do NOT descend into that conditional, because it itself contains a copy of f(g)
/// which would otherwise match and expand forever. Recursion stops at aggregate/window function nodes:
/// their arguments are evaluated before aggregation, where the __grouping_set column does not exist, so
/// those occurrences of f(g) must be left untouched. See #110715.
void rewriteOutputExpression(
    QueryTreeNodePtr & node,
    const std::unordered_map<QueryTreeNodePtrWithHashIgnoreAliases, QueryTreeNodePtr> & replacements)
{
    if (!node)
        return;

    auto it = replacements.find(node);
    if (it != replacements.end())
    {
        /// Preserve the alias so ORDER BY / HAVING references that resolve through it keep working.
        auto replacement = it->second->clone();
        if (node->hasAlias())
            replacement->setAlias(node->getAlias());
        node = std::move(replacement);
        return; /// Do not recurse into the replacement (it contains a copy of f(g)).
    }

    if (const auto * function = node->as<FunctionNode>())
    {
        /// Aggregate/window arguments are pre-aggregation: no __grouping_set there.
        if (function->isAggregateFunction() || function->isWindowFunction())
            return;
    }

    /// Do not descend into nested subqueries: they have their own GROUP BY scope.
    if (node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
        return;

    for (auto & child : node->getChildren())
        rewriteOutputExpression(child, replacements);
}

class OptimizeGroupByInjectiveFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>;
public:
    explicit OptimizeGroupByInjectiveFunctionsVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_injective_functions_in_group_by])
            return;

        /// Don't optimize injective functions when group_by_use_nulls=true,
        /// because in this case we make initial group by keys Nullable
        /// and eliminating some functions can cause issues with arguments Nullability
        /// during their execution. See examples in https://github.com/ClickHouse/ClickHouse/pull/61567#issuecomment-2008181143
        if (getSettings()[Setting::group_by_use_nulls])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy())
            return;

        bool allow_suspicious_types = getSettings()[Setting::allow_suspicious_types_in_group_by];

        const bool with_modifier = query->isGroupByWithCube() || query->isGroupByWithRollup()
            || query->isGroupByWithGroupingSets() || query->isGroupByWithTotals();

        if (!with_modifier)
        {
            /// Plain GROUP BY: every output row carries the key, so the rewrite is unconditionally safe.
            auto & group_by = query->getGroupBy().getNodes();
            group_by = unwrapInjectiveFunctionsInKeys(group_by, allow_suspicious_types);
            return;
        }

        optimizeWithModifier(*query, allow_suspicious_types);
    }

private:
    /// Under a GROUP BY modifier we only unwrap keys whose rewrite is cardinality-preserving and does not
    /// collide with another key (so the final key layout is exactly the original with f(g) -> g), then we
    /// guard the eliminated key's output occurrences with a grouping conditional so absent-key rows emit
    /// the correct output-type default instead of f(default). See #110715.
    void optimizeWithModifier(QueryNode & query, bool allow_suspicious_types)
    {
        /// WITH TOTALS on a plain/ROLLUP/CUBE/GROUPING SETS query is handled elsewhere (the totals row has
        /// no __grouping_set column). If the only modifier is WITH TOTALS we cannot correct it here, so we
        /// must not unwrap at all.
        const bool has_grouping_set_column = query.isGroupByWithCube() || query.isGroupByWithRollup()
            || query.isGroupByWithGroupingSets();
        if (!has_grouping_set_column)
            return;

        /// GROUPING SETS is a list of lists; ROLLUP/CUBE is a flat list treated as a single set.
        std::vector<QueryTreeNodes *> sets;
        if (query.isGroupByWithGroupingSets())
        {
            for (auto & set : query.getGroupBy().getNodes())
                sets.push_back(&set->as<ListNode>()->getNodes());
        }
        else
            sets.push_back(&query.getGroupBy().getNodes());

        /// A key may only be unwrapped if its single non-constant leaf does not appear as (or inside)
        /// another key anywhere in the GROUP BY. Build the set of all current keys to test collisions.
        QueryTreeNodePtrWithHashSet all_keys;
        for (const auto * set : sets)
            for (const auto & key : *set)
                all_keys.insert(key);

        std::vector<EliminatedKey> eliminated;

        for (auto * set : sets)
        {
            for (auto & key : *set)
            {
                const auto * function_node = key->as<FunctionNode>();
                if (!function_node)
                    continue;

                auto leaves = collectUnwrappedLeaves(key, allow_suspicious_types);

                /// Cardinality-preserving: exactly one non-constant leaf, and it is genuinely a rewrite
                /// (the key is not already the bare leaf).
                if (leaves.size() != 1)
                    continue;

                const auto & leaf = leaves.front();
                if (leaf->isEqual(*key))
                    continue;

                /// Collision guard: the leaf must not equal any OTHER key present in the GROUP BY. If it
                /// does, unwrapping would merge layouts and corrupt the sibling key's output.
                bool collides = false;
                for (const auto & other : all_keys)
                {
                    if (other.node->isEqual(*key))
                        continue;
                    if (other.node->isEqual(*leaf))
                    {
                        collides = true;
                        break;
                    }
                }
                if (collides)
                    continue;

                eliminated.push_back({key, leaf});
                key = leaf; /// Perform the unwrap in place, preserving key position within the set.
            }
        }

        if (eliminated.empty())
            return;

        buildAndApplyCorrections(query, eliminated);
    }

    /// Build a resolved grouping conditional for each eliminated key and rewrite the query's output
    /// expressions. The layout (kind + per-set key indices) is computed from the POST-unwrap GROUP BY,
    /// mirroring GroupingFunctionsResolvePass so the grouping function agrees with the execution layer.
    void buildAndApplyCorrections(QueryNode & query, const std::vector<EliminatedKey> & eliminated)
    {
        GroupByKind kind = GroupByKind::GROUPING_SETS;
        if (query.isGroupByWithRollup())
            kind = GroupByKind::ROLLUP;
        else if (query.isGroupByWithCube())
            kind = GroupByKind::CUBE;

        /// Assign a flat index to every distinct aggregation key, and record which indices each grouping
        /// set uses (only meaningful for GROUPING SETS). Same construction as resolveGroupingFunctions.
        AggredationKeyIndexMap aggregation_key_to_index;
        ColumnNumbersList grouping_sets_keys_indexes;
        size_t next_index = 0;

        if (query.isGroupByWithGroupingSets())
        {
            for (auto & set_node : query.getGroupBy().getNodes())
            {
                auto & set_keys = set_node->as<ListNode>()->getNodes();
                grouping_sets_keys_indexes.emplace_back();
                auto & set_indexes = grouping_sets_keys_indexes.back();

                QueryTreeNodePtrWithHashSet used_in_set;
                for (auto & key : set_keys)
                {
                    if (used_in_set.contains(key))
                        continue;
                    used_in_set.insert(key);

                    auto [it, inserted] = aggregation_key_to_index.emplace(key, next_index);
                    if (inserted)
                        ++next_index;
                    set_indexes.push_back(it->second);
                }
            }
        }
        else
        {
            for (auto & key : query.getGroupBy().getNodes())
            {
                auto [it, inserted] = aggregation_key_to_index.emplace(key, next_index);
                if (inserted)
                    ++next_index;
            }
        }

        const size_t aggregation_keys_size = aggregation_key_to_index.size();
        const bool force_compatibility = getSettings()[Setting::force_grouping_standard_compatibility];

        std::unordered_map<QueryTreeNodePtrWithHashIgnoreAliases, QueryTreeNodePtr> replacements;

        for (const auto & entry : eliminated)
        {
            auto index_it = aggregation_key_to_index.find(entry.unwrapped_key);
            if (index_it == aggregation_key_to_index.end())
                continue; /// Should not happen: the unwrapped key is now a real key.

            ColumnNumbers argument_indexes{index_it->second};

            auto grouping_conditional = buildGroupingConditional(
                entry.original_key, argument_indexes, kind, aggregation_keys_size,
                grouping_sets_keys_indexes, force_compatibility);

            replacements.emplace(entry.original_key, std::move(grouping_conditional));
        }

        if (replacements.empty())
            return;

        if (query.getProjectionNode())
            rewriteOutputExpression(query.getProjectionNode(), replacements);
        if (query.hasOrderBy())
            rewriteOutputExpression(query.getOrderByNode(), replacements);
        if (query.hasHaving())
            rewriteOutputExpression(query.getHaving(), replacements);
    }

    /// if(equals(groupingForKind(__grouping_set, unwrapped_key), present_value), original_key, default)
    QueryTreeNodePtr buildGroupingConditional(
        const QueryTreeNodePtr & original_key,
        const ColumnNumbers & argument_indexes,
        GroupByKind kind,
        size_t aggregation_keys_size,
        const ColumnNumbersList & grouping_sets_keys_indexes,
        bool force_compatibility)
    {
        /// Resolved specialized grouping function over __grouping_set + the unwrapped key column.
        FunctionOverloadResolverPtr grouping_resolver;
        switch (kind)
        {
            case GroupByKind::ROLLUP:
                grouping_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionGroupingForRollup>(argument_indexes, aggregation_keys_size, force_compatibility));
                break;
            case GroupByKind::CUBE:
                grouping_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionGroupingForCube>(argument_indexes, aggregation_keys_size, force_compatibility));
                break;
            case GroupByKind::GROUPING_SETS:
                grouping_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(
                    std::make_shared<FunctionGroupingForGroupingSets>(argument_indexes, grouping_sets_keys_indexes, force_compatibility));
                break;
        }

        auto grouping_set_column = std::make_shared<ColumnNode>(
            NameAndTypePair{"__grouping_set", std::make_shared<DataTypeUInt64>()}, QueryTreeNodeWeakPtr{});

        auto grouping_function = std::make_shared<FunctionNode>("groupingForResolved");
        grouping_function->getArguments().getNodes() = {grouping_set_column, original_key->clone()};
        grouping_function->resolveAsFunction(grouping_resolver->build(grouping_function->getArgumentColumns()));

        /// The grouping function returns 0 for a present single key when force_compatibility (the
        /// default), otherwise it returns an all-ones bitmask (here just 1 for one argument).
        const UInt64 present_value = force_compatibility ? 0 : ((UInt64(1) << argument_indexes.size()) - 1);
        auto present_const = std::make_shared<ConstantNode>(present_value, std::make_shared<DataTypeUInt64>());

        auto equals_node = std::make_shared<FunctionNode>("equals");
        equals_node->getArguments().getNodes() = {std::move(grouping_function), std::move(present_const)};
        resolveOrdinaryFunctionNodeByName(*equals_node, "equals", getContext());

        /// Column default of the eliminated output's result type (e.g. '' for String, 0 for numbers).
        auto result_type = original_key->getResultType();
        auto default_const = std::make_shared<ConstantNode>(result_type->getDefault(), result_type);

        auto if_node = std::make_shared<FunctionNode>("if");
        if_node->getArguments().getNodes() = {std::move(equals_node), original_key->clone(), std::move(default_const)};
        resolveOrdinaryFunctionNodeByName(*if_node, "if", getContext());

        return if_node;
    }
};

}

void OptimizeGroupByInjectiveFunctionsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeGroupByInjectiveFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
