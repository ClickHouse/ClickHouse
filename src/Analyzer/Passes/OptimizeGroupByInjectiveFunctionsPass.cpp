#include <Analyzer/Passes/OptimizeGroupByInjectiveFunctionsPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/GroupByKeyComparator.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/ValidationUtils.h>

#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>

#include <algorithm>

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

/// A key f(g) eliminated from the GROUP BY key list, paired with the single non-constant argument g it
/// was rewritten to. See #110715.
struct EliminatedKey
{
    QueryTreeNodePtr original_key; /// f(g), the node still present in the output expressions
    QueryTreeNodePtr unwrapped_key; /// g, now a real GROUP BY key
};

/// A single-key list, so the caller can require the rewrite to be cardinality-preserving (exactly one
/// non-constant leaf) before applying it. Also enforces allow_suspicious_types.
QueryTreeNodes collectUnwrappedLeaves(const QueryTreeNodePtr & key, bool allow_suspicious_types)
{
    return unwrapInjectiveFunctionsInKeys({key}, allow_suspicious_types);
}

/// Keyed on aggregation-key identity, which unlike QueryTreeNodePtrWithHashIgnoreAliases distinguishes
/// l.number from r.number of a self-join. See src/Analyzer/GroupByKeyComparator.h.
using AggredationKeyIndexMap = AggredationKeyNodeMap<size_t>;
using EliminatedKeyReplacementMap = AggredationKeyNodeMap<QueryTreeNodePtr>;

/// Matches `grouping` and every specialization GroupingFunctionsResolvePass installs before this pass.
bool isGroupingFunction(const FunctionNode & function)
{
    const auto & name = function.getFunctionName();
    return name == "grouping" || name == "groupingOrdinary" || name == "groupingForRollup"
        || name == "groupingForCube" || name == "groupingForGroupingSets";
}

/// Replace each eliminated whole argument f(g) of a GROUPING call with its unwrapped key g. The value is
/// unaffected: the resolved specialization keeps the argument indexes it was built with.
///
/// Re-resolution is required, not optional: an IFunctionBase records the argument types it was built for
/// (FunctionToFunctionBaseAdaptor::getArgumentTypes), and both QueryTreePassManager's validator and
/// PlannerActionsVisitor compare those against the actual arguments. Grouping specializations are not in
/// FunctionFactory and carry per-query state, so the existing IFunction is re-wrapped, not looked up.
void rewriteGroupingArguments(FunctionNode & function, const EliminatedKeyReplacementMap & unwrapped_keys)
{
    bool substituted = false;

    for (auto & argument : function.getArguments().getNodes())
    {
        auto it = unwrapped_keys.find(argument);
        if (it == unwrapped_keys.end())
            continue;

        auto replacement = it->second->clone();
        if (argument->hasAlias())
            replacement->setAlias(argument->getAlias());
        argument = std::move(replacement);
        substituted = true;
    }

    if (!substituted)
        return;

    const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(function.getFunction().get());
    if (!adaptor)
        return;

    function.resolveAsFunction(
        std::make_shared<FunctionToOverloadResolverAdaptor>(adaptor->getFunction())->build(function.getArgumentColumns()));
}

/// In an output subtree (projection / ORDER BY / HAVING / LIMIT BY / INTERPOLATE), replaces every whole
/// occurrence of an eliminated key f(g) with its prebuilt grouping conditional.
///
/// A manual recursion, not an InDepthQueryTreeVisitor, because it must NOT descend into the replacement:
/// the conditional contains a copy of f(g) and would expand forever. It also stops at aggregate/window
/// nodes (their arguments are pre-aggregation, where __grouping_set does not exist) and at a GROUPING
/// call, where the argument is substituted instead. See #110715.
void rewriteOutputExpression(
    QueryTreeNodePtr & node,
    const EliminatedKeyReplacementMap & replacements,
    const EliminatedKeyReplacementMap & unwrapped_keys)
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

    if (auto * function = node->as<FunctionNode>())
    {
        /// Aggregate/window arguments are pre-aggregation: no __grouping_set there.
        if (function->isAggregateFunction() || function->isWindowFunction())
            return;

        if (isGroupingFunction(*function))
        {
            rewriteGroupingArguments(*function, unwrapped_keys);
            return;
        }
    }

    /// Do not descend into nested subqueries: they have their own GROUP BY scope.
    if (node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
        return;

    for (auto & child : node->getChildren())
        rewriteOutputExpression(child, replacements, unwrapped_keys);
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
        /// A grouping conditional corrects every row of a grouping-set modifier because a __grouping_set
        /// column exists on each of them. Plain WITH TOTALS has no such column, and no correction expressed
        /// outside the query tree survives conversion to AST, so its keys stay wrapped: return below.
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
            {
                /// The planner drops a constant key when the query has aggregates, and whether it does so
                /// also depends on flags this pass cannot see (initiator vs shard). The grouping conditional
                /// is built from a key count taken here, so a key that execution removes would make that
                /// count too large and the conditional would mis-decide present-vs-absent. Keep everything
                /// wrapped instead. See PlannerExpressionAnalysis.cpp, check_constants_for_group_by_key.
                if (key->as<ConstantNode>())
                    return;
                all_keys.insert(key);
            }

        std::vector<EliminatedKey> eliminated;
        /// Leaves already claimed, with the original key each came from. A later key may reuse a claimed
        /// leaf only if it is the SAME original key (one injective key repeated across grouping sets, e.g.
        /// GROUPING SETS ((k),(k,k))); two DIFFERENT keys sharing a leaf would be deduplicated into one
        /// aggregation key and shrink the lattice, so the second is kept wrapped.
        std::vector<std::pair<QueryTreeNodePtr, QueryTreeNodePtr>> chosen_leaves; /// (leaf, original_key)

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
                /// does, unwrapping would merge layouts and corrupt the sibling key's output. Identity is
                /// compared the way the aggregation layer does, so l.number and r.number do not collide.
                bool collides = false;
                for (const auto & other : all_keys)
                {
                    if (compareGroupByKeys(other.node, key))
                        continue;
                    if (compareGroupByKeys(other.node, leaf))
                    {
                        collides = true;
                        break;
                    }
                }
                if (collides)
                    continue;

                /// Cross-elimination collision: the leaf must not have been claimed by a DIFFERENT original
                /// key already eliminated (that would merge two lattice keys into one).
                bool leaf_taken_by_other = false;
                for (const auto & [claimed_leaf, claimed_key] : chosen_leaves)
                {
                    if (compareGroupByKeys(claimed_leaf, leaf) && !compareGroupByKeys(claimed_key, key))
                    {
                        leaf_taken_by_other = true;
                        break;
                    }
                }
                if (leaf_taken_by_other)
                    continue;

                /// If the key is still referenced from a window function, QUALIFY or the WINDOW clause, those
                /// occurrences are evaluated post-aggregation and are not fixed by the grouping conditional,
                /// so unwrapping would leave f(column-default) there on subtotal rows. Keep the key wrapped.
                if (reachesPostAggregationWindowOrQualify(key, query))
                    continue;

                /// The correction replaces the key's output occurrences with a conditional, so a conditional
                /// whose result type differs from the key's would change the declared column type and break
                /// any enclosing function already resolved against the original type. Must be checked here,
                /// before the unwrap below is committed.
                if (!groupingConditionalPreservesType(key))
                    continue;

                eliminated.push_back({key, leaf});
                chosen_leaves.emplace_back(leaf, key);
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

                AggredationKeyNodeMap<std::monostate> used_in_set;
                for (auto & key : set_keys)
                {
                    if (used_in_set.contains(key))
                        continue;
                    used_in_set.emplace(key, std::monostate{});

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

        EliminatedKeyReplacementMap replacements;
        /// The same eliminated keys mapped to their unwrapped key g, for GROUPING arguments (see
        /// rewriteGroupingArguments).
        EliminatedKeyReplacementMap unwrapped_keys;

        for (const auto & entry : eliminated)
        {
            auto index_it = aggregation_key_to_index.find(entry.unwrapped_key);
            if (index_it == aggregation_key_to_index.end())
                continue; /// Should not happen: the unwrapped key is now a real key.

            ColumnNumbers argument_indexes{index_it->second};

            auto grouping_conditional = buildGroupingConditional(
                entry.original_key, entry.unwrapped_key, argument_indexes, kind, aggregation_keys_size,
                grouping_sets_keys_indexes, force_compatibility);

            replacements.emplace(entry.original_key, std::move(grouping_conditional));
            unwrapped_keys.emplace(entry.original_key, entry.unwrapped_key);
        }

        if (replacements.empty())
            return;

        /// Every clause evaluated after aggregation must be corrected. Per Planner.cpp those are the
        /// projection, ORDER BY, HAVING, LIMIT BY and INTERPOLATE; QUALIFY and WINDOW are already declined
        /// by reachesPostAggregationWindowOrQualify, and LIMIT/OFFSET accept constants only.
        if (query.getProjectionNode())
            rewriteOutputExpression(query.getProjectionNode(), replacements, unwrapped_keys);
        if (query.hasOrderBy())
            rewriteOutputExpression(query.getOrderByNode(), replacements, unwrapped_keys);
        if (query.hasHaving())
            rewriteOutputExpression(query.getHaving(), replacements, unwrapped_keys);
        if (query.hasLimitBy())
            rewriteOutputExpression(query.getLimitByNode(), replacements, unwrapped_keys);
        if (query.hasInterpolate())
            rewriteOutputExpression(query.getInterpolate(), replacements, unwrapped_keys);
    }

    /// True if `key` occurs anywhere in the subtree rooted at `node` (inclusive), not descending into
    /// nested subqueries (they have their own scope).
    static bool subtreeContains(const QueryTreeNodePtr & node, const QueryTreeNodePtr & key)
    {
        if (!node)
            return false;
        if (node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
            return false;
        if (node->isEqual(*key, {.compare_aliases = false}))
            return true;
        for (const auto & child : node->getChildren())
            if (subtreeContains(child, key))
                return true;
        return false;
    }

    /// True if `key` occurs inside a window function subtree (its arguments or its OVER clause) anywhere in
    /// `node`, without descending into nested subqueries.
    static bool containsKeyUnderWindow(const QueryTreeNodePtr & node, const QueryTreeNodePtr & key)
    {
        if (!node)
            return false;
        if (node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
            return false;
        if (const auto * function = node->as<FunctionNode>())
            if (function->isWindowFunction())
                return subtreeContains(node, key);
        for (const auto & child : node->getChildren())
            if (containsKeyUnderWindow(child, key))
                return true;
        return false;
    }

    /// True if the key is referenced from a post-aggregation position that neither correction mechanism
    /// rewrites: inside a window function, anywhere in QUALIFY, or in a named WINDOW clause. Such an
    /// occurrence would still compute f(column-default), so the key is kept wrapped. See #110715.
    ///
    /// Must scan EVERY carrier buildAndApplyCorrections rewrites (projection, ORDER BY, HAVING, LIMIT BY,
    /// INTERPOLATE) plus QUALIFY and WINDOW, which have no rewrite at all. A carrier missing here is a
    /// wrong-results hole: rewriteOutputExpression stops at a window function, so the occurrence is
    /// neither declined nor corrected. Keep the two lists in step, using the same accessors.
    static bool reachesPostAggregationWindowOrQualify(const QueryTreeNodePtr & key, QueryNode & query)
    {
        if (query.getProjectionNode() && containsKeyUnderWindow(query.getProjectionNode(), key))
            return true;
        if (query.hasOrderBy() && containsKeyUnderWindow(query.getOrderByNode(), key))
            return true;
        if (query.hasHaving() && containsKeyUnderWindow(query.getHaving(), key))
            return true;
        if (query.hasLimitBy() && containsKeyUnderWindow(query.getLimitByNode(), key))
            return true;
        if (query.hasInterpolate() && containsKeyUnderWindow(query.getInterpolate(), key))
            return true;
        if (query.hasQualify() && subtreeContains(query.getQualify(), key))
            return true;
        if (query.hasWindow() && subtreeContains(query.getWindowNode(), key))
            return true;
        return false;
    }

    /// True if the conditional that would replace this key keeps the key's result type. `if` narrows some
    /// types: it cannot return LowCardinality at all, because FunctionIf reports
    /// canBeExecutedOnLowCardinalityDictionary() = false and the re-wrap in
    /// IFunctionOverloadResolver::getReturnType is gated on it.
    ///
    /// Only the condition-independent part is built: the first argument's value cannot affect the return
    /// type (FunctionIf::getReturnTypeImpl only type-checks it as UInt8), so a placeholder is sound and
    /// avoids duplicating the grouping-resolver construction.
    bool groupingConditionalPreservesType(const QueryTreeNodePtr & original_key)
    {
        auto result_type = original_key->getResultType();

        auto condition = std::make_shared<ConstantNode>(Field(UInt64(1)), std::make_shared<DataTypeUInt8>());
        auto default_const = std::make_shared<ConstantNode>(result_type->getDefault(), result_type);

        auto if_node = std::make_shared<FunctionNode>("if");
        if_node->getArguments().getNodes() = {std::move(condition), original_key->clone(), std::move(default_const)};
        resolveOrdinaryFunctionNodeByName(*if_node, "if", getContext());

        return if_node->getResultType()->equals(*result_type);
    }

    /// if(equals(groupingForKind(__grouping_set, unwrapped_key), present_value), original_key, default)
    ///
    /// The grouping argument must be `unwrapped_key`, not `original_key`: the value is identical either way,
    /// but ValidateGroupByColumnsVisitor requires a grouping argument to be a current GROUP BY key, and a
    /// distributed shard re-analyzes the serialized query from scratch. rewriteGroupingArguments applies the
    /// same rule to the user's own grouping calls.
    QueryTreeNodePtr buildGroupingConditional(
        const QueryTreeNodePtr & original_key,
        const QueryTreeNodePtr & unwrapped_key,
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
            NameAndTypePair{"__grouping_set", std::make_shared<DataTypeUInt64>()}, TableExpressionNodeWeakPtr{});

        auto grouping_function = std::make_shared<FunctionNode>("groupingForResolved");
        grouping_function->getArguments().getNodes() = {grouping_set_column, unwrapped_key->clone()};
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
