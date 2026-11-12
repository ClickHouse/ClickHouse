#include <Analyzer/Passes/TruncateOrderByAfterGroupByKeysPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Common/checkStackSize.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 distributed_group_by_no_merge;
    extern const SettingsBool optimize_aggregators_of_group_by_keys;
    extern const SettingsBool optimize_truncate_order_by_after_group_by_keys;
}

namespace
{

class TruncateOrderByAfterGroupByKeysVisitor : public InDepthQueryTreeVisitorWithContext<TruncateOrderByAfterGroupByKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<TruncateOrderByAfterGroupByKeysVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_truncate_order_by_after_group_by_keys])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy() || !query->hasOrderBy())
            return;

        /// Do not optimize with CUBE, ROLLUP, GROUPING SETS — the effective key set varies per row.
        if (query->isGroupByWithCube() || query->isGroupByWithRollup() || query->isGroupByWithGroupingSets())
            return;

        /// WITH TIES semantics depend on the full ORDER BY list.
        if (query->isLimitWithTies())
            return;

        /// If any ORDER BY element uses WITH FILL or COLLATE, do not optimize.
        /// Collation can make distinct GROUP BY key values compare equal,
        /// so tiebreaker expressions after them must be preserved.
        auto & order_by_nodes = query->getOrderBy().getNodes();
        for (auto & sort_elem : order_by_nodes)
        {
            auto * sort_node = sort_elem->as<SortNode>();
            if (sort_node->withFill() || sort_node->getCollator())
                return;
        }

        /// Collect GROUP BY keys.
        const auto & group_by_nodes = query->getGroupBy().getNodes();
        QueryTreeNodePtrWithHashSet group_by_keys;
        for (const auto & key : group_by_nodes)
        {
            auto [equal_key, inserted] = group_by_keys.emplace(key);
            if (!inserted && !haveSameValue(key, equal_key->node))
                return;
        }

        if (group_by_keys.empty())
            return;

        /// The keys that are left after `OptimizeGroupByFunctionKeysPass`, and the keys that all others are functions of.
        auto remaining_keys = group_by_nodes;
        auto independent_keys = group_by_nodes;
        if (std::ranges::any_of(group_by_nodes, hasReplaceableWithArguments))
        {
            if (canRemoveGroupByKeysThatAreFunctionsOfOtherKeys(*query))
                removeKeysThatAreFunctionsOfOtherKeys(
                    remaining_keys, {}, UsableGroupByKeys::AFTER_REPLACEMENTS, getGroupingFunctionArguments(*query));
            removeKeysThatAreFunctionsOfOtherKeys(independent_keys);
        }

        const bool replaces_aggregates = getSettings()[Setting::optimize_aggregators_of_group_by_keys] && !query->isGroupByWithTotals();

        /// Scan ORDER BY elements and track which GROUP BY keys have not been covered.
        QueryTreeNodePtrWithHashSet not_covered_remaining_keys(remaining_keys.begin(), remaining_keys.end());
        QueryTreeNodePtrWithHashSet not_covered_independent_keys(independent_keys.begin(), independent_keys.end());
        size_t independent_keys_end = order_by_nodes.size();
        size_t truncate_after = order_by_nodes.size();

        for (size_t i = 0; i < order_by_nodes.size(); ++i)
        {
            forEachCoveredKey(
                order_by_nodes[i]->as<SortNode &>().getExpression(),
                group_by_keys,
                replaces_aggregates,
                [&](const auto & key)
                {
                    not_covered_remaining_keys.erase(key);
                    not_covered_independent_keys.erase(key);
                });

            if (i < independent_keys_end && not_covered_independent_keys.empty())
                independent_keys_end = i + 1;

            if (not_covered_remaining_keys.empty())
            {
                truncate_after = i + 1;
                break;
            }
        }

        if (independent_keys_end == order_by_nodes.size())
            return;

        /// Different groups can be equal in the comparison: 0 and -0.
        bool has_merged_keys = false;
        QueryTreeNodePtrWithHashSet columns_of_merged_keys;
        for (const auto & key : independent_keys)
        {
            if (comparisonCanMergeDistinctValues(*key->getResultType()))
            {
                has_merged_keys = true;
                collectColumns(key, columns_of_merged_keys);
            }
        }

        /// After the keys, ORDER BY elements can order the rows of a group, and the groups that are equal in the comparison. Also, the
        /// value of a stateful function depends on whether it is calculated before the sorting.
        const auto has_stateful_function
            = [](const auto & sort_elem) { return hasStatefulFunction(sort_elem->template as<SortNode &>().getExpression()); };
        if (!canHaveSeveralRowsPerGroup(*query) && !has_merged_keys
            && std::none_of(order_by_nodes.begin() + truncate_after, order_by_nodes.end(), has_stateful_function))
            order_by_nodes.resize(truncate_after);

        /// Remove the elements that are equal in all these rows. Before all keys are covered, only if another pass replaces them, because
        /// then they cannot be calculated from the keys.
        /// Window functions are not supported over the results of an aggregation on shards.
        const bool has_unsupported_window_functions = getSettings()[Setting::distributed_group_by_no_merge] == 2
            && !hasFunctionNode(query->getProjectionNode(), "arrayJoin") && !hasFunctionNode(query->getOrderByNode(), "arrayJoin");
        size_t size = independent_keys_end;
        for (size_t i = independent_keys_end; i < order_by_nodes.size(); ++i)
        {
            const auto & expression = order_by_nodes[i]->as<SortNode &>().getExpression();
            const bool is_redundant
                = isFunctionOfKeys(expression, columns_of_merged_keys) && (i >= truncate_after || hasReplaceableWithArguments(expression));
            /// It is already replaced, or not supported.
            const bool cannot_be_calculated = (isFunctionOfKeys(expression, {}) && readsOtherColumns(expression, group_by_keys))
                || (i >= truncate_after && has_unsupported_window_functions && hasWindowFunctionNodes(expression));
            if (!is_redundant && !cannot_be_calculated)
                order_by_nodes[size++] = order_by_nodes[i];
        }
        order_by_nodes.resize(size);
    }

private:
    static void collectColumns(const QueryTreeNodePtr & node, QueryTreeNodePtrWithHashSet & columns)
    {
        checkStackSize();

        if (node->getNodeType() == QueryTreeNodeType::COLUMN)
            columns.insert(node);

        for (const auto & child : node->getChildren())
            if (child)
                collectColumns(child, columns);
    }

    static bool hasStatefulFunction(const QueryTreeNodePtr & node)
    {
        checkStackSize();

        if (const auto * function_node = node->as<FunctionNode>())
        {
            const auto function = function_node->getFunction();
            if (function && function->isStateful())
                return true;
        }

        return std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && hasStatefulFunction(child); });
    }

    /// An expression in ORDER BY of a query with GROUP BY is a function of keys, unless it has aggregate or window functions, or
    /// functions that return different values for the same arguments. It is not necessarily written with the keys, because it can
    /// be already replaced: `toYear(d) = 2024` with a condition on `d`.
    static bool isFunctionOfKeys(const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & columns_to_exclude)
    {
        checkStackSize();

        switch (node->getNodeType())
        {
            case QueryTreeNodeType::CONSTANT:
                return true;
            case QueryTreeNodeType::COLUMN:
                return !columns_to_exclude.contains(node);
            case QueryTreeNodeType::FUNCTION:
            {
                const auto & function_node = node->as<FunctionNode &>();
                const auto function = function_node.isOrdinaryFunction() ? function_node.getFunction() : nullptr;
                if (!function || !function->isDeterministic() || function->isServerConstant() || function->isStateful()
                    || function_node.getFunctionName() == "arrayJoin")
                    return false;

                return std::ranges::all_of(
                    function_node.getArguments().getNodes(),
                    [&](const auto & argument) { return isFunctionOfKeys(argument, columns_to_exclude); });
            }
            default:
                return false;
        }
    }

    /// Returns true if an expression that is a function of keys reads columns that are not keys, so it cannot be calculated after the
    /// aggregation.
    static bool readsOtherColumns(const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & keys)
    {
        checkStackSize();

        if (keys.contains(node))
            return false;

        if (node->getNodeType() == QueryTreeNodeType::COLUMN)
            return true;

        return std::ranges::any_of(node->getChildren(), [&](const auto & child) { return child && readsOtherColumns(child, keys); });
    }

    /// Equal expressions with constants written differently have the same value, but an `arrayJoin` is calculated separately for each of
    /// them, so the rows have all combinations of their values.
    static bool haveSameValue(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
    {
        return !hasFunctionNode(lhs, "arrayJoin") || areConstantsWrittenAlike(lhs, rhs);
    }

    /// Finds the GROUP BY keys that an expression covers.
    /// An expression covers a key if it equals the key directly,
    /// or if it is an injective function whose arguments cover the key(s).
    template <typename Callback>
    static void forEachCoveredKey(
        const QueryTreeNodePtr & expr,
        const QueryTreeNodePtrWithHashSet & group_by_keys,
        bool replaces_aggregates,
        const Callback & callback)
    {
        if (auto key = group_by_keys.find(expr); key != group_by_keys.end() && haveSameValue(expr, key->node))
        {
            callback(*key);
            return;
        }

        auto * function = expr->as<FunctionNode>();
        if (!function)
            return;

        /// `max(k)` is replaced with `k` later, see `UsableGroupByKeys`.
        if (replaces_aggregates && isAggregateFunctionOfEqualValues(*function))
        {
            const auto & argument = function->getArguments().getNodes().at(0);
            if (hasReplaceableWithArguments(argument))
                forEachCoveredKey(argument, group_by_keys, replaces_aggregates, callback);
            return;
        }

        /// Only ordinary (non-aggregate, non-window) functions can be injective.
        auto function_base = function->getFunction();
        if (!function_base)
            return;

        if (!function_base->isInjective(function->getArgumentColumns()))
            return;

        /// The function is injective — its arguments uniquely determine its value,
        /// so any GROUP BY keys covered by the arguments are also covered by this expression.
        for (const auto & arg : function->getArguments().getNodes())
            forEachCoveredKey(arg, group_by_keys, replaces_aggregates, callback);
    }
};

}

void TruncateOrderByAfterGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    TruncateOrderByAfterGroupByKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
