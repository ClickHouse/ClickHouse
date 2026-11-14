#include <Analyzer/Passes/TruncateOrderByAfterGroupByKeysPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/ColumnNode.h>
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
#include <unordered_map>
#include <unordered_set>

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
        const Keys group_by_keys(group_by_nodes);
        if (group_by_nodes.empty())
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
        std::unordered_set<QueryTreeNodePtr> not_covered_remaining_keys;
        for (const auto & key : remaining_keys)
            not_covered_remaining_keys.insert(group_by_keys.find(key));

        size_t truncate_after = order_by_nodes.size();

        for (size_t i = 0; i < order_by_nodes.size(); ++i)
        {
            forEachCoveredKey(
                order_by_nodes[i]->as<SortNode &>().getExpression(),
                group_by_keys,
                replaces_aggregates,
                [&](const auto & key) { not_covered_remaining_keys.erase(key); });

            if (not_covered_remaining_keys.empty())
            {
                truncate_after = i + 1;
                break;
            }
        }

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

        /// Remove the elements that are functions of the keys before them. Before all keys are covered, only if another pass replaces them,
        /// because then they cannot be calculated from the keys.
        /// Window functions are not supported over the results of an aggregation on shards.
        const bool has_unsupported_window_functions = getSettings()[Setting::distributed_group_by_no_merge] == 2
            && !hasFunctionNode(query->getProjectionNode(), "arrayJoin") && !hasFunctionNode(query->getOrderByNode(), "arrayJoin");
        std::unordered_map<QueryTreeNodePtr, QueryTreeNodePtrWithHashSet> columns_of_not_covered_keys;
        for (const auto & key : independent_keys)
            collectColumns(key, columns_of_not_covered_keys[group_by_keys.find(key)]);

        size_t size = 0;
        for (size_t i = 0; i < order_by_nodes.size(); ++i)
        {
            const auto & expression = order_by_nodes[i]->as<SortNode &>().getExpression();
            const bool is_covered = std::ranges::none_of(
                columns_of_not_covered_keys, [&](const auto & key) { return hasColumns(expression, key.second); });
            const bool is_redundant = is_covered && isFunctionOfKeys(expression, columns_of_merged_keys)
                && (i >= truncate_after || hasReplaceableWithArguments(expression));
            /// A part of it is already replaced, or it is not supported.
            const bool cannot_be_calculated
                = (readsOtherColumns(expression, group_by_keys)
                   && (i >= truncate_after || (is_covered && isFunctionOfKeys(expression, {}))))
                || (i >= truncate_after && has_unsupported_window_functions && hasWindowFunctionNodes(expression));

            forEachCoveredKey(
                expression, group_by_keys, replaces_aggregates, [&](const auto & key) { columns_of_not_covered_keys.erase(key); });

            if (!is_redundant && !cannot_be_calculated)
                order_by_nodes[size++] = order_by_nodes[i];
        }
        order_by_nodes.resize(size);
    }

private:
    /// The keys without those that have the same value as another one.
    class Keys
    {
    public:
        explicit Keys(const QueryTreeNodes & nodes)
        {
            for (const auto & node : nodes)
                if (!find(node))
                    equal_keys[node].push_back(node);
        }

        /// Returns the key with the same value, or nullptr.
        QueryTreeNodePtr find(const QueryTreeNodePtr & node) const
        {
            const auto it = equal_keys.find(node);
            if (it == equal_keys.end())
                return nullptr;

            const auto key = std::ranges::find_if(it->second, [&](const auto & candidate) { return haveSameValue(candidate, node); });
            return key == it->second.end() ? nullptr : *key;
        }

        /// Returns true if `node` is a key, or `UnifyGroupByKeysPass` replaces it with one.
        bool contains(const QueryTreeNodePtr & node) const
        {
            if (find(node))
                return true;

            const auto it = equal_keys.find(node);
            return it != equal_keys.end() && !hasNonDeterministicFunction(node)
                && std::ranges::count_if(it->second, [&](const auto & key) { return areConstantsEqual(key, node); }) == 1;
        }

    private:
        /// Equal keys, but with different values.
        QueryTreeNodePtrWithHashMap<QueryTreeNodes> equal_keys;
    };

    static void collectColumns(const QueryTreeNodePtr & node, QueryTreeNodePtrWithHashSet & columns)
    {
        checkStackSize();

        if (node->getNodeType() == QueryTreeNodeType::COLUMN)
            columns.insert(node);

        for (const auto & child : node->getChildren())
            if (child)
                collectColumns(child, columns);
    }

    static bool hasColumns(const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & columns)
    {
        checkStackSize();

        if (node->getNodeType() == QueryTreeNodeType::COLUMN && columns.contains(node))
            return true;

        return std::ranges::any_of(node->getChildren(), [&](const auto & child) { return child && hasColumns(child, columns); });
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

    /// Returns true if an expression reads columns that are not keys, so it cannot be calculated after the aggregation.
    static bool readsOtherColumns(const QueryTreeNodePtr & node, const Keys & keys)
    {
        checkStackSize();

        if (keys.contains(node))
            return false;

        switch (node->getNodeType())
        {
            case QueryTreeNodeType::COLUMN:
            {
                const auto source = node->as<ColumnNode &>().getColumnSourceOrNull();
                return !source || source->getNodeType() != QueryTreeNodeType::LAMBDA_ARGS;
            }
            case QueryTreeNodeType::QUERY:
            case QueryTreeNodeType::UNION:
            case QueryTreeNodeType::TABLE:
            case QueryTreeNodeType::TABLE_FUNCTION:
                return false;
            case QueryTreeNodeType::FUNCTION:
            {
                if (node->as<FunctionNode &>().isAggregateFunction())
                    return false;
                break;
            }
            default:
                break;
        }

        return std::ranges::any_of(node->getChildren(), [&](const auto & child) { return child && readsOtherColumns(child, keys); });
    }

    /// Finds the GROUP BY keys that an expression covers.
    /// An expression covers a key if it equals the key directly,
    /// or if it is an injective function whose arguments cover the key(s).
    template <typename Callback>
    static void forEachCoveredKey(
        const QueryTreeNodePtr & expr,
        const Keys & group_by_keys,
        bool replaces_aggregates,
        const Callback & callback)
    {
        if (auto key = group_by_keys.find(expr))
        {
            callback(key);
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
