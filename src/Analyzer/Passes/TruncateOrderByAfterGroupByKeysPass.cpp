#include <Analyzer/Passes/TruncateOrderByAfterGroupByKeysPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/Utils.h>
#include <Common/checkStackSize.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <Interpreters/Context.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 distributed_group_by_no_merge;
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

        /// The `arrayJoin` function in the projection or in ORDER BY can multiply rows
        /// after GROUP BY, so a single GROUP BY key value may correspond to multiple
        /// output rows. Truncating ORDER BY elements after covering all GROUP BY keys
        /// would lose tiebreakers among those expanded rows, producing wrong output order.
        if (hasFunctionNode(query->getProjectionNode(), "arrayJoin") || hasFunctionNode(query->getOrderByNode(), "arrayJoin"))
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
        auto & group_by_nodes = query->getGroupBy().getNodes();
        QueryTreeNodePtrWithHashSet group_by_keys;
        for (auto & key : group_by_nodes)
            group_by_keys.insert(QueryTreeNodePtrWithHash(key));

        if (group_by_keys.empty())
            return;

        /// Scan ORDER BY elements and track which GROUP BY keys have been covered.
        QueryTreeNodePtrWithHashSet covered_keys;
        size_t truncate_after = order_by_nodes.size();

        for (size_t i = 0; i < order_by_nodes.size(); ++i)
        {
            auto & order_expr = order_by_nodes[i]->as<SortNode>()->getExpression();
            collectCoveredKeys(order_expr, group_by_keys, covered_keys);

            if (covered_keys.size() == group_by_keys.size())
            {
                truncate_after = i + 1;
                break;
            }
        }

        if (truncate_after == order_by_nodes.size())
            return;

        /// Different groups can be equal in the comparison: 0 and -0.
        bool has_merged_keys = false;
        QueryTreeNodePtrWithHashSet columns_of_merged_keys;
        for (const auto & key : group_by_nodes)
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
        {
            order_by_nodes.resize(truncate_after);
            return;
        }

        /// Remove only the elements that are functions of the keys, and those that cannot be calculated: a part of it is already replaced
        /// by another pass, or it has a correlated subquery.
        const auto is_removed = [&](const QueryTreeNodePtr & sort_elem)
        {
            const auto & expression = sort_elem->as<SortNode &>().getExpression();
            return isFunctionOfKeys(expression, columns_of_merged_keys) || !canBeCalculatedFromKeys(expression, group_by_nodes)
                || containsCorrelatedSubquery(expression);
        };
        order_by_nodes.erase(
            std::remove_if(order_by_nodes.begin() + truncate_after, order_by_nodes.end(), is_removed), order_by_nodes.end());

        /// A positional argument keeps the aliases that the projection loses, so a remote server can find different definitions of them.
        for (size_t i = truncate_after; i < order_by_nodes.size(); ++i)
            removeAliasesRecursive(order_by_nodes[i]->as<SortNode &>().getExpression());
    }

private:
    /// `arrayJoin` in the projection and in ORDER BY is checked above.
    [[nodiscard]] bool canHaveSeveralRowsPerGroup(const QueryNode & query) const
    {
        /// The groups of different shards or replicas are not merged.
        if (getSettings()[Setting::distributed_group_by_no_merge] || getContext()->canUseParallelReplicasCustomKey())
            return true;

        return (query.hasHaving() && hasFunctionNode(query.getHaving(), "arrayJoin"))
            || (query.hasQualify() && hasFunctionNode(query.getQualify(), "arrayJoin"))
            || hasFunctionNode(query.getLimitByNode(), "arrayJoin");
    }

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

    /// Check if an expression covers any GROUP BY keys.
    /// An expression covers a key if it equals the key directly,
    /// or if it is an injective function whose arguments cover the key(s).
    static void collectCoveredKeys(
        const QueryTreeNodePtr & expr,
        const QueryTreeNodePtrWithHashSet & group_by_keys,
        QueryTreeNodePtrWithHashSet & covered_keys)
    {
        if (group_by_keys.contains(QueryTreeNodePtrWithHash(expr)))
        {
            covered_keys.insert(QueryTreeNodePtrWithHash(expr));
            return;
        }

        auto * function = expr->as<FunctionNode>();
        if (!function)
            return;

        /// Only ordinary (non-aggregate, non-window) functions can be injective.
        auto function_base = function->getFunction();
        if (!function_base)
            return;

        if (!function_base->isInjective(function->getArgumentColumns()))
            return;

        /// The function is injective — its arguments uniquely determine its value,
        /// so any GROUP BY keys covered by the arguments are also covered by this expression.
        for (const auto & arg : function->getArguments().getNodes())
            collectCoveredKeys(arg, group_by_keys, covered_keys);
    }
};

}

void TruncateOrderByAfterGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    TruncateOrderByAfterGroupByKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
