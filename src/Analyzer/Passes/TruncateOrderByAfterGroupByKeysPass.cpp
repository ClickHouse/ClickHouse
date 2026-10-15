#include <Analyzer/Passes/TruncateOrderByAfterGroupByKeysPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
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

        if (truncate_after < order_by_nodes.size())
            order_by_nodes.resize(truncate_after);
    }

private:
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
