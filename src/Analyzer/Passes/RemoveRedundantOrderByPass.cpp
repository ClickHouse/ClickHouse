#include <Analyzer/Passes/RemoveRedundantOrderByPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace
{

/// Checks if SELECT has stateful functions
class FunctionStatefulVisitor : public InDepthQueryTreeVisitor<FunctionStatefulVisitor, true>
{
public:
    bool is_stateful = false;

    bool needChildVisit(VisitQueryTreeNodeType &, VisitQueryTreeNodeType &) const { return !is_stateful; }

    void visitImpl(VisitQueryTreeNodeType & node)
    {
        if (!node)
            return;

        if (node->getNodeType() != QueryTreeNodeType::FUNCTION)
            return;

        const auto * function_node = node->as<FunctionNode>();

        auto aggregate_function_properties = AggregateFunctionFactory::instance().tryGetProperties(function_node->getFunctionName());
        if (aggregate_function_properties && aggregate_function_properties->is_order_dependent)
        {
            is_stateful = true;
            return;
        }

        if (function_node->isOrdinaryFunction() && function_node->getFunction()->isStateful())
        {
            is_stateful = true;
            return;
        }
    }
};

class RemoveRedundantOrderByClausesVisitor : public InDepthQueryTreeVisitor<RemoveRedundantOrderByClausesVisitor>
{
    bool try_drop_order_by_in_subquery = false;
    std::vector<QueryTreeNodePtr> parent_queries_breaking_order;

    static void tryEliminateOrderBy(QueryNode * query_node)
    {
        if (!query_node->hasOrderBy())
            return;

        /// If we have limits then the ORDER BY is non-removable
        if (query_node->hasLimitBy() || query_node->hasLimitByLimit() || query_node->hasLimitByOffset())
            return;

        /// If ORDER BY contains filling (in addition to sorting) it is non-removable.
        auto & order_by_nodes = query_node->getOrderBy().getNodes();
        for (const auto & sort_node : order_by_nodes)
        {
            const auto & sort_node_typed = sort_node->as<const SortNode &>();
            if (sort_node_typed.withFill())
                return;
        }

        query_node->getOrderByNode()->getChildren().clear();
    }

public:
    bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType & child [[maybe_unused]])
    {
        auto * query_node = parent->as<QueryNode>();
        if (query_node)
        {
            if (parent_queries_breaking_order.empty() || parent != parent_queries_breaking_order.back())
            {
                if (query_node->hasOrderBy() || query_node->hasGroupBy())
                    parent_queries_breaking_order.push_back(parent);
            }
        }

        return true;
    }

    void allChildrenVisited(VisitQueryTreeNodeType & parent) {
        auto * query_node = parent->as<QueryNode>();
        if (query_node)
        {
            if (!parent_queries_breaking_order.empty() && parent == parent_queries_breaking_order.back())
            {
                parent_queries_breaking_order.pop_back();
                if (parent_queries_breaking_order.empty())
                    try_drop_order_by_in_subquery = false;
            }
        }
    }

    void visitImpl(VisitQueryTreeNodeType & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        /// drop ORDER BY in current subquery if necessary
        if (query_node->isSubquery() && try_drop_order_by_in_subquery)
            tryEliminateOrderBy(query_node);

        /// check if we can eliminate ORDER BY in subquery
        const auto & projection_nodes = query_node->getProjection().getNodes();
        for (const auto & elem : projection_nodes)
        {
            FunctionStatefulVisitor function_visitor;
            function_visitor.visit(elem);
            if (function_visitor.is_stateful)
            {
                try_drop_order_by_in_subquery = false;
                return;
            }
        }

        if (parent_queries_breaking_order.empty())
            try_drop_order_by_in_subquery = query_node->hasOrderBy() || query_node->hasGroupBy();
        else
            try_drop_order_by_in_subquery = true;
    }
};

}

void RemoveRedundantOrderByClausesPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    RemoveRedundantOrderByClausesVisitor visitor;
    visitor.visit(query_tree_node);
}

}
