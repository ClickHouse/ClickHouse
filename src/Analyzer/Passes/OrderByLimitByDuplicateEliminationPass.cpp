#include <Analyzer/Passes/OrderByLimitByDuplicateEliminationPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/HashUtils.h>

namespace DB
{

namespace
{

class OrderByLimitByDuplicateEliminationVisitor : public InDepthQueryTreeVisitor<OrderByLimitByDuplicateEliminationVisitor>
{
public:
    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        if (query_node->hasOrderBy())
        {
            QueryTreeNodeConstRawPtrWithHashSet unique_expressions_nodes_set;
            QueryTreeNodes result_nodes;

            auto & query_order_by_nodes = query_node->getOrderBy().getNodes();

            for (auto & sort_node : query_order_by_nodes)
            {
                auto & sort_node_typed = sort_node->as<SortNode &>();

                /// Skip elements with WITH FILL
                if (sort_node_typed.withFill())
                {
                    result_nodes.push_back(sort_node);
                    continue;
                }

                const auto * expression = sort_node_typed.getExpression().get();

                /// Different values can be equal with a collation, so the following keys are not redundant after it.
                const bool redundant = sort_node_typed.getCollator() ? unique_expressions_nodes_set.contains(expression)
                                                                     : !unique_expressions_nodes_set.emplace(expression).second;
                if (!redundant)
                    result_nodes.push_back(sort_node);
            }

            query_order_by_nodes = std::move(result_nodes);
        }

        if (query_node->hasLimitBy())
        {
            QueryTreeNodeConstRawPtrWithHashSet unique_expressions_nodes_set;
            QueryTreeNodes result_nodes;

            auto & query_limit_by_nodes = query_node->getLimitBy().getNodes();

            for (auto & limit_by_node : query_limit_by_nodes)
            {
                auto [_, inserted] = unique_expressions_nodes_set.emplace(limit_by_node.get());
                if (inserted)
                    result_nodes.push_back(limit_by_node);
            }

            query_limit_by_nodes = std::move(result_nodes);
        }
    }
};

}

void OrderByLimitByDuplicateEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr)
{
    OrderByLimitByDuplicateEliminationVisitor visitor;
    visitor.visit(query_tree_node);
}

}

