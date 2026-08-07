#include <Analyzer/Passes/OrderByTupleEliminationPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class OrderByTupleEliminationVisitor : public InDepthQueryTreeVisitor<OrderByTupleEliminationVisitor>
{
public:
    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node || !query_node->hasOrderBy())
            return;

        QueryTreeNodes result_nodes;

        for (auto & sort_node : query_node->getOrderBy().getNodes())
        {
            auto & sort_node_typed = sort_node->as<SortNode &>();
            auto * function_expression = sort_node_typed.getExpression()->as<FunctionNode>();
            if (sort_node_typed.withFill() || !function_expression || function_expression->getFunctionName() != "tuple")
            {
                result_nodes.push_back(sort_node);
                continue;
            }

            auto & tuple_arguments_nodes = function_expression->getArguments().getNodes();
            for (auto & argument_node : tuple_arguments_nodes)
            {
                auto result_sort_node = std::make_shared<SortNode>(argument_node,
                    sort_node_typed.getSortDirection(),
                    sort_node_typed.getNullsSortDirection(),
                    sort_node_typed.getCollator());
                result_nodes.push_back(std::move(result_sort_node));
            }
        }

        query_node->getOrderBy().getNodes() = std::move(result_nodes);
    }
};

}

void OrderByTupleEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr)
{
    OrderByTupleEliminationVisitor visitor;
    visitor.visit(query_tree_node);
}

}
