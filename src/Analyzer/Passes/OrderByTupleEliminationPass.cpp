#include <Analyzer/Passes/OrderByTupleEliminationPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
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

        const auto group_by_keys = getGroupByKeys(*query_node);

        QueryTreeNodes result_nodes;

        for (auto & sort_node : query_node->getOrderBy().getNodes())
        {
            auto & sort_node_typed = sort_node->as<SortNode &>();
            auto * function_expression = sort_node_typed.getExpression()->as<FunctionNode>();
            /// Splitting a key would read its arguments, which do not exist after aggregation.
            if (sort_node_typed.withFill() || !function_expression || function_expression->getFunctionName() != "tuple"
                || isGroupByKeyAfterAggregation(sort_node_typed.getExpression(), group_by_keys))
            {
                result_nodes.push_back(sort_node);
                continue;
            }

            auto & tuple_arguments_nodes = function_expression->getArguments().getNodes();
            const auto & arguments_indexes = sort_node_typed.getArgumentsProjectionColumnIndexes();

            /// A tuple selected as a whole stays a sort key too, so that rows added by WITH FILL have its value.
            bool keep_tuple = !sort_node_typed.getProjectionColumnIndexes().empty();
            if (keep_tuple)
                result_nodes.push_back(sort_node);

            for (size_t i = 0; i < tuple_arguments_nodes.size(); ++i)
            {
                auto result_sort_node = std::make_shared<SortNode>(keep_tuple ? tuple_arguments_nodes[i]->clone() : tuple_arguments_nodes[i],
                    sort_node_typed.getSortDirection(),
                    sort_node_typed.getNullsSortDirection(),
                    sort_node_typed.getCollator());
                if (i < arguments_indexes.size())
                    result_sort_node->setProjectionColumnIndexes(arguments_indexes[i]);
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
