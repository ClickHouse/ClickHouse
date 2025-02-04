#include <Analyzer/Passes/CountDistinctPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool count_distinct_optimization;
}

namespace
{

class CountDistinctVisitor : public InDepthQueryTreeVisitorWithContext<CountDistinctVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CountDistinctVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::count_distinct_optimization])
            return;

        auto * query_node = node->as<QueryNode>();

        /// Check that query has only SELECT clause
        if (!query_node || (query_node->hasWith() || query_node->hasPrewhere() || query_node->hasWhere() || query_node->hasGroupBy() ||
            query_node->hasHaving() || query_node->hasWindow() || query_node->hasOrderBy() || query_node->hasLimitByLimit() || query_node->hasLimitByOffset() ||
            query_node->hasLimitBy() || query_node->hasLimit() || query_node->hasOffset()))
            return;

        /// Check that query has only single table expression
        auto join_tree_node_type = query_node->getJoinTree()->getNodeType();
        if (join_tree_node_type == QueryTreeNodeType::JOIN || join_tree_node_type == QueryTreeNodeType::ARRAY_JOIN)
            return;

        /// Check that query has only single node in projection
        auto & projection_nodes = query_node->getProjection().getNodes();
        if (projection_nodes.size() != 1)
            return;

        /// Check that query single projection node is `countDistinct` function
        auto & projection_node = projection_nodes[0];
        auto * function_node = projection_node->as<FunctionNode>();
        if (!function_node)
            return;

        auto lower_function_name = Poco::toLower(function_node->getFunctionName());
        if (lower_function_name != "countdistinct" && lower_function_name != "uniqexact")
            return;

        /// Check that `countDistinct` function has single COLUMN argument
        auto & count_distinct_arguments_nodes = function_node->getArguments().getNodes();
        if (count_distinct_arguments_nodes.size() != 1 && count_distinct_arguments_nodes[0]->getNodeType() != QueryTreeNodeType::COLUMN)
            return;

        auto & count_distinct_argument_column = count_distinct_arguments_nodes[0];
        if (count_distinct_argument_column->getNodeType() != QueryTreeNodeType::COLUMN)
            return;
        auto & count_distinct_argument_column_typed = count_distinct_argument_column->as<ColumnNode &>();

        /// Build subquery SELECT count_distinct_argument_column FROM table_expression GROUP BY count_distinct_argument_column
        auto subquery = std::make_shared<QueryNode>(Context::createCopy(query_node->getContext()));
        subquery->getJoinTree() = query_node->getJoinTree();
        subquery->getProjection().getNodes().push_back(count_distinct_argument_column);
        subquery->getGroupBy().getNodes().push_back(count_distinct_argument_column);
        subquery->resolveProjectionColumns({count_distinct_argument_column_typed.getColumn()});

        /// Put subquery into JOIN TREE of initial query
        query_node->getJoinTree() = std::move(subquery);

        /// Replace `countDistinct` of initial query into `count`
        auto result_type = function_node->getResultType();

        function_node->getArguments().getNodes().clear();
        resolveAggregateFunctionNodeByName(*function_node, "count");
    }
};

}

void CountDistinctPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    CountDistinctVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
