#include "UniqToCountPass.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>

namespace DB
{

namespace
{

bool matchFnUniq(String func_name)
{
    auto name = Poco::toLower(func_name);
    return name == "uniq" || name == "uniqHLL12" || name == "uniqExact" || name == "uniqTheta" || name == "uniqCombined"
        || name == "uniqCombined64";
}

bool nodeEquals(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    auto * lhs_node = lhs->as<ColumnNode>();
    auto * rhs_node = rhs->as<ColumnNode>();

    if (lhs_node && rhs_node && lhs_node->getColumn() == rhs_node->getColumn())
        return true;
    return false;
}

bool nodeListEquals(const QueryTreeNodes & lhs, const QueryTreeNodes & rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); i++)
    {
        if (!nodeEquals(lhs[i], rhs[i]))
            return false;
    }
    return true;
}

bool nodeListContainsAll(const QueryTreeNodes & lhs, const QueryTreeNodes & rhs)
{
    if (lhs.size() < rhs.size())
        return false;
    for (const auto & re : rhs)
    {
        auto predicate = [&](const QueryTreeNodePtr & le) { return nodeEquals(le, re); };
        if (std::find_if(lhs.begin(), lhs.end(), predicate) == lhs.end())
            return false;
    }
    return true;
}

}

class UniqToCountVisitor : public InDepthQueryTreeVisitorWithContext<UniqToCountVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<UniqToCountVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_uniq_to_count)
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        /// Check that query has only single table expression which is subquery
        auto * subquery_node = query_node->getJoinTree()->as<QueryNode>();
        if (!subquery_node)
            return;

        /// Check that query has only single node in projection
        auto & projection_nodes = query_node->getProjection().getNodes();
        if (projection_nodes.size() != 1)
            return;

        /// Check that projection_node is a function
        auto & projection_node = projection_nodes[0];
        auto * function_node = projection_node->as<FunctionNode>();
        if (!function_node)
            return;

        /// Check that query single projection node is `uniq` or its variants
        if (!matchFnUniq(function_node->getFunctionName()))
            return;

        auto & uniq_arguments_nodes = function_node->getArguments().getNodes();

        /// Whether query matches 'SELECT uniq(x ...) FROM (SELECT DISTINCT x ...)'
        auto match_subquery_with_distinct = [&]() -> bool
        {
            if (!subquery_node->isDistinct())
                return false;
            /// uniq expression list == subquery group by expression list
            if (!nodeListEquals(uniq_arguments_nodes, subquery_node->getProjection().getNodes()))
                return false;
            return true;
        };

        /// Whether query matches 'SELECT uniq(x ...) FROM (SELECT x ... GROUP BY x ...)'
        auto match_subquery_with_group_by = [&]() -> bool
        {
            if (!subquery_node->hasGroupBy())
                return false;
            /// uniq argument node list == subquery group by node list
            if (!nodeListEquals(uniq_arguments_nodes, subquery_node->getGroupByNode()->getChildren()))
                return false;
            /// subquery select node list must contain all columns in uniq argument node list
            if (!nodeListContainsAll(subquery_node->getProjection().getNodes(), uniq_arguments_nodes))
                return false;
            return true;
        };

        /// Replace uniq of initial query to count
        if (match_subquery_with_distinct() || match_subquery_with_group_by())
        {
            AggregateFunctionProperties properties;
            auto aggregate_function = AggregateFunctionFactory::instance().get("count", {}, {}, properties);
            function_node->resolveAsAggregateFunction(std::move(aggregate_function));
            function_node->getArguments().getNodes().clear();
            query_node->resolveProjectionColumns({{"count()", function_node->getResultType()}});
        }
    }
};


void UniqToCountPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    UniqToCountVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
