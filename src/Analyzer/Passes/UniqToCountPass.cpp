#include <Analyzer/Passes/UniqToCountPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_uniq_to_count;
}

namespace
{

bool matchFnUniq(String name)
{
    return name == "uniq" || name == "uniqHLL12" || name == "uniqExact" || name == "uniqTheta" || name == "uniqCombined"
        || name == "uniqCombined64";
}

/// Extract the corresponding projection columns for group by node list.
/// For example:
///     SELECT a as aa, any(b) FROM table group by a;  ->  aa(ColumnNode)
NamesAndTypes extractProjectionColumnsForGroupBy(const QueryNode * query_node)
{
    if (!query_node->hasGroupBy())
        return {};

    NamesAndTypes result;
    const auto & group_by_elements = query_node->getGroupByNode()->getChildren();
    for (const auto & group_by_element : group_by_elements)
    {
        const auto & projection_columns = query_node->getProjectionColumns();
        const auto & projection_nodes = query_node->getProjection().getNodes();

        assert(projection_columns.size() == projection_nodes.size());

        for (size_t i = 0; i < projection_columns.size(); i++)
        {
            if (projection_nodes[i]->isEqual(*group_by_element))
            {
                result.push_back(projection_columns[i]);
                break;
            }
        }
    }
    /// If some group by keys are not matched, we cannot apply optimization,
    /// because prefix of group by keys may not be unique.
    if (result.size() != group_by_elements.size())
        return {};

    return result;
}

/// Whether query_columns equals subquery_columns.
///     query_columns: query columns from query
///     subquery_columns: projection columns from subquery
bool nodeListEquals(const QueryTreeNodes & query_columns, const NamesAndTypes & subquery_columns)
{
    if (query_columns.size() != subquery_columns.size())
        return false;

    for (const auto & query_column : query_columns)
    {
        auto find = std::find_if(
            subquery_columns.begin(),
            subquery_columns.end(),
            [&](const auto & subquery_column) -> bool
            {
                if (auto * column_node = query_column->as<ColumnNode>())
                {
                    return subquery_column == column_node->getColumn();
                }
                return false;
            });

        if (find == subquery_columns.end())
            return false;
    }
    return true;
}

/// Whether subquery_columns contains all columns in subquery_columns.
///     query_columns: query columns from query
///     subquery_columns: projection columns from subquery
bool nodeListContainsAll(const QueryTreeNodes & query_columns, const NamesAndTypes & subquery_columns)
{
    if (query_columns.size() > subquery_columns.size())
        return false;

    for (const auto & query_column : query_columns)
    {
        auto find = std::find_if(
            subquery_columns.begin(),
            subquery_columns.end(),
            [&](const auto & subquery_column) -> bool
            {
                if (auto * column_node = query_column->as<ColumnNode>())
                {
                    return subquery_column == column_node->getColumn();
                }
                return false;
            });

        if (find == subquery_columns.end())
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

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_uniq_to_count])
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

            /// uniq expression list == subquery projection columns
            if (!nodeListEquals(uniq_arguments_nodes, subquery_node->getProjectionColumns()))
                return false;

            return true;
        };

        /// Whether query matches 'SELECT uniq(x ...) FROM (SELECT x ... GROUP BY x ...)'
        auto match_subquery_with_group_by = [&]() -> bool
        {
            if (!subquery_node->hasGroupBy())
                return false;

            /// uniq argument node list == subquery group by node list
            auto group_by_columns = extractProjectionColumnsForGroupBy(subquery_node);

            if (!nodeListEquals(uniq_arguments_nodes, group_by_columns))
                return false;

            /// subquery projection columns must contain all columns in uniq argument node list
            if (!nodeListContainsAll(uniq_arguments_nodes, subquery_node->getProjectionColumns()))
                return false;

            return true;
        };

        /// Replace uniq of initial query to count
        if (match_subquery_with_distinct() || match_subquery_with_group_by())
        {
            function_node->getArguments().getNodes().clear();
            resolveAggregateFunctionNodeByName(*function_node, "count");
        }
    }
};


void UniqToCountPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    UniqToCountVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
