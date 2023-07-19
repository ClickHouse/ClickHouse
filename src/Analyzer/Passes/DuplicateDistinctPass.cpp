#include "DuplicateDistinctPass.h"

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>

namespace DB
{

namespace
{

/// Check whether query_node and subquery_node projection columns matches.
bool projectionMatches(const QueryNode * query_node, const QueryNode * subquery_node)
{
    auto query_columns = query_node->getProjection().getNodes();
    auto subquery_columns = subquery_node->getProjectionColumns();

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
};

class DuplicateDistinctVisitor : public InDepthQueryTreeVisitor<DuplicateDistinctVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<DuplicateDistinctVisitor>;
    using Base::Base;

    static bool shouldTraverseTopToBottom()
    {
        return false;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto * subquery_node = query_node->getJoinTree()->as<QueryNode>();
        if (!subquery_node)
        {
            subquery_is_distinct = query_node->isDistinct();
            return;
        }

        if (!subquery_is_distinct)
        {
            subquery_is_distinct = query_node->isDistinct();
            return;
        }

        /// Check whether query_node and subquery_node projection columns matches.
        auto match = projectionMatches(query_node, subquery_node);

        if (query_node->isDistinct() && match)
        {
            /// we can remove distinct
            query_node->setIsDistinct(false);
        }

        subquery_is_distinct = query_node->isDistinct() || match;
    }

private:
    /// Identify whether subquery provide distinct result.
    ///     1. true if subquery has distinct modifier
    ///     2. true if subquery has no distinct modifier, but its subquery can provide distinct result
    ///        and column list can match. Example(subquery): SELECT a FROM (SELECT DISTINCT a FROM t)
    ///     3. false
    bool subquery_is_distinct;
};

}

void DuplicateDistinctPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    if (!context->getSettings().optimize_duplicate_order_by_and_distinct)
        return;

    if (context->getSettings().distributed_group_by_no_merge)
        return;

    DuplicateDistinctVisitor visitor;
    visitor.visit(query_tree_node);
}

}
