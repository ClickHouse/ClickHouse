#include <Analyzer/Passes/RemoveUnusedProjectionColumnsPass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace
{

class CollectUsedColumnsVisitor : public InDepthQueryTreeVisitorWithContext<CollectUsedColumnsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CollectUsedColumnsVisitor>;
    using Base::Base;

    bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        if (isQueryOrUnionNode(child))
        {
            subqueries_nodes_to_visit.insert(child);
            return false;
        }

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto node_type = node->getNodeType();

        if (node_type == QueryTreeNodeType::QUERY)
        {
            auto & query_node = node->as<QueryNode &>();
            auto table_expressions = extractTableExpressions(query_node.getJoinTree());
            for (const auto & table_expression : table_expressions)
                if (isQueryOrUnionNode(table_expression))
                    query_or_union_node_to_used_columns.emplace(table_expression, std::unordered_set<std::string>());

            return;
        }

        if (node_type != QueryTreeNodeType::COLUMN)
            return;

        auto & column_node = node->as<ColumnNode &>();
        if (column_node.getColumnName() == "__grouping_set")
            return;

        auto column_source_node = column_node.getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::QUERY || column_source_node_type == QueryTreeNodeType::UNION)
            query_or_union_node_to_used_columns[column_source_node].insert(column_node.getColumnName());
    }

    void reset()
    {
        subqueries_nodes_to_visit.clear();
        query_or_union_node_to_used_columns.clear();
    }

    std::unordered_set<QueryTreeNodePtr> subqueries_nodes_to_visit;
    std::unordered_map<QueryTreeNodePtr, std::unordered_set<std::string>> query_or_union_node_to_used_columns;
};

std::unordered_set<size_t> convertUsedColumnNamesToUsedProjectionIndexes(const QueryTreeNodePtr & query_or_union_node, const std::unordered_set<std::string> & used_column_names)
{
    std::unordered_set<size_t> result;

    auto * union_node = query_or_union_node->as<UnionNode>();
    auto * query_node = query_or_union_node->as<QueryNode>();

    const auto & projection_columns = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
    size_t projection_columns_size = projection_columns.size();

    for (size_t i = 0; i < projection_columns_size; ++i)
    {
        const auto & projection_column = projection_columns[i];
        if (used_column_names.contains(projection_column.name))
            result.insert(i);
    }

    return result;
}

/// We cannot remove aggregate functions, if query does not contain GROUP BY or arrayJoin from subquery projection
void updateUsedProjectionIndexes(const QueryTreeNodePtr & query_or_union_node, std::unordered_set<size_t> & used_projection_columns_indexes)
{
    if (auto * union_node = query_or_union_node->as<UnionNode>())
    {
        auto union_node_mode = union_node->getUnionMode();
        bool is_distinct = union_node_mode == SelectUnionMode::UNION_DISTINCT ||
            union_node_mode == SelectUnionMode::INTERSECT_DISTINCT ||
            union_node_mode == SelectUnionMode::EXCEPT_DISTINCT;

        if (is_distinct)
        {
            auto union_projection_columns = union_node->computeProjectionColumns();
            size_t union_projection_columns_size = union_projection_columns.size();

            for (size_t i = 0; i < union_projection_columns_size; ++i)
                used_projection_columns_indexes.insert(i);

            return;
        }

        for (auto & query_node : union_node->getQueries().getNodes())
            updateUsedProjectionIndexes(query_node, used_projection_columns_indexes);
        return;
    }

    const auto & query_node = query_or_union_node->as<const QueryNode &>();
    const auto & projection_nodes = query_node.getProjection().getNodes();
    size_t projection_nodes_size = projection_nodes.size();

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        const auto & projection_node = projection_nodes[i];
        if ((!query_node.hasGroupBy() && hasAggregateFunctionNodes(projection_node)) || hasFunctionNode(projection_node, "arrayJoin"))
            used_projection_columns_indexes.insert(i);
    }
}

}

void RemoveUnusedProjectionColumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    std::vector<QueryTreeNodePtr> nodes_to_visit;
    nodes_to_visit.push_back(query_tree_node);

    CollectUsedColumnsVisitor visitor(std::move(context));

    while (!nodes_to_visit.empty())
    {
        auto node_to_visit = std::move(nodes_to_visit.back());
        nodes_to_visit.pop_back();

        visitor.visit(node_to_visit);

        for (auto & [query_or_union_node, used_columns] : visitor.query_or_union_node_to_used_columns)
        {
            /// can't remove columns from distinct, see example - 03023_remove_unused_column_distinct.sql
            if (auto * query_node = query_or_union_node->as<QueryNode>())
            {
                if (query_node->isDistinct())
                    continue;
            }

            auto used_projection_indexes = convertUsedColumnNamesToUsedProjectionIndexes(query_or_union_node, used_columns);
            updateUsedProjectionIndexes(query_or_union_node, used_projection_indexes);

            /// Keep at least 1 column if used projection columns are empty
            if (used_projection_indexes.empty())
                used_projection_indexes.insert(0);

            if (auto * union_node = query_or_union_node->as<UnionNode>())
                union_node->removeUnusedProjectionColumns(used_projection_indexes);
            else if (auto * query_node = query_or_union_node->as<QueryNode>())
                query_node->removeUnusedProjectionColumns(used_projection_indexes);
        }

        for (const auto & subquery_node_to_visit : visitor.subqueries_nodes_to_visit)
            nodes_to_visit.push_back(subquery_node_to_visit);

        visitor.reset();
    }
}

}
