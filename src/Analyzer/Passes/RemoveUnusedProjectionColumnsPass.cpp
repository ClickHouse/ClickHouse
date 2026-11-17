#include <Analyzer/Passes/RemoveUnusedProjectionColumnsPass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <DataTypes/DataTypesNumber.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMerge.h>

#include <Analyzer/traverseQueryTree.h>

namespace DB
{

namespace
{

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

/// Whether the rows of a query over this join tree do not depend on which columns it reads.
/// `ARRAY JOIN` of a `Nested` column joins only the subcolumns that are read, which may have different sizes,
/// and a view is read like an `ARRAY JOIN` subquery would be, so the projection must name the columns to read.
bool rowsDoNotDependOnReadColumns(const TableExpressionNodePtr & join_tree_node)
{
    for (const auto & table_expression : extractTableExpressions(join_tree_node, /*add_array_join=*/ true))
    {
        switch (table_expression->getNodeType())
        {
            case QueryTreeNodeType::ARRAY_JOIN:
                return false;
            case QueryTreeNodeType::TABLE:
                if (rowsMayDependOnReadColumns(table_expression->as<TableNode &>().getStorage()))
                    return false;
                break;
            case QueryTreeNodeType::TABLE_FUNCTION:
                if (rowsMayDependOnReadColumns(table_expression->as<TableFunctionNode &>().getStorage()))
                    return false;
                break;
            case QueryTreeNodeType::QUERY:
                if (!rowsDoNotDependOnReadColumns(table_expression->as<QueryNode &>().getJoinTreeNodeTyped()))
                    return false;
                break;
            case QueryTreeNodeType::UNION:
                for (const auto & query : table_expression->as<UnionNode &>().getQueries().getNodes())
                    if (!rowsDoNotDependOnReadColumns(static_pointer_cast<ITableExpressionNode>(query)))
                        return false;
                break;
            default:
                return false;
        }
    }
    return true;
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

    /// If the query uses DISTINCT, all of its projection columns are
    /// significant — `DISTINCT` deduplicates over the full row of selected
    /// columns, so removing any of them changes the result.
    /// The pass-level guard in `run` skips DISTINCT queries that are direct
    /// FROM-children, but a DISTINCT query reached through a `UNION ALL`
    /// is processed here (via the recursion above) and would otherwise
    /// have its columns silently pruned.
    if (query_node.isDistinct())
    {
        for (size_t i = 0; i < projection_nodes_size; ++i)
            used_projection_columns_indexes.insert(i);
        return;
    }

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        const auto & projection_node = projection_nodes[i];
        if ((!query_node.hasGroupBy() && hasAggregateFunctionNodes(projection_node)) || hasFunctionNode(projection_node, "arrayJoin"))
            used_projection_columns_indexes.insert(i);
    }
}

}

void RemoveUnusedProjectionColumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /*context*/)
{
    QueryTreeNodes nodes_to_visit = { query_tree_node };

    while (!nodes_to_visit.empty())
    {
        auto node_to_visit = std::move(nodes_to_visit.back());
        nodes_to_visit.pop_back();

        std::unordered_set<QueryTreeNodePtr> subqueries_nodes_to_visit;
        std::unordered_map<QueryTreeNodePtr, std::unordered_set<std::string>> node_to_used_columns;

        /// Initialize map with query and union nodes in the FROM clause
        if (auto * query_node = node_to_visit->as<QueryNode>())
        {
            for (const auto & table_expression : extractTableExpressions(query_node->getJoinTreeNodeTyped()))
                if (isQueryOrUnionNode(table_expression))
                    node_to_used_columns.emplace(table_expression, std::unordered_set<std::string>());
        }

        /// Collect information about what columns are used in the query.
        traverseQueryTree(node_to_visit,
            [&subqueries_nodes_to_visit, &node_to_used_columns](
                const QueryTreeNodePtr & /*parent*/,
                const QueryTreeNodePtr & child
            )
            {
                if (isQueryOrUnionNode(child))
                {
                    subqueries_nodes_to_visit.insert(child);

                    auto * query_node = child->as<QueryNode>();
                    auto * union_node = child->as<UnionNode>();

                    const auto & correlated_columns = query_node != nullptr ? query_node->getCorrelatedColumns() : union_node->getCorrelatedColumns();
                    for (const auto & correlated_column : correlated_columns)
                    {
                        auto * column_node = correlated_column->as<ColumnNode>();
                        auto column_source_node = column_node->getColumnSource();
                        auto column_source_node_type = column_source_node->getNodeType();
                        if (column_source_node_type == QueryTreeNodeType::QUERY || column_source_node_type == QueryTreeNodeType::UNION)
                        {
                            if (auto it = node_to_used_columns.find(column_source_node); it != node_to_used_columns.end())
                                it->second.insert(column_node->getColumnName());
                        }
                    }
                    return false;
                }
                return true;
            },
            [&node_to_used_columns](const QueryTreeNodePtr & node)
            {
                const auto node_type = node->getNodeType();
                if (node_type != QueryTreeNodeType::COLUMN)
                    return;

                auto & column_node = node->as<ColumnNode &>();
                if (column_node.getColumnName() == "__grouping_set")
                    return;

                auto column_source_node = column_node.getColumnSource();

                auto it = node_to_used_columns.find(column_source_node);
                /// If the source node is not found in the map then:
                /// 1. Tt's either not a Query or Union node.
                /// 2. It's a correlated column and it comes from the outer scope.
                if (it != node_to_used_columns.end())
                {
                    it->second.insert(column_node.getColumnName());
                }
            });

        /// Pass information about used columns to subqueries and remove unused projection columns
        for (auto & [query_or_union_node, used_columns] : node_to_used_columns)
        {
            /// can't remove columns from distinct, see example - 03023_remove_unused_column_distinct.sql
            if (auto * query_node = query_or_union_node->as<QueryNode>())
            {
                if (query_node->isDistinct())
                    continue;
            }

            auto used_projection_indexes = convertUsedColumnNamesToUsedProjectionIndexes(query_or_union_node, used_columns);
            updateUsedProjectionIndexes(query_or_union_node, used_projection_indexes);

            /// No projection column is used and none of them changes the number of rows: the query is read only
            /// to count its rows, so it does not need to compute any of them, and the planner reads from its
            /// tables only the columns that count the rows. With `ARRAY JOIN` the projection can change the
            /// number of rows, see `rowsDoNotDependOnReadColumns`.
            if (used_projection_indexes.empty())
            {
                auto * query_node = query_or_union_node->as<QueryNode>();
                if (query_node && !query_node->hasInterpolate() && query_node->getProjectionAliasesToOverride().empty()
                    && rowsDoNotDependOnReadColumns(query_node->getJoinTreeNodeTyped()))
                {
                    NameAndTypePair projection_column{query_node->getProjectionColumns().front().name, std::make_shared<DataTypeUInt8>()};
                    query_node->removeUnusedProjectionColumns({0});
                    query_node->getProjection().getNodes().front() = std::make_shared<ConstantNode>(UInt64(0), projection_column.type);
                    query_node->resolveProjectionColumns({std::move(projection_column)});
                    continue;
                }

                /// Keep at least 1 column.
                used_projection_indexes.insert(0);
            }

            if (auto * union_node = query_or_union_node->as<UnionNode>())
                union_node->removeUnusedProjectionColumns(used_projection_indexes);
            else if (auto * query_node = query_or_union_node->as<QueryNode>())
                query_node->removeUnusedProjectionColumns(used_projection_indexes);
        }

        for (const auto & subquery_node_to_visit : subqueries_nodes_to_visit)
            nodes_to_visit.push_back(subquery_node_to_visit);
    }
}

}
