#include <Analyzer/getResultColumnOrigins.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>


namespace DB
{

namespace
{

/// The alias of a table expression as written in the query, or an empty string if it has none.
/// The analyzer replaces table aliases with unique `__tableN` ones (see `createUniqueAliasesIfNecessary`),
/// keeping the alias written in the query as the original one. So the alias is the user's only if it was replaced.
String getUserAlias(const IQueryTreeNode & table_expression)
{
    if (table_expression.getOriginalAlias() != table_expression.getAlias())
        return table_expression.getOriginalAlias();
    return {};
}

/// Fills the origin of a column and returns true if it is a plain reference to a table column,
/// directly or through the projections of subqueries, as in `SELECT id FROM (SELECT id FROM t)`.
/// This also covers the subquery that wraps the query to apply the `limit`, `offset`, `filter`, `order`
/// and `select` settings (see `applyQueryConstructionSettings`).
bool fillColumnOrigin(const ColumnNode & column_node, ResultColumnOrigin & origin)
{
    auto column_source = column_node.getColumnSourceOrNull();
    if (!column_source)
        return false;

    if (const auto * table_node = column_source->as<TableNode>())
    {
        if (table_node->isTemporaryTable())
        {
            origin.org_table = table_node->getTemporaryTableName();
        }
        else
        {
            const auto & storage_id = table_node->getStorageID();
            origin.database = storage_id.getDatabaseName();
            origin.org_table = storage_id.getTableName();
        }

        origin.table = getUserAlias(*table_node);
        if (origin.table.empty())
            origin.table = origin.org_table;

        origin.column = column_node.getColumnName();
        return true;
    }

    if (const auto * subquery_node = column_source->as<QueryNode>())
    {
        const auto & projection_columns = subquery_node->getProjectionColumns();
        const auto & projection_nodes = subquery_node->getProjection().getNodes();
        if (projection_columns.size() != projection_nodes.size())
            return false;

        for (size_t i = 0; i < projection_columns.size(); ++i)
        {
            if (projection_columns[i].name != column_node.getColumnName())
                continue;

            const auto * inner_column_node = projection_nodes[i]->as<ColumnNode>();
            if (!inner_column_node || !fillColumnOrigin(*inner_column_node, origin))
                return false;

            /// As in MySQL, the table of a column of a derived table is the alias of the derived table.
            if (String alias = getUserAlias(*subquery_node); !alias.empty())
                origin.table = std::move(alias);
            return true;
        }
    }

    return false;
}

}

ResultColumnOrigins getResultColumnOrigins(const QueryTreeNodePtr & query_tree)
{
    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        return {};

    const auto & projection_nodes = query_node->getProjection().getNodes();

    ResultColumnOrigins origins;
    origins.resize(projection_nodes.size());

    for (size_t i = 0; i < projection_nodes.size(); ++i)
    {
        const auto * column_node = projection_nodes[i]->as<ColumnNode>();
        if (column_node && !fillColumnOrigin(*column_node, origins[i]))
            origins[i] = {};
    }

    return origins;
}

}
