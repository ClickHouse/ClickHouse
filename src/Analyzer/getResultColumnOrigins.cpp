#include <Analyzer/getResultColumnOrigins.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>


namespace DB
{

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
        if (!column_node)
            continue;

        auto column_source = column_node->getColumnSourceOrNull();
        const auto * table_node = column_source ? column_source->as<TableNode>() : nullptr;
        if (!table_node)
            continue;

        auto & origin = origins[i];
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

        /// The analyzer replaces table aliases with unique `__tableN` ones (see `createUniqueAliasesIfNecessary`),
        /// keeping the alias written in the query as the original one. So the alias is the user's only if it was replaced.
        if (table_node->getOriginalAlias() != table_node->getAlias())
            origin.table = table_node->getOriginalAlias();
        else
            origin.table = origin.org_table;

        origin.column = column_node->getColumnName();
    }

    return origins;
}

}
