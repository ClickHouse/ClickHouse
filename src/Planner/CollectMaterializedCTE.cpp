#include <unordered_set>
#include <Planner/CollectMaterializedCTE.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/traverseQueryTree.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

TableHolderToCTEMap collectMaterializedCTEs(const QueryTreeNodePtr & node)
{
    TableHolderToCTEMap materialized_ctes;

    traverseQueryTree(node, ExceptSubqueries{},
    [&](const QueryTreeNodePtr & current_node)
    {
        if (auto * table_node = current_node->as<TableNode>())
        {
            if (table_node->isMaterializedCTE())
            {
                LOG_DEBUG(getLogger("collectMaterializedCTEs"), "Found materialized CTE:\n{}", table_node->dumpTree());
                materialized_ctes.emplace(table_node->getTemporaryTableHolder().get(), current_node);
            }
        }
    });

    return materialized_ctes;
}

}
