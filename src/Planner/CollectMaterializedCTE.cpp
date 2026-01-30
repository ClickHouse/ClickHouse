#include <Planner/CollectMaterializedCTE.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/traverseQueryTree.h>

namespace DB
{

QueryTreeNodes collectMaterializedCTEs(const QueryTreeNodePtr & node)
{
    QueryTreeNodes materialized_ctes;

    traverseQueryTree(node, ExceptSubqueries{},
    [&](const QueryTreeNodePtr & current_node)
    {
        if (auto * table_node = current_node->as<TableNode>())
        {
            if (table_node->isMaterializedCTE())
                materialized_ctes.push_back(current_node);
        }
    });

    return materialized_ctes;
}

}
