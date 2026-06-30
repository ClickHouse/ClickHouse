#include <Planner/CollectMaterializedCTE.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/traverseQueryTree.h>
#include <Interpreters/MaterializedCTE.h>

namespace DB
{


namespace
{

struct MaterializedCteWithLevel
{
    QueryTreeNodePtr table_node;
    size_t level;
};

using CTEToLevelMap = std::unordered_map<MaterializedCTEPtr, MaterializedCteWithLevel>;

}

OrderedMaterializedCTEs collectMaterializedCTEs(const QueryTreeNodePtr & node, const SelectQueryOptions & select_query_options)
{
    if (select_query_options.is_subquery && !select_query_options.force_materialize_cte)
        return {};

    CTEToLevelMap materialized_ctes;
    OrderedMaterializedCTEs ctes_by_level;

    size_t level = 0;
    size_t max_level = 0;
    traverseQueryTree(node, Everything{},
    [&](const QueryTreeNodePtr & current_node)
    {
        if (auto * table_node = current_node->as<TableNode>())
        {
            const auto & cte = table_node->getMaterializedCTE();
            /// Skip a subquery-less reference that is not plan-backed: it is the CTE's temp storage
            /// resolved by name (e.g. a per-shard local plan reading the table the initiator shipped
            /// as an external table). It has no subquery to materialize, so it is read directly and
            /// must not be wrapped in a materializing step.
            if (cte && (table_node->isMaterializedCTE() || cte->hasPlanOrBuilt()))
            {
                auto [it, _] = materialized_ctes.emplace(cte, MaterializedCteWithLevel{current_node, level});

                it->second.level = std::max(it->second.level, level);
                max_level = std::max(max_level, level);

                ++level;
            }
        }
    },
    [&level](const QueryTreeNodePtr & current_node)
    {
        if (auto * table_node = current_node->as<TableNode>())
        {
            const auto & cte = table_node->getMaterializedCTE();
            if (cte && (table_node->isMaterializedCTE() || cte->hasPlanOrBuilt()))
                --level;
        }
    });

    if (materialized_ctes.empty())
        return ctes_by_level;

    ctes_by_level.resize(max_level + 1);
    for (const auto & [_, future_table] : materialized_ctes)
    {
        /// Deepest materialized CTEs should be executed first, because CTEs with lower levels depend on them.
        ctes_by_level[future_table.level].push_back(future_table.table_node);
    }

    return ctes_by_level;
}

}
