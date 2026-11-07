#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Disable parallel replicas in case of correleated subquery
 */
class DisableParallelReplicasPass final : public IQueryTreePass
{
public:
    String getName() override { return "DisableParallelReplicas"; }

    String getDescription() override
    {
        return "Disable parallel replicas if correlated subquery is present";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

/// Whether `DisableParallelReplicasPass` would turn parallel replicas off in every query and union of the tree,
/// which leaves the planner nothing to read with them. A storage other than `MergeTree` counts as reading with them,
/// since it may plan a query the tree does not show. Does not change the tree.
bool disablesParallelReplicasInWholeTree(const QueryTreeNodePtr & query_tree_node);

}
