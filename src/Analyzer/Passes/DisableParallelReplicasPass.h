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

}
