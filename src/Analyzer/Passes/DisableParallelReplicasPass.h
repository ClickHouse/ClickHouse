#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Disable parallel replicas in case of correlated subquery, or if the query reads a temporary table that is not sent to the replicas.
 */
class DisableParallelReplicasPass final : public IQueryTreePass
{
public:
    String getName() override { return "DisableParallelReplicas"; }

    String getDescription() override
    {
        return "Disable parallel replicas if correlated subquery is present or a temporary table that is not sent to the replicas is read";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
