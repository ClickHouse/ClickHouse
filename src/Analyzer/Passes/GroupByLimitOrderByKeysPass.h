#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// Orders `GROUP BY ... LIMIT n` without `ORDER BY` by the grouping keys when the query may run with parallel replicas.
///
/// Any `n` groups answer such a query, so the smallest `n` by the keys do too. That is what lets the `GROUP BY` top-K heap
/// prune the aggregation, but with parallel replicas the replicas only produce partial states: each may prune its own
/// groups only if the initiator then keeps the smallest `n` of the merged ones, and the replicas learn the query from its
/// text. With the `ORDER BY` in that text the replicas and the initiator agree, and the replicas put the heap on their
/// partial aggregation (`applyTopKPushdownToPartialAggregation`). The single-node plan gets the same `ORDER BY`, so that
/// it measures what the replicas would run and automatic parallel replicas can match the two plans.
class GroupByLimitOrderByKeysPass final : public IQueryTreePass
{
public:
    String getName() override { return "GroupByLimitOrderByKeys"; }

    String getDescription() override
    {
        return "Orders GROUP BY ... LIMIT by the grouping keys when the query may run with parallel replicas.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
