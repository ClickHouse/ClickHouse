#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Inject the semi-join predicate of an `INNER JOIN` into the subquery on the other side, so that a
  * parallel-replicas fragment - which is shipped as query text and therefore cannot carry a join
  * runtime filter - filters its own share before aggregating.
  *
  *   SELECT ... FROM (SELECT k, sum(v) FROM probe GROUP BY k) AS agg JOIN dim ON agg.k = dim.k
  *   ->
  *   SELECT ... FROM (SELECT k, sum(v) FROM probe WHERE k IN (SELECT k FROM dim) GROUP BY k) AS agg
  *              JOIN dim ON agg.k = dim.k
  *
  * The join stays where it was, so row multiplicity is untouched: the added predicate only removes rows
  * the join would drop anyway. `parallel_replicas_ship_join_predicate` selects `in` (evaluated by every
  * replica) or `globalIn` (evaluated once on the initiator and broadcast).
  */
class ShipJoinPredicateToParallelReplicasPass final : public IQueryTreePass
{
public:
    String getName() override { return "ShipJoinPredicateToParallelReplicas"; }

    String getDescription() override
    {
        return "Inject an INNER JOIN's semi-join predicate into the subquery on the other side so parallel replicas can filter before aggregating";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};


/// Whether the rewrite above could inject a predicate into this query tree, without changing it. The
/// automatic parallel replicas cost model needs the join's match rate to decide whether shipping pays,
/// and a join only counts it when built with analyze statistics on - which has to be decided before
/// planning, long before that cost model runs.
bool hasShippableJoinPredicate(const QueryTreeNodePtr & query_tree_node, ContextPtr context);

}
