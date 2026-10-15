#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class QueryNode;

/** Move the AND-conjuncts of the `HAVING` of a resolved aggregating query that do not depend on the
  * aggregation result into `WHERE`. This mimics the `tryMovePredicatesFromHavingToWhere` rewrite of the
  * query analysis that ClickHouse used before v24.3.
  *
  * Conjuncts containing aggregate, `grouping`, `arrayJoin` or non-deterministic functions stay in `HAVING`,
  * as well as conjuncts with subqueries containing non-deterministic or stateful functions or table functions.
  * With `keep_index_hint_conjuncts`, conjuncts containing `indexHint` also stay in `HAVING`: in `HAVING` it is always true,
  * while in `WHERE` it prunes granules and leaves partial groups. The legacy rewrite moved them, so the compatibility
  * rewrite of the analyzer keeps doing it.
  * With `keep_server_dependent_conjuncts`, conjuncts whose result may differ between servers also stay in `HAVING`:
  * the ones with functions like `shardNum` or `dictGet`, reading a table or a table function, or depending on the time zone
  * of the server, like `toHour(toDateTime(k))`. This is needed when
  * the query runs on a shard, while `HAVING` would be executed on the initiator.
  * The right-hand side of `GLOBAL IN` is not considered server-dependent, because the initiator sends the same set to every server.
  * If any conjunct contains a window function, a stateful function or a function with observable side effects, nothing is moved.
  * Nothing is moved for `WITH CUBE`, `WITH ROLLUP`, `WITH TOTALS` and `GROUPING SETS`, because
  * there the result of `HAVING` depends on the super-aggregate rows or on the totals.
  */
void moveNonAggregateHavingConjunctsToWhere(
    QueryNode & query_node, const ContextPtr & context, bool keep_server_dependent_conjuncts, bool keep_index_hint_conjuncts);

}
