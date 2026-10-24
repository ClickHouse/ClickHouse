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
  * of the server (including any value of type `Dynamic` or `JSON`, which may hold a `DateTime`), like `toHour(toDateTime(k))`, `toDate(k)` for a numeric `k` or `toRelativeSecondNum(d)` for a `Date` `d`. This is needed when
  * the query runs on a shard, while `HAVING` would be executed on the initiator.
  * The right-hand side of `GLOBAL IN` is not considered server-dependent, because the initiator sends the same set to every server.
  * If any conjunct contains a window function or a stateful function, nothing is moved.
  * With `keep_pre_aggregation_expressions`, nothing is moved either if any conjunct contains a function with observable
  * side effects, or if any other section of the query, like the projection, `WHERE`, `GROUP BY`, `QUALIFY`, `ORDER BY`
  * or `LIMIT BY`, or the join tree, like `JOIN ON` or `ARRAY JOIN`, contains a stateful function, like `sum(rowNumberInAllBlocks())`, a function that is not deterministic
  * in the scope of the query, like `sum(blockSize())`, or a function with observable side effects,
  * like `sum(sleepEachRow(0.001))`: it would be computed on fewer rows before aggregation. The compatibility rewrite of
  * the analyzer does not check it, like the legacy rewrite.
  * Nothing is moved for `WITH CUBE`, `WITH ROLLUP`, `WITH TOTALS` and `GROUPING SETS`, because
  * there the result of `HAVING` depends on the super-aggregate rows or on the totals.
  */
void moveNonAggregateHavingConjunctsToWhere(
    QueryNode & query_node,
    const ContextPtr & context,
    bool keep_server_dependent_conjuncts,
    bool keep_index_hint_conjuncts,
    bool keep_pre_aggregation_expressions);

}
