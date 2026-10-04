#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class QueryNode;

/** Move the AND-conjuncts of the `HAVING` of a resolved aggregating query that do not depend on the
  * aggregation result into `WHERE`. This mimics the `tryMovePredicatesFromHavingToWhere` rewrite of the
  * query analysis that ClickHouse used before v24.3.
  *
  * Conjuncts containing aggregate, `grouping`, `arrayJoin` or non-deterministic functions stay in `HAVING`.
  * If any conjunct contains a window function or a stateful function, nothing is moved.
  * Nothing is moved for `WITH CUBE`, `WITH ROLLUP`, `WITH TOTALS` and `GROUPING SETS`, because
  * there the result of `HAVING` depends on the super-aggregate rows or on the totals.
  */
void moveNonAggregateHavingConjunctsToWhere(QueryNode & query_node, const ContextPtr & context);

}
