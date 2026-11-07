#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrites a correlated scalar aggregate subquery over a table that the enclosing query also reads
  * into a window function over that table:
  *
  *   SELECT ... FROM lineitem, part WHERE p_partkey = l_partkey AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)
  *   ->
  *   SELECT ... FROM (SELECT ..., avg(l_quantity) OVER (PARTITION BY l_partkey) AS a FROM lineitem) AS lineitem, part
  *   WHERE p_partkey = l_partkey AND l_quantity < 0.2 * a
  *
  * The window is computed over all rows of the table, before any condition or join of the enclosing
  * query, so it sees exactly the rows the subquery sees for the key of the row.
  */
class CorrelatedScalarAggregateToWindowPass final : public IQueryTreePass
{
public:
    String getName() override { return "CorrelatedScalarAggregateToWindow"; }

    String getDescription() override
    {
        return "Rewrite correlated scalar aggregate subqueries into window functions over a table of the enclosing query";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
