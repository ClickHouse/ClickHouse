#pragma once

#include <Analyzer/QueryTreePassManager.h>

namespace DB
{

/** Rewrite "order by limit" as a subquery.
  *
  * Example: SELECT * FROM test_table order by a limit 10;
  * Result:
  *   SELECT *
  *   FROM test_table
  *   WHERE (_part_starting_offset + _part_offset) IN
  *      (SELECT _part_starting_offset + _part_offset
  *       FROM test_table
  *       ORDER BY a
  *       LIMIT 10)
  *   ORDER BY a;
  */
class RewriteOrderByLimitPass final : public IQueryTreePass
{
public:
    String getName() override { return "OrderByLimitRewritePass"; }
    String getDescription() override { return "Rewrite ORDER BY LIMIT pattern to use (_part_starting_offset + _part_offset) subquery."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
