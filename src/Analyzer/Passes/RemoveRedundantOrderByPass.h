#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove redundant ORDER BY clauses
  *
  * Example: SELECT * FROM (SELECT * FROM t ORDER BY a) ORDER BY a
  * Result: SELECT * FROM (SELECT * FROM t) ORDER BY a
  *
  * See more examples in tests - 02479_duplicate_order_by.sql
  */
class RemoveRedundantOrderByClausesPass final : public IQueryTreePass
{
public:
    String getName() override { return "RemoveRedundantOrderByClausesPass"; }

    String getDescription() override { return "Removes redundant ORDER BY clauses."; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
