#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Eliminate tuples from ORDER BY.
  *
  * Example: SELECT * FROM test_table ORDER BY (a, b);
  * Result: SELECT * FROM test_table ORDER BY a, b;
  */
class OrderByTupleEliminationPass final : public IQueryTreePass
{
public:
    String getName() override { return "OrderByTupleElimination"; }

    String getDescription() override { return "Remove tuple from ORDER BY."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
