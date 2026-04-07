#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Eliminate duplicate columns from ORDER BY and LIMIT BY.
  *
  * Example: SELECT * FROM test_table ORDER BY id, id;
  * Result: SELECT * FROM test_table ORDER BY id;
  *
  * Example: SELECT * FROM test_table LIMIT 5 BY id, id;
  * Result: SELECT * FROM test_table LIMIT 5 BY id;
  */
class OrderByLimitByDuplicateEliminationPass final : public IQueryTreePass
{
public:
    String getName() override { return "OrderByLimitByDuplicateElimination"; }

    String getDescription() override { return "Remove duplicate columns from ORDER BY, LIMIT BY."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
