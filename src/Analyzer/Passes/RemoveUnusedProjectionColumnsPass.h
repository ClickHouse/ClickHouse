#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove unused projection columns in subqueries.
  *
  * Example: SELECT a FROM (SELECT a, b FROM test_table);
  * Result: SELECT a FROM (SELECT a FROM test_table);
  */
class RemoveUnusedProjectionColumnsPass final : public IQueryTreePass
{
public:
    String getName() override { return "RemoveUnusedProjectionColumnsPass"; }

    String getDescription() override { return "Remove unused projection columns in subqueries."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
