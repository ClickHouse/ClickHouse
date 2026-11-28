#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Resolve GROUPING functions in query node.
  * GROUPING function is replaced with specialized GROUPING function based on GROUP BY modifiers.
  * For ROLLUP, CUBE, GROUPING SETS specialized GROUPING function take special __grouping_set column as argument
  * and previous GROUPING function arguments.
  *
  * Example: SELECT grouping(id) FROM test_table GROUP BY id;
  * Result: SELECT groupingOrdinary(id) FROM test_table GROUP BY id;
  *
  * Example: SELECT grouping(id), grouping(value) FROM test_table GROUP BY GROUPING SETS ((id), (value));
  * Result: SELECT groupingForGroupingSets(__grouping_set, id), groupingForGroupingSets(__grouping_set, value)
  * FROM test_table GROUP BY GROUPING SETS ((id), (value));
  */
class GroupingFunctionsResolvePass final : public IQueryTreePass
{
public:
    String getName() override { return "GroupingFunctionsResolvePass"; }

    String getDescription() override { return "Resolve GROUPING functions based on GROUP BY modifiers"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
