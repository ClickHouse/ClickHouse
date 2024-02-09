#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Extract arithmeric operations from aggregate functions.
  *
  * Example: SELECT sum(a * 2);
  * Result: SELECT sum(a) * 2;
  */
class AggregateFunctionsArithmericOperationsPass final : public IQueryTreePass
{
public:
    String getName() override { return "AggregateFunctionsArithmericOperations"; }

    String getDescription() override { return "Extract arithmeric operations from aggregate functions."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
