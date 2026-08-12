#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Extract arithmetic operations from aggregate functions.
  *
  * Example: SELECT sum(a * 2);
  * Result: SELECT sum(a) * 2;
  */
class AggregateFunctionsArithmeticOperationsPass final : public IQueryTreePass
{
public:
    String getName() override { return "AggregateFunctionsArithmeticOperations"; }

    String getDescription() override { return "Extract arithmetic operations from aggregate functions."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
