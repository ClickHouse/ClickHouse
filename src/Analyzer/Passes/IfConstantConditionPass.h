#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert `if` with constant condition or `multiIf` with single constant condition into true condition argument value
  * or false condition argument value.
  *
  * Example: SELECT if(1, true_value, false_value);
  * Result: SELECT true_value;
  *
  * Example: SELECT if(0, true_value, false_value);
  * Result: SELECT false_value;
  */
class IfConstantConditionPass final : public IQueryTreePass
{
public:
    String getName() override { return "IfConstantCondition"; }

    String getDescription() override { return "Optimize if, multiIf for constant condition."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
