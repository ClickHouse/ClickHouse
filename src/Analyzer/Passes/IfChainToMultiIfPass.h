#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert `if` chain into single `multiIf`.
  * Replace if(cond_1, then_1_value, if(cond_2, ...)) chains into multiIf(cond_1, then_1_value, cond_2, ...).
  *
  * Example: SELECT if(cond_1, then_1_value, if(cond_2, then_2_value, else_value));
  * Result: SELECT multiIf(cond_1, then_1_value, cond_2, then_2_value, else_value);
  */
class IfChainToMultiIfPass final : public IQueryTreePass
{
public:
    String getName() override { return "IfChainToMultiIf"; }

    String getDescription() override { return "Optimize if chain to multiIf"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
