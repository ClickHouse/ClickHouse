#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite `sum(if(cond, value_1, value_2))` and `sumIf` functions to `countIf`.
  *
  * Example: SELECT sumIf(1, cond);
  * Result: SELECT countIf(cond);
  *
  * Example: SELECT sum(if(cond, 1, 0));
  * Result: SELECT countIf(cond);
  *
  * Example: SELECT sum(if(cond, 0, 1));
  * Result: SELECT countIf(not(cond));
  */
class SumIfToCountIfPass final : public IQueryTreePass
{
public:
    String getName() override { return "SumIfToCountIf"; }

    String getDescription() override { return "Rewrite sum(if) and sumIf into countIf"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
