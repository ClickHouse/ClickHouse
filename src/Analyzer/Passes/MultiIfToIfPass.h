#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert `multiIf` with single argument into `if`.
  * Example: SELECT multiIf(x, 1, 0);
  * Result: SELECT if(x, 1, 0);
  */
class MultiIfToIfPass final : public IQueryTreePass
{
public:
    String getName() override { return "MultiIfToIf"; }

    String getDescription() override { return "Optimize multiIf to if for single argument."; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}

