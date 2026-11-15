#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove injective functions from `uniq*` functions arguments.
  *
  * Example: SELECT uniq(injectiveFunction(argument));
  * Result: SELECT uniq(argument);
  */
class UniqInjectiveFunctionsEliminationPass final : public IQueryTreePass
{
public:
    String getName() override { return "UniqInjectiveFunctionsElimination"; }

    String getDescription() override { return "Remove injective functions from uniq functions arguments."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
