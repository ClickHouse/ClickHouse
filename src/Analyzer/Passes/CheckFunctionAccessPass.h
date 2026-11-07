#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Checks `GRANT FUNCTION` for every ordinary function in the optimized query tree, see
  * `access_control_improvements.functions_requiring_grant`.
  *
  * The analyzer checks a call when it resolves it, but the optimization passes after it can replace a call
  * with another function, e.g. `x = 1 OR x = 2 OR x = 3` with `x IN (1, 2, 3)`. The function which is
  * executed requires the grant as well, so this pass must run after all the passes that can change functions.
  */
class CheckFunctionAccessPass final : public IQueryTreePass
{
public:
    String getName() override { return "CheckFunctionAccess"; }

    String getDescription() override { return "Check the FUNCTION grant for the functions of the optimized query tree"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
