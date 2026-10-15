#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Removes the keys of GROUP BY, ORDER BY and window PARTITION BY that WHERE fixes to a constant.
  * Example: SELECT tenant, day, count() FROM t WHERE tenant = 'x' GROUP BY tenant, day ORDER BY tenant, day
  * Result:  SELECT materialize('x'), day, count() FROM t WHERE tenant = 'x' GROUP BY 'x', day ORDER BY day
  * The planner then drops the constant GROUP BY key.
  */
class OptimizeKeysFixedByWherePass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeKeysFixedByWhere"; }

    String getDescription() override { return "Removes GROUP BY, ORDER BY and PARTITION BY keys that WHERE fixes to a constant."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
