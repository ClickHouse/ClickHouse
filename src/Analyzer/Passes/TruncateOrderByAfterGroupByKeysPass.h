#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** When a query has both GROUP BY and ORDER BY, the sort order is fully determined
  * once all GROUP BY keys (or injective functions of them) appear in the ORDER BY prefix.
  * Any remaining ORDER BY elements after that point are redundant and can be removed.
  *
  * Example: SELECT a, b, sum(x) FROM t GROUP BY a, b ORDER BY a, b, sum(x)
  *  → ORDER BY a, b  (sum(x) is redundant because a, b fully determine the group)
  *
  * This optimization does not apply when WITH TIES or WITH FILL is used.
  */
class TruncateOrderByAfterGroupByKeysPass final : public IQueryTreePass
{
public:
    String getName() override { return "TruncateOrderByAfterGroupByKeys"; }

    String getDescription() override
    {
        return "Remove trailing ORDER BY elements after all GROUP BY keys are covered.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
