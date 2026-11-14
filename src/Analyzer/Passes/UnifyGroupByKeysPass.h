#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** After the aggregation, an expression that is equal to a `GROUP BY` key is read from its result by name. The name of an expression
  * depends on how its constants are written, see `areConstantsWrittenAlike`. So the pass replaces expressions that are equal to a key,
  * but written differently, with the key, if they cannot be calculated from the keys as they are.
  *
  * Example: SELECT intDiv(x, 24 * 3600) FROM t GROUP BY intDiv(x, 86400)
  * Result: SELECT intDiv(x, 86400) FROM t GROUP BY intDiv(x, 86400)
  */
class UnifyGroupByKeysPass final : public IQueryTreePass
{
public:
    String getName() override { return "UnifyGroupByKeys"; }

    String getDescription() override { return "Replace expressions that are equal to a GROUP BY key, but written differently, with it."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
