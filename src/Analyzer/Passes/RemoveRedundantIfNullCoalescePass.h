#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** In `WHERE` and `PREWHERE`, replace `ifNull(x, ...)` and `coalesce(x, ...)` with `x` when `x` cannot be NULL
  * and the other arguments are constants, so that index analysis matches `x` inside a key expression.
  *
  * Example: SELECT count() FROM t WHERE sipHash64(ifNull(non_nullable_col, 0)) = 42;
  * Result:  SELECT count() FROM t WHERE sipHash64(non_nullable_col) = 42;
  */
class RemoveRedundantIfNullCoalescePass final : public IQueryTreePass
{
public:
    String getName() override { return "RemoveRedundantIfNullCoalesce"; }

    String getDescription() override { return "Remove ifNull/coalesce that are identity on a non-Nullable first argument in WHERE and PREWHERE."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
