#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replace `ifNull(x, ...)` and `coalesce(x, ...)` with `x` when `x` cannot be NULL,
  * i.e. when the function is a pure identity on its first argument (its result type
  * equals the first argument's result type).
  *
  * This keeps the query tree free of redundant wrapper nodes so that expression matching
  * (partition/primary key/skip index pruning in KeyCondition) can see the bare argument even
  * when the wrapper is nested inside a larger key expression, e.g. `sipHash64(ifNull(p, 0))`.
  *
  * Example: SELECT sipHash64(ifNull(non_nullable_col, 0));
  * Result:  SELECT sipHash64(non_nullable_col);
  */
class RemoveRedundantIfNullCoalescePass final : public IQueryTreePass
{
public:
    String getName() override { return "RemoveRedundantIfNullCoalesce"; }

    String getDescription() override { return "Remove ifNull/coalesce that are identity on a non-Nullable first argument."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
