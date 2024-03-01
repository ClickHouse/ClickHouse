#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** If ORDER BY has argument x followed by f(x) transforms it to ORDER BY x.
  * Optimize ORDER BY x, y, f(x), g(x, y), f(h(x)), t(f(x), g(x)) into ORDER BY x, y
  * in case if f(), g(), h(), t() are deterministic (in scope of query).
  * Don't optimize ORDER BY f(x), g(x), x even if f(x) is bijection for x or g(x).
  */
class OptimizeRedundantFunctionsInOrderByPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeRedundantFunctionsInOrderBy"; }

    String getDescription() override { return "If ORDER BY has argument x followed by f(x) transforms it to ORDER BY x."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
