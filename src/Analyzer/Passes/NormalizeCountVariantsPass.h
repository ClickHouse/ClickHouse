#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove a non-NULL literal or an eligible non-nullable stored `MergeTree` column argument from `count`.
  * Convert `sum` with single `1` literal argument into `count`.
  *
  * Example: SELECT count(1);
  * Result: SELECT count();
  *
  * Example: SELECT sum(1);
  * Result: SELECT count();
  */
class NormalizeCountVariantsPass final : public IQueryTreePass
{
public:
    String getName() override { return "NormalizeCountVariants"; }

    String getDescription() override { return "Optimize count of non-NULL literals or stored MergeTree columns, and sum(1), into count()."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
