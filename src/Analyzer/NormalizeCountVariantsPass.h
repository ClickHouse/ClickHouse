#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert if with constant condition or multiIf into true condition argument value
  * or false condition argument value.
  * Example: SELECT count(1)
  * Result: SELECT count();
  *
  * Example: SELECT sum(1);
  * Result: SELECT count();
  */
class NormalizeCountVariantsPass final : public IQueryTreePass
{
public:
    String getName() override { return "NormalizeCountVariantsPass"; }

    String getDescription() override { return "Optimize count(literal), sum(1) into count()."; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}

