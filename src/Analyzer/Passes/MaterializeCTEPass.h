#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Materialize a CTE node if it satisfies one of the following conditions:
  * - is_materialized is true
  * - the CTE is referred more than once
  */
class MaterializeCTEPass final : public IQueryTreePass
{
public:
    String getName() override { return "MaterializeCTE"; }

    String getDescription() override { return "Materialize CTE if it satisfies one of the following conditions: it has MATERIALIZED keyword, or it is referred more than once"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
