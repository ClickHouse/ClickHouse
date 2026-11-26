#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrites constant-key `Map` element lookups (`arrayElement(col, 'k')`) on plain
  * MergeTree tables to matching `MATERIALIZED` columns defined as `col['k']` of the same type.
  *
  * Enabled with setting `optimize_map_materialized_columns`.
  */
class MaterializedColumnSubstitutionPass final : public IQueryTreePass
{
public:
    String getName() override { return "MaterializedColumnSubstitution"; }

    String getDescription() override
    {
        return "Rewrite constant-key Map arrayElement lookups to matching MATERIALIZED columns on MergeTree tables";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
