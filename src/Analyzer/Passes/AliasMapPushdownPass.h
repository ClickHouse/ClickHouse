#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Pushes constant-key `Map` element/membership lookups (`arrayElement`, `has`, `mapContainsKey`)
  * into table `ALIAS` column expressions (`mapFilter`, `mapApply`, `mapUpdate`, `if`, `multiIf`, literal `map`)
  * while preserving `SELECT` privilege delegation on the `ALIAS` column.
  *
  * Enabled with setting `optimize_map_alias_pushdown`.
  */
class AliasMapPushdownPass final : public IQueryTreePass
{
public:
    String getName() override { return "AliasMapPushdown"; }

    String getDescription() override
    {
        return "Push constant-key Map lookups into ALIAS column expressions";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
