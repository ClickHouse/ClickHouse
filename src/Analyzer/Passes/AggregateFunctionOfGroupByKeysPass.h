#pragma once

#include <Analyzer/IQueryTreePass.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>

namespace DB
{

/** Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section.
  *
  * Example: SELECT max(column) FROM table GROUP BY column;
  * Result: SELECT column FROM table GROUP BY column;
  */
class AggregateFunctionOfGroupByKeysPass final : public IQueryTreePass
{
public:
    explicit AggregateFunctionOfGroupByKeysPass(UsableGroupByKeys usable_keys_) : usable_keys(usable_keys_) {}

    String getName() override { return "AggregateFunctionOfGroupByKeys"; }

    String getDescription() override
    {
        return "Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

private:
    const UsableGroupByKeys usable_keys;
};

}

