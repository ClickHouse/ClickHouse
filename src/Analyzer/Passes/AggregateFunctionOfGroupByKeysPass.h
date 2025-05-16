#pragma once

#include <Analyzer/IQueryTreePass.h>

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
    String getName() override { return "AggregateFunctionOfGroupByKeys"; }

    String getDescription() override
    {
        return "Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}

