#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Optimize single `countDistinct` into `count` over subquery.
  *
  * Example: SELECT countDistinct(column) FROM table;
  * Result: SELECT count() FROM (SELECT column FROM table GROUP BY column);
  */
class CountDistinctPass final : public IQueryTreePass
{
public:
    String getName() override { return "CountDistinct"; }

    String getDescription() override
    {
        return "Optimize single countDistinct into count over subquery";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
