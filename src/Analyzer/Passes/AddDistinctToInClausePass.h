#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{
/**
 * * Add DISTINCT to IN clause.
 * * Example: SELECT * FROM table WHERE column IN (SELECT column FROM table);
 * * Result: SELECT * FROM table WHERE column IN (SELECT DISTINCT column FROM table);
 * *
 * * This pass is mainly used to reduce the amount of data transferred as temporary tables
 * * across the cluster for GLOBAL IN queries, by ensuring only unique values are sent.
 * *
 * * Note: This is a trade-off setting. Adding DISTINCT reduces the size of temporary tables
 * * transferred between nodes, but introduces additional merging effort on each node to deduplicate
 * * the data. Use this setting when network transfer is a bottleneck and the cost of merging is acceptable.
 */

class AddDistinctToInClausePass final : public IQueryTreePass
{
public:
    String getName() override { return "AddDistinctToInClause"; }

    String getDescription() override
    {
        return "Add DISTINCT to IN clause";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
