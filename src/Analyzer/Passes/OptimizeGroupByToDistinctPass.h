#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class OptimizeGroupByToDistinctPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeGroupByToDistinct"; }

    String getDescription() override
    {
        return "Rewrite GROUP BY with LIMIT to DISTINCT";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
