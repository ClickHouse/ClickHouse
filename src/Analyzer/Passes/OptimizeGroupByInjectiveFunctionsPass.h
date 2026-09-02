#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/* Eliminates injective functions in GROUP BY section.
 */
class OptimizeGroupByInjectiveFunctionsPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeGroupByInjectiveFunctionsPass"; }

    String getDescription() override { return "Replaces injective functions by it's arguments in GROUP BY section."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
