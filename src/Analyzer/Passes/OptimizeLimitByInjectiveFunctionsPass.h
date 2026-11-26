#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/* Eliminates injective functions in LIMIT BY section.
 * Ex.:    LIMIT 5 BY toString(x)
 * Output: LIMIT 5 BY x
 */
class OptimizeLimitByInjectiveFunctionsPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeLimitByInjectiveFunctions"; }

    String getDescription() override { return "Replaces injective functions by their arguments in LIMIT BY section."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
