#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/* Eliminates functions of other keys in LIMIT BY section.
 * Ex.:    LIMIT 5 BY x, f(x)
 * Output: LIMIT 5 BY x
 */
class OptimizeLimitByFunctionKeysPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeLimitByFunctionKeys"; }

    String getDescription() override { return "Eliminates functions of other keys in LIMIT BY section."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
