#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/* Eliminates functions of other keys in GROUP BY section.
 * Ex.:    GROUP BY x, f(x)
 * Output: GROUP BY x
 */
class OptimizeGroupByFunctionKeysPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeGroupByFunctionKeys"; }

    String getDescription() override { return "Eliminates functions of other keys in GROUP BY section."; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

    bool enabled(ContextPtr context) const override
    {
        return context->getSettings().optimize_group_by_function_keys;
    }
};

}
