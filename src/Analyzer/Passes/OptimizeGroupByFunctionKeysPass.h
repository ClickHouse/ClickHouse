#pragma once

#include <Analyzer/IQueryTreePass.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>

namespace DB
{

/* Eliminates functions of other keys in GROUP BY section.
 * Ex.:    GROUP BY x, f(x)
 * Output: GROUP BY x
 */
class OptimizeGroupByFunctionKeysPass final : public IQueryTreePass
{
public:
    explicit OptimizeGroupByFunctionKeysPass(UsableGroupByKeys usable_keys_) : usable_keys(usable_keys_) {}

    String getName() override { return "OptimizeGroupByFunctionKeys"; }

    String getDescription() override { return "Eliminates functions of other keys in GROUP BY section."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

private:
    const UsableGroupByKeys usable_keys;
};

}
