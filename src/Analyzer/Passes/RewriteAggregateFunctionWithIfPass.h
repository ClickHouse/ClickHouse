#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// Rewrite '<aggregate-function>(if())' to '<aggregate-function>If[OrNull]()'
/// sum(if(cond, a, 0)) -> sumIf[OrNull](a, cond)
/// sum(if(cond, a, null)) -> sumIf[OrNull](a, cond)
/// avg(if(cond, a, null)) -> avgIf[OrNull](a, cond)
/// ...
class RewriteAggregateFunctionWithIfPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteAggregateFunctionWithIf"; }

    String getDescription() override
    {
        return "Rewrite aggregate functions with if expression as argument when logically equivalent";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
