#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// Rewrite 'sum(if())' to 'sumIf()'
/// sum(if(cond, a, 0)) -> sumIf(a, cond)
/// sum(if(cond, a, null)) -> sumIf(a, cond)
/// avg(if(cond, a, null)) -> avgIf(a, cond)
/// ..

class RewriteAggregateFunctionWithIfPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteAggregateFunctionWithIf"; }

    String getDescription() override
    {
        return "Rewrite aggregate functions with if expression as argument when logically equivalent";
    }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
