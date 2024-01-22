#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class RewriteSumFunctionWithSumAndCountPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteSumFunctionWithSumAndCountPass"; }

    String getDescription() override { return "Rewrite sum(column +/- literal) into sum(column) and literal * count(column)"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
