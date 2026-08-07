#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class ConvertLogicalExpressionToCNFPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertLogicalExpressionToCNFPass"; }

    String getDescription() override { return "Convert logical expression to CNF and apply optimizations using constraints"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
