#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class ConvertQueryToCNFPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertQueryToCNFPass"; }

    String getDescription() override { return "Convert query to CNF and apply optimizations using constraints"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
