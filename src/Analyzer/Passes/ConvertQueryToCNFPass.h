#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class ConvertQueryToCNFPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertQueryToCnfPass"; }

    String getDescription() override { return "Convert query to CNF"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
