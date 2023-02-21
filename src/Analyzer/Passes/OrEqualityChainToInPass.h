#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class OrEqualityChainToInPass final : public IQueryTreePass
{
public:
    String getName() override { return "OrEqualityChainToIn"; }

    String getDescription() override { return "Transform all the 'or's with equality check to a single IN function"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
