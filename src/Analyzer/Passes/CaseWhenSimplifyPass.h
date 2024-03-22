#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class CaseWhenSimplifyPass final : public IQueryTreePass
{
public:
    String getName() override { return "CaseWhenSimplifyPass"; }

    String getDescription() override { return "Simplify case when"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};
}
