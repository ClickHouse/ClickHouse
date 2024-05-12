#pragma once

#include <Analyzer/IQueryTreePass.h>
#include <Parsers/IAST.h>

namespace DB
{

class OptimizeL2DistancePass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeL2Distance"; }

    String getDescription() override { return "Optimize l2distance to sqrt(l2squared)"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

private:
    void visit(ASTSelectQuery & select_query, Data & data);
};

}
