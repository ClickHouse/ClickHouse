#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

class UtilizeProjectionPass final : public IQueryTreePass
{
public:
    String getName() override { return "UtilizeProjection"; }

    String getDescription() override
    {
        return "Rewrite where clause to include a indexHint() predicates, in order to utilize projection feature";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
