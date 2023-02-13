#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{


/** Replace CROSS JOIN with INNER JOIN.
  */
class CrossToInnerJoinPass final : public IQueryTreePass
{
public:
    String getName() override { return "CrossToInnerJoin"; }

    String getDescription() override
    {
        return "Replace CROSS JOIN with INNER JOIN";
    }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
