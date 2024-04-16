#pragma once
#include <Analyzer/IQueryTreePass.h>

namespace DB
{
/**
 *  If join_use_nulls = true, then rewrite join nide
 *  - if it's full join, make all columns from left/right table to nullable;
 *  - if it's left join, make all columns from right table to nullable;
 *  - if it's right join, make all columns from left table to nullable;
 */
class RewriteJoinUseNullsPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteJoinUseNullsPass"; }

    String getDescription() override { return "Rewrite JOIN with join_use_nulls enable"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
