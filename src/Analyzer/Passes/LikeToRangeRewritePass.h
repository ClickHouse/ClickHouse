#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{
class LikeToRangeRewritePass final : public IQueryTreePass
{
public:
    String getName() override { return "LikeToRangeRewritePass"; }

    String getDescription() override { return "Rewrite like expressions with perfect prefixes into ranges"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
