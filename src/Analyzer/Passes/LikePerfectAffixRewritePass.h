#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{
class LikePerfectAffixRewritePass final : public IQueryTreePass
{
public:
    String getName() override { return "LikePerfectAffixRewritePass"; }

    String getDescription() override { return "Rewrite LIKE expressions with perfect affix (prefix or suffix) into startsWith or endsWith functions."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
