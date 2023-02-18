#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{
/// Rewrite possible 'arrayExists(func, arr)' to 'has(arr, elem)' to improve performance
/// arrayExists(x -> x = 1, arr) -> has(arr, 1)
class RewriteArrayExistsToHasPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteArrayExistsToHas"; }

    String getDescription() override { return "Rewrite arrayExists(func, arr) functions to has(arr, elem) when logically equivalent"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};
}
