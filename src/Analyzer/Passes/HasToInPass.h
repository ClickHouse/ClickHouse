#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite 'has(const_array, x)' to 'in(x, const_array)' when the first argument is a constant array.
  *
  * The 'in' function can be more efficient for constant arrays because it can pre-build a set
  * for faster lookups, especially when the same constant array is used multiple times.
  *
  * Example: SELECT has([1, 2, 3], x) FROM table;
  * Result: SELECT in(x, [1, 2, 3]) FROM table;
  */
class RewriteHasToInPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteHasToIn"; }

    String getDescription() override { return "Rewrite has(const_array, elem) to in(elem, const_array) when first argument is constant array"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
