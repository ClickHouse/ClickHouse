#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite 'has(const_array, x)' to 'in(x, const_array)' when the first argument is a constant array.
  * The has() implementation has to handle both const/non-const arrays and it uses Field.
  * With a medium to large array, has() is significantly slower than in() implementation.
  */
class RewriteHasToInPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteHasToIn"; }

    String getDescription() override { return "Rewrite has(const_array, elem) to in(elem, const_array) when first argument is constant array"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
