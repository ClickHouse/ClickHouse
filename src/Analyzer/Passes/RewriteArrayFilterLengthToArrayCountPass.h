#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite 'length(arrayFilter(func, arr, ...))' to 'arrayCount(func, arr, ...)'.
  *
  * `arrayFilter` materializes an array of the elements that pass the predicate only for `length` to
  * throw it away, while `arrayCount` just counts them.
  *
  * Example: SELECT length(arrayFilter(x -> x > 1, arr));
  * Result: SELECT arrayCount(x -> x > 1, arr);
  */
class RewriteArrayFilterLengthToArrayCountPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteArrayFilterLengthToArrayCount"; }

    String getDescription() override { return "Rewrite length(arrayFilter(func, arr)) to arrayCount(func, arr) to avoid materializing the filtered array"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
