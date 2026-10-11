#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Merges `hasAny` calls with constant needle arrays inside one `OR`:
  * `hasAny(arr, [1, 2]) OR hasAny(arr, [2, 3])` -> `hasAny(arr, [1, 2, 3])`.
  * Calls are merged only when their haystacks are equal deterministic expressions and their
  * needle constants have the same type. The merged needles keep the order of the original calls, without duplicates.
  * Nested `OR`s are looked through: `(hasAny(arr, [1]) OR x) OR hasAny(arr, [2])` -> `hasAny(arr, [1, 2]) OR x`.
  * In a query with aggregation, the `GROUP BY` keys and the expressions calculated after aggregation are not rewritten,
  * except for the arguments of aggregate functions.
  */
class ConvertOrHasAnyChainPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertOrHasAnyChain"; }

    String getDescription() override { return "Merges hasAny calls with constant arrays on the same haystack in OR chains into a single hasAny call"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
