#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Merge a chain of `has`, `hasAny` and single-element `hasAll` calls with constant needles on the same array into one
  * `hasAny`. Needles are sets, so the merged call takes their union:
  *   `has(arr, 'a') OR hasAny(arr, ['b', 'c'])` becomes `hasAny(arr, ['a', 'b', 'c'])`,
  *   `NOT has(arr, 'a') AND NOT hasAny(arr, ['b'])` becomes `NOT hasAny(arr, ['a', 'b'])`.
  * A `hasAll` with two or more elements is a conjunction and is left as written, and so is a chain of plain `has`
  * under `AND`: its merged form would be a `hasAll`, which is slower than the separate calls without a text index.
  * A text index answers one `hasAny` once per block, a chain of calls once per call.
  * Only `WHERE`, `PREWHERE` and `JOIN ON` are rewritten; a chain in the projection, `GROUP BY`, `HAVING` or
  * `ORDER BY` may be made of grouping keys, which have to stay as written.
  */
class MergeHasChainToHasAnyPass final : public IQueryTreePass
{
public:
    String getName() override { return "MergeHasChainToHasAny"; }

    String getDescription() override { return "Merge chains of has, hasAny and single-element hasAll calls with constant needles on the same array into one hasAny"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
