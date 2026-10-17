#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Optimize expressions of the form `op(toString(x), 'string constant')`, where `op` is `equals`, `like`, `ilike`, `position`
  * (and their negated variants), the conversion is `toString` or `CAST(..., 'String')`, and `x` is an expression of a type
  * whose text representation uses a restricted alphabet (numbers, dates, times, and arrays/tuples/maps of them).
  *
  * 1. Pruning impossible expressions: if the string constant requires characters that can never appear in the text
  *    representation of the type of `x`, the whole expression is replaced with a constant.
  *    For example, `toString(number) LIKE '%hello%'` is always false because the text representation
  *    of an unsigned integer consists only of the digits 0..9.
  *
  * 2. Destructuring tuples: `toString(tuple) LIKE '%needle%'` is rewritten into a chain of per-element conditions
  *    `toString(x) LIKE '%needle%' OR toString(y) LIKE '%needle%'` when the needle cannot span multiple tuple elements.
  *    This may allow the query to use a text index.
  */
class OptimizeStringConversionComparisonPass final : public IQueryTreePass
{
public:
    String getName() override { return "OptimizeStringConversionComparisonPass"; }

    String getDescription() override
    {
        return "Prune always-false comparisons of stringified restricted-alphabet types with string constants and destructure stringified tuple matching into per-element conditions.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
