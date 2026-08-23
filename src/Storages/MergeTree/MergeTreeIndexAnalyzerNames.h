#pragma once

#include <memory>

#include <Interpreters/Context_fwd.h>

namespace DB
{

struct IndexDescription;
struct AlternativeKeyExpression;
using AlternativeKeyExpressionPtr = std::shared_ptr<const AlternativeKeyExpression>;

/** Compute the alternative form of the index key for the query analyzer: the names the index
  * expressions get after the query analyzer's rewrite passes, applied with the settings of the
  * given query context, together with the rewritten expressions themselves.
  *
  * The index expressions are analyzed with the legacy expression analyzer, so their column names
  * do not reflect the rewrites the analyzer applies to the query (e.g. `multiIf` with a single
  * condition is rewritten to `if`, map element access to a subcolumn read). Index analysis
  * matches filter expressions against index expressions (and their subexpressions) by name, so
  * without the alternative form a rewritten filter expression does not match the index
  * expression, and the index is not used (issue #103128).
  *
  * Returns nullptr when not applicable: the query does not use the analyzer, the index is on
  * plain columns, no name differs after the rewrites, or the index expression cannot be
  * analyzed (best effort). Otherwise the result's `column_names` is parallel to
  * `index.column_names` and `expression` computes the same index columns in the rewritten form.
  */
AlternativeKeyExpressionPtr getAlternativeIndexExpressionForAnalyzer(const IndexDescription & index, const ContextPtr & context);

}
