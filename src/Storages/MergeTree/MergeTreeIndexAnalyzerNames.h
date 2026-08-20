#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct IndexDescription;

/** Compute alternative names for the index columns: the names the index expressions get after
  * the query analyzer's rewrite passes, applied with the settings of the given query context.
  *
  * The index expressions are analyzed with the legacy expression analyzer, so their column names
  * do not reflect the rewrites the analyzer applies to the query (e.g. `multiIf` with a single
  * condition is rewritten to `if`, map element access to a subcolumn read). Index analysis
  * matches filter expressions against index expressions by name, so without these alternative
  * names a rewritten filter expression does not match the index expression, and the index is
  * not used (issue #103128).
  *
  * Returns a list parallel to `index.column_names`, or an empty list when not applicable:
  * the query does not use the analyzer, the index is on plain columns, no name differs after
  * the rewrites, or the index expression cannot be analyzed (best effort).
  */
Names getAlternativeIndexColumnNamesForAnalyzer(const IndexDescription & index, const ContextPtr & context);

}
