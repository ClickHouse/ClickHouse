#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Extract a namespace-pushdown hint from a top-level conjunct on `column_name`:
/// `<column> = '…'` (Equals), `<column> LIKE '…'` / its analyzer rewrite
/// `startsWith(<column>, '…')` (Like), plus an optional `<column> NOT LIKE '…'` exclusion.
TablesFilter extractTableNameFilter(const ActionsDAG::Node * predicate, const String & column_name);

}
