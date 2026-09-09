#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/ActionsDAG.h>

#include <string_view>

namespace DB
{

/// Extract a namespace-pushdown hint from a top-level conjunct on the column holding the table
/// name: `<column> = '...'` (Equals), or `<column> LIKE '...%'` and its analyzer rewrite
/// `startsWith(<column>, '...')` (Like). The hint lets a DataLake catalog restrict what it lists
/// instead of enumerating everything; an engine that cannot use it ignores it.
///
/// `name_column` is what the querying system table calls that column, and it differs between
/// tables: `system.tables` calls it `name`, while `system.table_settings` calls it `table` and
/// uses `name` for the *setting* name. Naming the wrong column turns a predicate on an unrelated
/// column into a table-name hint - `WHERE name = 'max_threads'` would ask the catalog for a table
/// called `max_threads` and quietly return nothing - so there is deliberately no default.
TablesFilter extractTableNameFilter(const ActionsDAG::Node * predicate, std::string_view name_column);

}
