#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Derives what a query asks of the table names of the databases a system table enumerates, from
/// its filter on the column that holds the table name (`name` in `system.tables`, `table` almost
/// everywhere else). Recognizes `col = 'a'` and `col IN ('a', 'b')` - including through `OR`, and
/// through an `IN` over a subquery once its set is ready - as an exact set of names, and
/// `col LIKE 'prefix%'` (or its analyzer rewrite `startsWith(col, 'prefix')`) as a pattern.
///
/// A database uses the result to enumerate less than everything it holds. It is only ever a way
/// to do less work: the caller must still apply the real filter to whatever came back, because a
/// name the filter allows need not exist and `TablesFilter::Kind::Like` is a listing hint that
/// nothing is obliged to match.
TablesFilter extractTablesFilter(const ActionsDAG::Node * predicate, const String & table_name_column, const ContextPtr & context);

/// The exact-set half of the above, as a `filter_by_table_name` predicate: accepts only the
/// values of `column_name` the query can ask for, and is an empty function when the filter does
/// not pin them down and everything has to be visited. Used for the `database` column, and for
/// the table name column of the system tables that have no use for the `Like` listing hint.
std::function<bool(const String &)> extractNameFilter(
    const ActionsDAG::Node * predicate, const String & column_name, const ContextPtr & context);

}
