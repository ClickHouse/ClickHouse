#pragma once

#include <base/types.h>

namespace DB
{

class AccessRightsElements;
class IAST;
class VirtualColumnsDescription;

/// Appends a `SELECT` access requirement for the columns read by a mutation expression — a
/// `WHERE` predicate or the right-hand side of an `UPDATE` assignment. Evaluating such an
/// expression reads those columns, so it requires `SELECT` on them, exactly like a plain query
/// (otherwise their values could be inferred indirectly, or copied into a readable column).
///
/// Qualifications (`table.col`, `db.table.col`) are stripped so column-level grants match, and
/// virtual columns (e.g. `_part`, `_row_exists`) are skipped because they are not real data and
/// need no `SELECT` grant — mirroring how a plain `SELECT` treats them. Does nothing when
/// `expression` is null.
void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const VirtualColumnsDescription & virtual_columns);

}
