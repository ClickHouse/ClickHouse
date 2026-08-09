#pragma once

#include <base/types.h>

namespace DB
{

class AccessRightsElements;
class IAST;
struct StorageInMemoryMetadata;

/// Appends a `SELECT` access requirement for the columns read by a mutation expression — a
/// `WHERE` predicate or the right-hand side of an `UPDATE` assignment. Evaluating such an
/// expression reads those columns, so it requires `SELECT` on them, exactly like a plain query
/// (otherwise their values could be inferred indirectly, or copied into a readable column).
///
/// Columns are resolved against `metadata` the same way a plain `SELECT` does: a name that is a
/// real column requires `SELECT` on it (qualification is stripped only when the result is not
/// itself a real column, so a real dotted name like `` `t.id` `` is preserved); a name that is a
/// virtual column not shadowed by a real one (e.g. `_part`, `_row_exists`) is skipped, since it is
/// not real data and needs no grant. Does nothing when `expression` is null.
void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const StorageInMemoryMetadata & metadata);

}
