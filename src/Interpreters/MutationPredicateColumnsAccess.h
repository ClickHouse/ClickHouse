#pragma once

#include <base/types.h>

namespace DB
{

class AccessRightsElements;
class IAST;

/// Appends a `SELECT` access requirement for the columns referenced in a mutation `WHERE` predicate
/// (`ALTER ... UPDATE/DELETE`, lightweight `UPDATE`/`DELETE`). The predicate columns are read to select
/// the affected rows, so without this check their values could be inferred indirectly by observing how
/// many rows a probing predicate touches. Qualifications (`table.col`, `db.table.col`) are stripped so
/// that column-level grants match. Does nothing when `predicate` is null.
void addPredicateColumnsSelectAccess(
    AccessRightsElements & required_access, const IAST * predicate, const String & database, const String & table);

}
