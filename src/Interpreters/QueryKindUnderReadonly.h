#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// Whether a query of this kind is rejected under `readonly` because it modifies data or server state.
///
/// This mirrors the `readonly` enforcement in `ContextAccess`: a query of a kind reported here can never run in a
/// context with `readonly` enabled, so a caller that runs queries in such a context (for example the HTTP
/// execution path, which enables `readonly` for the safe HTTP methods) can tell in advance that the query is not
/// servable there. It is a property of the kind alone - a concrete query of a kind reported here may still be
/// allowed (`CREATE TEMPORARY TABLE` is a `Create` query allowed under `readonly = 2`), so refine it where the
/// distinction matters.
///
/// A kind reported as not rejected is not necessarily free of side effects under `readonly = 2` - see
/// `queryKindHasSideEffectsUnderReadonly`.
bool isQueryKindRejectedUnderReadonly(IAST::QueryKind kind);

/// Whether `readonly = 2` (the mode that allows changing settings but forbids writes) still lets a query of this
/// kind produce side effects. Two groups:
/// - `BACKUP` writes an archive to disk or object storage and `RESTORE` writes data into tables, yet
///   `BackupsWorker` rejects them only under the strict, user-set `readonly = 1`.
/// - Session- and transaction-mutating statements: `SET` changes session settings (allowed under `readonly = 2`,
///   which forbids only changing `readonly` itself), `SET ROLE` changes the active roles, `USE` changes the
///   session database, and `BEGIN` / `COMMIT` / `ROLLBACK` / `SET TRANSACTION SNAPSHOT` mutate the current
///   transaction - none of which `readonly` blocks. These effects outlive the query whenever the session outlives
///   it, which for the HTTP interface is the case when the client passes `session_id`.
///
/// So `readonly = 2` alone is not enough to make a query effect-free, and a caller that relies on that (a
/// read-only HTTP endpoint, for example) has to exclude these kinds by itself.
bool queryKindHasSideEffectsUnderReadonly(IAST::QueryKind kind);

}
