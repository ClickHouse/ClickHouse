#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class StorageMergeTree;


/// Execute a UNIQUE KEY synchronous DELETE. Owns the full body of the delete:
///   1. Build internal `SELECT _part, _part_offset FROM t WHERE <pred>` to
///      resolve `(part_name, row_number)` pairs (reuses MergeTree's
///      predicate-pushdown / PREWHERE / primary-key skipping).
///   2. Per partition: take the per-partition UK mutex, resolve part pointers,
///      and commit a marker-part staged commit that writes each affected
///      part's cumulative delete-bitmap sidecar.
///   3. Increment `UniqueKeyDeleteRows` by the rows committed.
///
/// Writes only a 0-row marker part (no data rows, no mutation entry);
/// synchronous. Resurrection is handled implicitly by `_block_number`
/// ordering — a later INSERT of a now-dead key supersedes the bitmap-dead
/// row.
///
/// Must be called only on storages whose metadata has UNIQUE KEY; the
/// caller's dispatcher checks `metadata->hasUniqueKey()` first.
void executeUniqueKeyDelete(
    StorageMergeTree & storage,
    const ASTPtr & query_ptr,
    ContextPtr query_context);

}
