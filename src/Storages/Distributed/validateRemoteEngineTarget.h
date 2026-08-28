#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Distributed/parseRemoteFunctionArguments.h>


namespace DB
{

struct StorageID;

/// Everything `parseAndValidateRemoteEngineTarget` produced: the parsed arguments plus the
/// structure it inferred, if it inferred one.
struct ValidatedRemoteEngineTarget
{
    ParsedRemoteFunctionArguments parsed;

    /// The structure inferred from the target. Empty when the caller supplied one, or when a
    /// tolerated inference failure left the caller's structure in place.
    ColumnsDescription inferred_columns;
};

/// Parses the arguments of a `Remote`/`RemoteSecure` engine and performs every access check the
/// engine requires of its target, under `local_context` (i.e. as the user who supplied the
/// definition).
///
/// This is the single definition of what the engine requires: both the storage-construction path
/// and an `ON CLUSTER` initiator call it.
///
/// `columns_given` says whether the caller already has the table's structure; when it does not, the
/// target is analyzed to obtain it. `dependent_table_id` is forwarded to
/// `parseRemoteFunctionArguments`, which registers the id as a dependent of a named collection when
/// the addresses come from one: pass the table's id from the storage-construction path, and nullptr
/// from a preflight, which must not register a dependency for a table that may never exist.
ValidatedRemoteEngineTarget parseAndValidateRemoteEngineTarget(
    ASTs & engine_args,
    ContextPtr local_context,
    LoadingStrictnessLevel mode,
    bool attach_short_syntax,
    bool columns_given,
    bool secure,
    bool is_restore_from_backup,
    const StorageID * dependent_table_id);

}
