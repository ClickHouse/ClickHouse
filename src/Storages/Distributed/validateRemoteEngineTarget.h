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
/// engine requires of its target, under `local_context`, i.e. as the user who supplied the
/// definition. Without `columns_given` the target is analyzed to obtain the structure.
///
/// `dependent_table_id` reaches `parseRemoteFunctionArguments`, which registers it as a dependent of
/// a named collection the addresses come from: pass the table's id while constructing a storage, and
/// nullptr from a preflight, which must not leave a dependency on a table that may never exist.
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
