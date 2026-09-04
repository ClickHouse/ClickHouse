#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class ASTStorage;

struct PersistedArchivePathSyntax
{
    bool enabled;
    ContextPtr context;
};

/// A full-definition `ATTACH` is fresh user input. `CREATE` and replicated-database
/// DDL replay are fresh too, but `RESTORE`, short `ATTACH`, and `FORCE_*` modes replay
/// metadata that was persisted earlier.
bool isFreshTableDefinition(
    LoadingStrictnessLevel mode,
    bool attach_short_syntax,
    bool is_restore_from_backup);

/// Resolve the archive-path interpretation for a persistent table/database and
/// materialize it into its `SETTINGS` clause. Fresh definitions inherit the
/// current session value when it was not specified explicitly. Old metadata
/// without the setting uses the historical default (`true`) instead of the
/// context that happens to reload it.
PersistedArchivePathSyntax resolveAndPersistArchivePathSyntax(
    ASTStorage & storage_def,
    const ContextPtr & context,
    bool is_fresh_definition);

/// Return a context whose archive-path setting has the supplied value. The
/// original context is reused when it already has that value.
ContextPtr contextWithArchivePathSyntax(const ContextPtr & context, bool enabled);

}
