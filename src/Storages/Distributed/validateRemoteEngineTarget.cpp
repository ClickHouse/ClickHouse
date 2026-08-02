#include <Storages/Distributed/validateRemoteEngineTarget.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

ValidatedRemoteEngineTarget parseAndValidateRemoteEngineTarget(
    ASTs & engine_args,
    ContextPtr local_context,
    LoadingStrictnessLevel mode,
    bool attach_short_syntax,
    bool columns_given,
    bool secure,
    bool is_restore_from_backup,
    const StorageID * dependent_table_id)
{
    auto help_message = PreformattedMessage::create(
        "Storage engine '{}' requires from 1 to 6 parameters: "
        "<addresses pattern> [, <name of remote database>, <name of remote table>] [, username[, password], sharding_key]",
        secure ? "RemoteSecure" : "Remote");

    if (engine_args.empty())
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ValidatedRemoteEngineTarget result;
    auto & parsed = result.parsed;
    parsed = parseRemoteFunctionArguments(
        engine_args,
        local_context,
        /* name = */ "remote",
        /* is_cluster_function = */ false,
        secure,
        help_message,
        dependent_table_id);

    bool has_local_shard = false;
    for (const auto & shard_info : parsed.cluster->getShardsInfo())
    {
        if (shard_info.isLocal())
        {
            has_local_shard = true;
            break;
        }
    }

    /// When the structure is not specified, infer it from the remote table under the user's
    /// context. `StorageDistributed` stores only the global context and would otherwise infer
    /// the structure under it, bypassing the `SHOW_COLUMNS` access check in
    /// `getStructureOfRemoteTableInShard` for a local shard — that would let a user who can
    /// create a `Remote` table read the schema of a local table they are not allowed to describe.
    ///
    /// When the target is a table function (e.g. `view(...)`, `numbers(...)`) and the cluster
    /// contains a local shard, the function must be analyzed under the user's context even when
    /// the columns are given explicitly: otherwise a persisted
    /// `Remote('127.0.0.1', view(SELECT ... FROM secret), ...)` could later route the query back
    /// to this server under the engine credentials, while `CREATE` never validated the creator's
    /// access to the function's underlying tables. For a local shard,
    /// `getStructureOfRemoteTableInShard` runs the table function through
    /// `getActualTableStructureWithAccess`, which performs exactly that check.
    ///
    /// These access checks validate the user-supplied definition and must run when it is first
    /// introduced: a `CREATE`, a user `ATTACH` query that carries a full definition, or a backup
    /// `RESTORE` (which brings in a new definition under the restoring user). When the table is
    /// loaded from already-validated metadata that lives on this server (server startup),
    /// re-running them is unnecessary. The inference still runs unconditionally when the structure
    /// was omitted, because then it is the only source of the table's columns.
    ///
    /// `isLoadingFromExistingMetadata` covers server startup (`FORCE_ATTACH`) and the legacy
    /// `force_restore_data` flag (`FORCE_RESTORE`): the definition already lives on disk and was
    /// validated when it was first created on this server, so no check is re-run. A short
    /// `ATTACH TABLE t` query, and the tables loaded by an `ATTACH DATABASE` query, reach here
    /// with `mode == ATTACH`, but their definitions are likewise read back from the metadata
    /// stored on this server (`attach_short_syntax`), not supplied by the user, so they are the
    /// same existing-metadata case: only an `ATTACH` query that carries a full definition
    /// introduces one that still needs validation.
    ///
    /// A backup `RESTORE` is different: it reaches here with `mode == SECONDARY_CREATE` and
    /// `is_restore_from_backup`, introducing the definition under a possibly different user, so it
    /// must be validated like a `CREATE`. Both the plain local-shard `SELECT`/`INSERT` check below and
    /// the table-function analysis run for it, otherwise a user who can restore could smuggle in
    /// `Remote('127.0.0.1', protected_db, protected_table, 'default')` or
    /// `Remote('127.0.0.1', merge(db, '^protected$'), 'default')` and reach a local target they cannot
    /// access directly, even though a direct `CREATE` would be rejected.
    ///
    /// The one concession for a table-function target on restore is that the analysis is allowed to
    /// fail for reasons other than access control: the target's underlying tables may legitimately be
    /// absent in the restore environment (e.g. the table matched by `merge(...)` was dropped since the
    /// backup was taken), and a valid persisted table must still be restorable in that case. An
    /// access-control failure (`ACCESS_DENIED`) is always fatal — it is the exact case a direct
    /// `CREATE` would reject and the only one that could let the restoring user reach a local target
    /// they cannot access. Any other failure means the target could not be analyzed (and therefore
    /// cannot be read either, so there is nothing to leak), so the restore proceeds with the columns
    /// carried in the backup metadata.
    const bool loading_from_existing_metadata = isLoadingFromExistingMetadata(mode) || attach_short_syntax;

    /// The table-function target must be analyzed under the user's context whenever the definition is
    /// freshly introduced (`CREATE`, a full-definition `ATTACH`, or backup `RESTORE`) and can route
    /// back to a local shard; only loads of already-validated stored metadata (server startup, short
    /// `ATTACH`) skip it.
    const bool analyze_table_function_target
        = has_local_shard && parsed.remote_table_function_ptr && !loading_from_existing_metadata;

    if (!columns_given || analyze_table_function_target)
    {
        /// When the structure was carried in the definition, the analysis runs purely for its access
        /// side effect, so on restore a non-access failure (absent target) may be tolerated. When the
        /// structure was omitted, the analysis is the only source of columns and must always succeed.
        const bool tolerate_absent_target = columns_given && is_restore_from_backup;

        try
        {
            ColumnsDescription inferred = getStructureOfRemoteTable(
                *parsed.cluster,
                parsed.remote_table_id,
                local_context,
                parsed.remote_table_function_ptr);
            if (!columns_given)
                result.inferred_columns = std::move(inferred);
        }
        catch (const Exception & e)
        {
            if (!tolerate_absent_target || e.code() == ErrorCodes::ACCESS_DENIED)
                throw;

            /// The target could not be analyzed for a reason other than access control (e.g. the
            /// table matched by `merge(...)` was dropped since the backup was taken). The restore
            /// proceeds with the columns carried in the backup metadata, but the swallowed error is
            /// logged rather than silently discarded, so a genuine problem is not hidden.
            LOG_WARNING(
                getLogger(secure ? "RemoteSecure" : "Remote"),
                "Could not analyze the table function target of {} during RESTORE; proceeding with "
                "the columns from the backup metadata. Error: {}",
                /// Only the storage-construction path can reach this branch (it requires
                /// `is_restore_from_backup`), and it always passes the table's id.
                dependent_table_id ? dependent_table_id->getNameForLogs() : String{"a Remote table"},
                getExceptionMessage(e, /* with_stacktrace = */ true));
        }
    }

    /// If the cluster contains a local shard, a query against this table can be routed back to
    /// this server under the credentials supplied to the engine (e.g. with `prefer_localhost_replica = 0`),
    /// bypassing the caller's access rights on the local target. `TableFunctionRemote::executeImpl`
    /// guards against this with a local-shard `SELECT`/`INSERT` check; mirror it here under the user's
    /// context. A persistent table can be used for both reading and writing, so require both privileges.
    /// For a table-function target, `parsed.remote_table_id` is the meaningless parser default
    /// (`system.one`), so checking it would be both wrong and a spurious rejection of harmless
    /// targets like `numbers(...)`; the equivalent validation is performed above by analyzing the
    /// function itself. Unlike the re-analysis above, this check also runs on a backup `RESTORE`.
    if (has_local_shard && !parsed.remote_table_function_ptr && !loading_from_existing_metadata)
    {
        local_context->checkAccess(AccessType::SELECT, parsed.remote_table_id);
        local_context->checkAccess(AccessType::INSERT, parsed.remote_table_id);
    }

    return result;
}

}
