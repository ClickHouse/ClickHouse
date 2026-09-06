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

    /// A freshly introduced definition (`CREATE`, a full-definition `ATTACH`, or a backup
    /// `RESTORE`) must be validated; a load of already-validated metadata stored on this server
    /// (server startup, short `ATTACH`) must not be, or a validly created table could not be
    /// re-attached. Inference still runs whenever the structure was omitted, because it is then the
    /// only source of the table's columns.
    const bool loading_from_existing_metadata = isLoadingFromExistingMetadata(mode) || attach_short_syntax;

    /// A table-function target routing back to a local shard is analyzed under the user's context
    /// even when the columns are given: analyzing it is what checks the creator's access to the
    /// tables the persisted table would later read.
    const bool analyze_table_function_target
        = has_local_shard && parsed.remote_table_function_ptr && !loading_from_existing_metadata;

    if (!columns_given || analyze_table_function_target)
    {
        /// When the columns were given the analysis runs purely for its access side effect, so a
        /// restore may tolerate a target that has since vanished. When they were omitted the
        /// analysis is the only source of columns and must succeed.
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

            /// Restore proceeds with the backup's columns, but the swallowed error is logged so a
            /// genuine problem is not hidden.
            LOG_WARNING(
                getLogger(secure ? "RemoteSecure" : "Remote"),
                "Could not analyze the table function target of {} during RESTORE; proceeding with "
                "the columns from the backup metadata. Error: {}",
                dependent_table_id ? dependent_table_id->getNameForLogs() : String{"a Remote table"},
                getExceptionMessage(e, /* with_stacktrace = */ true));
        }
    }

    /// A query against this table can be routed back to a local shard under the credentials supplied
    /// to the engine, bypassing the caller's rights on the target; a persistent table can be both
    /// read and written, so require both. Excluded for a table-function target, whose
    /// `remote_table_id` is the meaningless parser default (`system.one`) - the analysis above is
    /// its equivalent check.
    if (has_local_shard && !parsed.remote_table_function_ptr && !loading_from_existing_metadata)
    {
        local_context->checkAccess(AccessType::SELECT, parsed.remote_table_id);
        local_context->checkAccess(AccessType::INSERT, parsed.remote_table_id);
    }

    return result;
}

}
