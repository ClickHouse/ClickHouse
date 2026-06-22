#include <Interpreters/InterpreterUpdateQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MutationPredicateColumnsAccess.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/executeDDLQueryOnCluster.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_PERMANENTLY_READ_ONLY;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int QUERY_IS_PROHIBITED;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool enable_lightweight_update;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

InterpreterUpdateQuery::InterpreterUpdateQuery(ASTPtr query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(std::move(query_ptr_))
{
}

static MutationCommand createMutationCommand(const ASTUpdateQuery & update_query, const Settings & settings)
{
    auto alter_query = make_intrusive<ASTAlterCommand>();

    alter_query->type = ASTAlterCommand::UPDATE;
    alter_query->set(alter_query->predicate, update_query.predicate);
    alter_query->set(alter_query->update_assignments, update_query.assignments);

    if (update_query.partition)
        alter_query->set(alter_query->partition, update_query.partition);

    auto mutation_command = MutationCommand::parse(
        *alter_query,
        /* parse_alter_commands = */ false,
        /* with_pure_metadata_commands = */ false,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
    if (!mutation_command)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Failed to convert query '{}' to mutation command. It's a bug", update_query.formatForErrorMessage());

    return *mutation_command;
}

BlockIO InterpreterUpdateQuery::execute()
{
    const auto & settings = getContext()->getSettingsRef();
    if (!settings[Setting::enable_lightweight_update])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Lightweight updates are not allowed. Set 'enable_lightweight_update = 1' to allow them");

    FunctionNameNormalizer::visit(query_ptr.get());
    auto & update_query = query_ptr->as<ASTUpdateQuery &>();

    /// Resolve the target table up front and qualify the query with its database, so the access
    /// checks and the dispatched ON CLUSTER query all refer to the same database (matching
    /// InterpreterAlterQuery). Otherwise an unqualified `UPDATE tab ON CLUSTER c ...` would be checked
    /// against the initiator's database while the remote query could bind to each host's default
    /// database.
    ///
    /// The WHERE predicate and the assignment expressions read columns, so they require SELECT on
    /// those columns (virtual columns excluded, as in a plain SELECT). Computed before any dispatch so
    /// the initiating user's read access is enforced on every path, including ON CLUSTER, where the
    /// remote DDL worker may not run as the initiating user.
    AccessRightsElements read_access;
    auto resolved_id = getContext()->tryResolveStorageID(update_query, Context::ResolveOrdinary);
    if (resolved_id)
    {
        update_query.setDatabase(resolved_id.database_name);

        if (StoragePtr resolved_table = DatabaseCatalog::instance().tryGetTable(resolved_id, getContext()))
        {
            const auto & metadata = *resolved_table->getInMemoryMetadataPtr(getContext(), false);
            addExpressionColumnsSelectAccess(read_access, update_query.predicate.get(), resolved_id.database_name, resolved_id.table_name, metadata);
            for (const ASTPtr & assignment : update_query.assignments->children)
                addExpressionColumnsSelectAccess(
                    read_access, assignment->as<const ASTAssignment &>().expression().get(),
                    resolved_id.database_name, resolved_id.table_name, metadata);
        }
        else if (!update_query.cluster.empty())
        {
            /// ON CLUSTER from a node without the table: columns cannot be resolved, so fail closed
            /// by requiring SELECT on the whole table.
            read_access.emplace_back(AccessType::SELECT, resolved_id.database_name, resolved_id.table_name);
        }
    }
    else if (!update_query.cluster.empty())
    {
        /// ON CLUSTER with no current database: the id stays unresolved here, but
        /// executeDDLQueryOnCluster expands empty-database access elements to each host's default
        /// database. Fail closed with the AST table name and an empty database so the predicate/RHS
        /// SELECT requirement is expanded together with ALTER_UPDATE instead of being dropped.
        read_access.emplace_back(AccessType::SELECT, update_query.getDatabase(), update_query.getTable());
    }

    /// Built after `setDatabase` so the ALTER_UPDATE requirement uses the same (resolved) database as
    /// the dispatched query and the read requirements above.
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_UPDATE, update_query.getDatabase(), update_query.getTable());

    if (!update_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        /// Enforce the read (SELECT) requirements on the initiator too, since the remote DDL worker
        /// may not run as the initiating user.
        required_access.append_range(read_access);
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (getContext()->getGlobalContext()->getServerSettings()[ServerSetting::disable_insertion_and_mutation])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Update queries are prohibited");

    getContext()->checkAccess(required_access);
    auto table_id = getContext()->resolveStorageID(update_query, Context::ResolveOrdinary);
    update_query.setDatabase(table_id.database_name);

    /// First check table storage for validations.
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY, "Table is read-only");

    if (!read_access.empty())
        getContext()->checkAccess(read_access);

    if (auto supports = table->supportsLightweightUpdate(); !supports)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Lightweight updates are not supported. {}", supports.error().text);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name, database.get());
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), {}, std::move(guard));
    }

    MutationCommands commands;
    commands.emplace_back(createMutationCommand(update_query, settings));

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);

    BlockIO res;
    res.pipeline = table->updateLightweight(commands, getContext());
    res.pipeline.addStorageHolder(table);
    return res;
}

void registerInterpreterUpdateQuery(InterpreterFactory & factory);
void registerInterpreterUpdateQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUpdateQuery>(args.query, args.context);
    };

    factory.registerInterpreter("InterpreterUpdateQuery", create_fn);
}

}
