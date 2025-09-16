#include <Interpreters/InterpreterUpdateQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserAlterQuery.h>
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
    extern const int TABLE_IS_READ_ONLY;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int QUERY_IS_PROHIBITED;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool enable_lightweight_update;
}

namespace ServerSetting
{
    extern const ServerSettingsBool disable_insertion_and_mutation;
}

InterpreterUpdateQuery::InterpreterUpdateQuery(ASTPtr query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(std::move(query_ptr_))
{
}

static MutationCommand createMutationCommand(const ASTUpdateQuery & update_query)
{
    auto alter_query = std::make_shared<ASTAlterCommand>();

    alter_query->type = ASTAlterCommand::UPDATE;
    alter_query->set(alter_query->predicate, update_query.predicate);
    alter_query->set(alter_query->update_assignments, update_query.assignments);

    if (update_query.partition)
        alter_query->set(alter_query->partition, update_query.partition);

    auto mutation_command = MutationCommand::parse(alter_query.get());
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

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_UPDATE, update_query.getDatabase(), update_query.getTable());

    if (!update_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
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
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

    if (auto supports = table->supportsLightweightUpdate(); !supports)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Lightweight updates are not supported. {}", supports.error().text);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), {});
    }

    MutationCommands commands;
    commands.emplace_back(createMutationCommand(update_query));

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);

    BlockIO res;
    res.pipeline = table->updateLightweight(commands, getContext());
    res.pipeline.addStorageHolder(table);
    return res;
}

void registerInterpreterUpdateQuery(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUpdateQuery>(args.query, args.context);
    };

    factory.registerInterpreter("InterpreterUpdateQuery", create_fn);
}

}
