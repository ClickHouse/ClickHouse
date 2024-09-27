#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropIndexQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/AlterCommands.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
}


BlockIO InterpreterDropIndexQuery::execute()
{
    auto current_context = getContext();
    const auto & drop_index = query_ptr->as<ASTDropIndexQuery &>();

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_DROP_INDEX, drop_index.getDatabase(), drop_index.getTable());

    if (!drop_index.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(required_access);
    auto table_id = current_context->resolveStorageID(drop_index, Context::ResolveOrdinary);
    query_ptr->as<ASTDropIndexQuery &>().setDatabase(table_id.database_name);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, current_context);
    }

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, current_context);
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

    /// Convert ASTDropIndexQuery to AlterCommand.
    AlterCommands alter_commands;

    AlterCommand command;
    command.ast = drop_index.convertToASTAlterCommand();
    command.type = AlterCommand::DROP_INDEX;
    command.index_name = drop_index.index_name->as<ASTIdentifier &>().name();
    command.if_exists = drop_index.if_exists;

    alter_commands.emplace_back(std::move(command));

    auto alter_lock = table->lockForAlter(current_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
    alter_commands.validate(table, current_context);
    alter_commands.prepare(metadata);
    table->checkAlterIsPossible(alter_commands, current_context);
    table->alter(alter_commands, current_context, alter_lock);

    return {};
}

void registerInterpreterDropIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropIndexQuery", create_fn);
}

}
