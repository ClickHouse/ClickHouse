#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterDropIndexQuery.h>
#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/AlterCommands.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
}


BlockIO InterpreterDropIndexQuery::execute()
{
    const auto & drop_index = query_ptr->as<ASTDropIndexQuery &>();

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_DROP_INDEX, drop_index.getDatabase(), drop_index.getTable());

    if (!drop_index.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(required_access);
    auto table_id = getContext()->resolveStorageID(drop_index, Context::ResolveOrdinary);
    query_ptr->as<ASTDropIndexQuery &>().setDatabase(table_id.database_name);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (typeid_cast<DatabaseReplicated *>(database.get())
        && !getContext()->getClientInfo().is_replicated_database_internal)
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return typeid_cast<DatabaseReplicated *>(database.get())->tryEnqueueReplicatedDDL(query_ptr, getContext());
    }

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
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

    auto alter_lock = table->lockForAlter(getContext()->getSettingsRef().lock_acquire_timeout);
    StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
    alter_commands.validate(table, getContext());
    alter_commands.prepare(metadata);
    table->checkAlterIsPossible(alter_commands, getContext());
    table->alter(alter_commands, getContext(), alter_lock);

    return {};
}

}
