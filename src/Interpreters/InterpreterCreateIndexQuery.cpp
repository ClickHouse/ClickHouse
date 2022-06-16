#include <Interpreters/InterpreterCreateIndexQuery.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Storages/AlterCommands.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_READ_ONLY;
}


BlockIO InterpreterCreateIndexQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const auto & create_index = query_ptr->as<ASTCreateIndexQuery &>();

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_ADD_INDEX, create_index.getDatabase(), create_index.getTable());

    if (!create_index.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    getContext()->checkAccess(required_access);
    auto table_id = getContext()->resolveStorageID(create_index, Context::ResolveOrdinary);
    query_ptr->as<ASTCreateIndexQuery &>().setDatabase(table_id.database_name);

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

    /// Convert ASTCreateIndexQuery to AlterCommand.
    AlterCommands alter_commands;

    AlterCommand command;
    command.index_decl = create_index.index_decl;
    command.type = AlterCommand::ADD_INDEX;
    command.index_name = create_index.index_name->as<ASTIdentifier &>().name();
    command.if_not_exists = create_index.if_not_exists;

    /// Fill name in ASTIndexDeclaration
    auto & ast_index_decl = command.index_decl->as<ASTIndexDeclaration &>();
    ast_index_decl.name = command.index_name;

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
