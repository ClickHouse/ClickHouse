
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterCreateIndexQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Storages/AlterCommands.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_create_index_without_type;
    extern const SettingsBool create_index_ignore_unique;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}


BlockIO InterpreterCreateIndexQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());
    auto current_context = getContext();
    const auto & create_index = query_ptr->as<ASTCreateIndexQuery &>();

    if (create_index.unique)
    {
        if (!current_context->getSettingsRef()[Setting::create_index_ignore_unique])
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CREATE UNIQUE INDEX is not supported."
                " SET create_index_ignore_unique=1 to ignore this UNIQUE keyword.");
        }

    }
    // Noop if allow_create_index_without_type = true. throw otherwise
    if (!create_index.index_decl->as<ASTIndexDeclaration>()->getType())
    {
        if (!current_context->getSettingsRef()[Setting::allow_create_index_without_type])
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY, "CREATE INDEX without TYPE is forbidden."
                " SET allow_create_index_without_type=1 to ignore this statements");
        }
        else
        {
            // Nothing to do
            return {};
        }
    }

    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::ALTER_ADD_INDEX, create_index.getDatabase(), create_index.getTable());

    if (!create_index.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    current_context->checkAccess(required_access);
    auto table_id = current_context->resolveStorageID(create_index, Context::ResolveOrdinary);
    query_ptr->as<ASTCreateIndexQuery &>().setDatabase(table_id.database_name);

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

    /// Convert ASTCreateIndexQuery to AlterCommand.
    AlterCommands alter_commands;

    AlterCommand command;
    command.ast = create_index.convertToASTAlterCommand();
    command.index_decl = create_index.index_decl;
    command.type = AlterCommand::ADD_INDEX;
    command.index_name = create_index.index_name->as<ASTIdentifier &>().name();
    command.if_not_exists = create_index.if_not_exists;

    alter_commands.emplace_back(std::move(command));

    auto alter_lock = table->lockForAlter(current_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
    alter_commands.validate(table, current_context);
    alter_commands.prepare(metadata);
    table->checkAlterIsPossible(alter_commands, current_context);
    table->alter(alter_commands, current_context, alter_lock);

    return {};
}

void registerInterpreterCreateIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateIndexQuery", create_fn);
}

}
