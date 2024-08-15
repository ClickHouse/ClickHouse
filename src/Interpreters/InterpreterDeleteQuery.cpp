#include <Interpreters/InterpreterDeleteQuery.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <Storages/LightweightDeleteDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_IS_READ_ONLY;
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
}


InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterDeleteQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const ASTDeleteQuery & delete_query = query_ptr->as<ASTDeleteQuery &>();
    auto table_id = getContext()->resolveStorageID(delete_query, Context::ResolveOrdinary);

    getContext()->checkAccess(AccessType::ALTER_DELETE, table_id);

    query_ptr->as<ASTDeleteQuery &>().setDatabase(table_id.database_name);

    /// First check table storage for validations.
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (database->shouldReplicateQuery(getContext(), query_ptr))
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return database->tryEnqueueReplicatedDDL(query_ptr, getContext());
    }

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    if (table->supportsDelete())
    {
        /// Convert to MutationCommand
        MutationCommands mutation_commands;
        MutationCommand mut_command;

        mut_command.type = MutationCommand::Type::DELETE;
        mut_command.predicate = delete_query.predicate;

        mutation_commands.emplace_back(mut_command);

        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter::Settings settings(false);
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), settings).validate();
        table->mutate(mutation_commands, getContext());
        return {};
    }
    else if (table->supportsLightweightDelete())
    {
        if (!getContext()->getSettingsRef().enable_lightweight_delete)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Lightweight delete mutate is disabled. "
                            "Set `enable_lightweight_delete` setting to enable it");

        /// Build "ALTER ... UPDATE _row_exists = 0 WHERE predicate" query
        String alter_query =
            "ALTER TABLE " + table->getStorageID().getFullTableName()
            + (delete_query.cluster.empty() ? "" : " ON CLUSTER " + backQuoteIfNeed(delete_query.cluster))
            + " UPDATE `_row_exists` = 0 WHERE " + serializeAST(*delete_query.predicate);

        ParserAlterQuery parser;
        ASTPtr alter_ast = parseQuery(
            parser,
            alter_query.data(),
            alter_query.data() + alter_query.size(),
            "ALTER query",
            0,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);

        auto context = Context::createCopy(getContext());
        context->setSetting("mutations_sync", 2);   /// Lightweight delete is always synchronous
        InterpreterAlterQuery alter_interpreter(alter_ast, context);
        return alter_interpreter.execute();
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DELETE query is not supported for table {}", table->getStorageID().getFullTableName());
    }
}

}
