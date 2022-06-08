#include <Interpreters/InterpreterDeleteQuery.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TABLE_IS_READ_ONLY;
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
    auto storage_merge_tree = std::dynamic_pointer_cast<StorageMergeTree>(table);
    if (!storage_merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only MergeTree tables are supported");

    checkStorageSupportsTransactionsIfNeeded(table, getContext());
    if (table->isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    if (typeid_cast<DatabaseReplicated *>(database.get())
        && !getContext()->getClientInfo().is_replicated_database_internal)
    {
        auto guard = DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name);
        guard->releaseTableLock();
        return typeid_cast<DatabaseReplicated *>(database.get())->tryEnqueueReplicatedDDL(query_ptr, getContext());
    }

    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    /// Currently do similar as alter table delete.
    /// TODO: Mark this delete as lightweight.
    MutationCommands mutation_commands;
    MutationCommand mut_command;

    mut_command.type = MutationCommand::Type::DELETE;
    mut_command.predicate = delete_query.predicate;

    auto command = std::make_shared<ASTAlterCommand>();
    command->type = ASTAlterCommand::DELETE;
    command->predicate = delete_query.predicate;
    command->children.push_back(command->predicate);
    mut_command.ast = command->ptr();

    mutation_commands.emplace_back(mut_command);

    if (!mutation_commands.empty())
    {
        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), false).validate();
        storage_merge_tree->mutate(mutation_commands, getContext(), MutationType::Lightweight);
    }

    return {};
}

}
