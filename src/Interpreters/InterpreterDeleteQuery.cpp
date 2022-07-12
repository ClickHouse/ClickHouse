#include <Interpreters/InterpreterDeleteQuery.h>

#include <Access/ContextAccess.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTExpressionList.h>
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
    extern const int SUPPORT_IS_DISABLED;
}


InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterDeleteQuery::execute()
{
    if (!getContext()->getSettingsRef().allow_experimental_lightweight_delete && !getContext()->getSettingsRef().allow_experimental_lwd2)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Lightweight delete mutate is experimental. Set `allow_experimental_lightweight_delete` setting to enable it");

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

    /// Convert to MutationCommand
    MutationCommands mutation_commands;
    MutationCommand mut_command;

    if (getContext()->getSettingsRef().allow_experimental_lwd2)
    {
        /// UPDATE __row_exists = 0 WHERE predicate
        mut_command.type = MutationCommand::Type::UPDATE;
        mut_command.predicate = delete_query.predicate;

        auto command = std::make_shared<ASTAlterCommand>();
        command->type = ASTAlterCommand::UPDATE;
        command->predicate = delete_query.predicate;
        command->update_assignments = std::make_shared<ASTExpressionList>();
        auto set_row_exists = std::make_shared<ASTAssignment>();
        set_row_exists->column_name = "__row_exists";
        auto zero_value = std::make_shared<ASTLiteral>(DB::Field(UInt8(0)));
        set_row_exists->children.push_back(zero_value);
        command->update_assignments->children.push_back(set_row_exists);
        command->children.push_back(command->predicate);
        command->children.push_back(command->update_assignments);
        mut_command.column_to_update_expression[set_row_exists->column_name] = zero_value;
        mut_command.ast = command->ptr();

        mutation_commands.emplace_back(mut_command);

        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), false, false).validate();
        storage_merge_tree->mutate(mutation_commands, getContext(), MutationType::Ordinary);
    }
    else
    {
        mut_command.type = MutationCommand::Type::DELETE;
        mut_command.predicate = delete_query.predicate;

        auto command = std::make_shared<ASTAlterCommand>();
        command->type = ASTAlterCommand::DELETE;
        command->predicate = delete_query.predicate;
        command->children.push_back(command->predicate);
        mut_command.ast = command->ptr();

        mutation_commands.emplace_back(mut_command);

        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), false, false).validate();
        storage_merge_tree->mutate(mutation_commands, getContext(), MutationType::Lightweight);
    }


    return {};
}

}
