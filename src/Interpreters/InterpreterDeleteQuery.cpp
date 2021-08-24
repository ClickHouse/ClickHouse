#include <Interpreters/InterpreterDeleteQuery.h>
#include <Access/ContextAccess.h>
#include "Interpreters/Context.h"
#include <Storages/StorageMergeTree.h>
#include <Interpreters/MutationsInterpreter.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB
{
InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_) { }

BlockIO InterpreterDeleteQuery::execute()
{
    ASTDeleteQuery & query = query_ptr->as<ASTDeleteQuery &>();

    const StorageID table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);

    getContext()->checkAccess(AccessType::DELETE, table_id);

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    StorageMergeTree * const ptr = typeid_cast<StorageMergeTree *>(table.get());

    if (ptr == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only MergeTree tables are supported");

    // TODO
    auto alter_lock = table->lockForAlter(
        getContext()->getCurrentQueryId(),
        getContext()->getSettingsRef().lock_acquire_timeout);

    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    // TODO
    /// Add default database to table identifiers that we can encounter in e.g. default expressions,
    /// mutation expression, etc.
    //AddDefaultDatabaseVisitor visitor(table_id.getDatabaseName());
    //ASTPtr command_list_ptr = alter.command_list->ptr();
    //visitor.visit(command_list_ptr);

    auto & predicate = query.predicate;

    // TODO no check for non-replicated tables as for now
    /// table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());

    // TODO no validation required for non-replicated tables
    //MutationsInterpreter(table, metadata_snapshot, predicate, getContext()).validate();

    ptr->pointDelete(predicate, getContext());

    return {};
}
}
