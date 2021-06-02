#include <Interpreters/InterpreterDeleteQuery.h>
#include <Access/ContextAccess.h>
#include "Interpreters/Context.h"
#include "Storages/StorageMergeTree.h"

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB
{
InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{ }

BlockIO InterpreterDeleteQuery::execute()
{
    auto & query = query_ptr->as<ASTDeleteQuery&>();

    const StorageID table_id = getContext()->resolveStorageID({query.database, query.table}, Context::ResolveOrdinary);

    getContext()->checkAccess(AccessType::DELETE_FROM_TABLE, table_id);

    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    StorageMergeTree * const ptr = dynamic_cast<StorageMergeTree*>(table.get());

    if (ptr == nullptr)
        throw Exception("Only MergeTree tables are supported", ErrorCodes::BAD_ARGUMENTS);

    return {};
}
}
