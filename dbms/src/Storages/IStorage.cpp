#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>


namespace DB
{

TableStructureReadLock::TableStructureReadLock(StoragePtr storage_, bool lock_structure, bool lock_data, const String & query_id)
    : storage(storage_)
{
    if (lock_data)
        data_lock = storage->data_lock->getLock(RWLockImpl::Read, query_id);
    if (lock_structure)
        structure_lock = storage->structure_lock->getLock(RWLockImpl::Read, query_id);
}

void IStorage::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    for (const auto & param : params)
    {
        if (param.is_mutable())
            throw Exception("Method alter supports only change comment of column for storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    auto lock = lockStructureForAlter(context.getCurrentQueryId());
    auto new_columns = getColumns();
    auto new_indices = getIndicesDescription();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, new_indices, {});
    setColumns(std::move(new_columns));
}

}
