#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>


namespace DB
{

void IStorage::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context, TableStructureWriteLockHolder & table_lock_holder)
{
    for (const auto & param : params)
    {
        if (param.isMutable())
            throw Exception("Method alter supports only change comment of column for storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());
    auto new_columns = getColumns();
    auto new_indices = getIndices();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, new_indices, {});
    setColumns(std::move(new_columns));
}

}
