#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>


namespace DB
{

void IStorage::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context, TableStructureWriteLockHolder & structure_lock)
{
    for (const auto & param : params)
    {
        if (param.is_mutable())
            throw Exception("Method alter supports only change comment of column for storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    lockStructureExclusively(structure_lock, context.getCurrentQueryId());
    auto new_columns = getColumns();
    auto new_indices = getIndicesDescription();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, new_indices, {});
    setColumns(std::move(new_columns));
}

}
