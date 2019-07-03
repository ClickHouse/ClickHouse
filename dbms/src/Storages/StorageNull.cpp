#include <Storages/StorageNull.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>

#include <Interpreters/InterpreterAlterQuery.h>
#include <Databases/IDatabase.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


void registerStorageNull(StorageFactory & factory)
{
    factory.registerStorage("Null", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageNull::create(args.table_name, args.columns);
    });
}

void StorageNull::alter(
    const AlterCommands & params, const String & current_database_name, const String & current_table_name,
    const Context & context, TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());

    ColumnsDescription new_columns = getColumns();
    IndicesDescription new_indices = getIndices();
    params.apply(new_columns);
    context.getDatabase(current_database_name)->alterTable(context, current_table_name, new_columns, new_indices, {});
    setColumns(std::move(new_columns));
}

}
