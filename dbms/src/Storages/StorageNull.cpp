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

        return StorageNull::create(args.database_name, args.table_name, args.columns, args.constraints);
    });
}

void StorageNull::alter(
    const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder)
{
    lockStructureExclusively(table_lock_holder, context.getCurrentQueryId());
    auto table_id = getStorageID();

    ColumnsDescription new_columns = getColumns();
    IndicesDescription new_indices = getIndices();
    ConstraintsDescription new_constraints = getConstraints();
    params.applyForColumnsOnly(new_columns);
    context.getDatabase(table_id.database_name)->alterTable(context, table_id.table_name, new_columns, new_indices, new_constraints, {});
    setColumns(std::move(new_columns));
}

}
