#include <Storages/StorageNull.h>
#include <Storages/StorageFactory.h>

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

        return StorageNull::create(args.table_name,
            args.columns, args.materialized_columns, args.alias_columns, args.column_defaults);
    });
}

void StorageNull::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    params.apply(columns, materialized_columns, alias_columns, column_defaults);

    context.getDatabase(database_name)->alterTable(
        context, table_name,
        columns, materialized_columns, alias_columns, column_defaults, {});
}

}
