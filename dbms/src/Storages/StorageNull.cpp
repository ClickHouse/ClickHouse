#include <Storages/StorageNull.h>
#include <Storages/StorageFactory.h>

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

}
