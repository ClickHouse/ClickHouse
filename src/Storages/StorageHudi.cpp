#include <Storages/StorageHudi.h>
#include <Storages/StorageFactory.h>

namespace DB {

namespace ErrorCodes {
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageHudi::StorageHudi(
    const StorageID & table_id_,
    ColumnsDescription /*columns_description_*/,
    ConstraintsDescription /*constraints_description_*/,
    const String & /*comment*/
) : IStorage(table_id_) {}



void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage("Hudi", [](const StorageFactory::Arguments & args)
        {
            if (!args.engine_args.empty())
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Engine {} doesn't support any arguments ({} given)",
                    args.engine_name,
                    args.engine_args.size());

            return std::make_shared<StorageHudi>(args.table_id, args.columns, args.constraints, args.comment);
        });
}

}

