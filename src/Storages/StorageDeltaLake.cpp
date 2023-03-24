#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageDeltaLake.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>


namespace DB
{

void registerStorageDeltaLake(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [](const StorageFactory::Arguments & args)
        {
            auto configuration = StorageDeltaLake::getConfiguration(args.engine_args, args.getLocalContext());
            return std::make_shared<StorageDeltaLake>(
                std::move(configuration), args.getContext(), args.table_id, args.columns, args.constraints,
                args.comment, getFormatSettings(args.getContext()));
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
