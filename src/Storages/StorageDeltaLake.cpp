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
            StorageS3::Configuration configuration = StorageDeltaLake::getConfiguration(args.engine_args, args.getLocalContext());
            return std::make_shared<StorageDeltaLake>(
                configuration, args.table_id, args.columns, args.constraints, args.comment, args.getContext(), getFormatSettings(args.getContext()));
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
