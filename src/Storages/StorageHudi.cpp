#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageHudi.h>
#include <Formats/FormatFactory.h>
#include <Storages/StorageFactory.h>


namespace DB
{

void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage(
        "Hudi",
        [](const StorageFactory::Arguments & args)
        {
            auto configuration = StorageHudi::getConfiguration(args.engine_args, args.getLocalContext());
            return std::make_shared<StorageHudi>(
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
