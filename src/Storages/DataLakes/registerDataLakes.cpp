#include <Storages/DataLakes/IStorageDataLake.h>
#include "config.h"

#if USE_AWS_S3

#include <Storages/DataLakes/StorageDeltaLake.h>
#include <Storages/DataLakes/Iceberg/StorageIceberg.h>
#include <Storages/DataLakes/StorageHudi.h>
#include <Storages/DataLakes/DeltaLakeMetadataParser.h>


namespace DB
{

#if USE_PARQUET
void registerStorageDeltaLake(StorageFactory & )
{
    // factory.registerStorage(
    //     StorageDeltaLakeName::name,
    //     [&](const StorageFactory::Arguments & args)
    //     {
    //         auto configuration = std::make_shared<StorageS3Configuration>();
    //         return IStorageDataLake<StorageS3Settings, StorageDeltaLakeName, DeltaLakeMetadataParser>::create(
    //             configuration, args.getContext(), "deltaLake", args.table_id, args.columns,
    //             args.constraints, args.comment, std::nullopt, args.attach);
    //     },
    //     {
    //         .supports_settings = false,
    //         .supports_schema_inference = true,
    //         .source_access_type = AccessType::S3,
    //     });
}
#endif

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

void registerStorageIceberg(StorageFactory &)
{
    // REGISTER_DATA_LAKE_STORAGE(StorageIceberg, StorageIceberg::name)
}

#endif

void registerStorageHudi(StorageFactory &)
{
}

}

#endif
