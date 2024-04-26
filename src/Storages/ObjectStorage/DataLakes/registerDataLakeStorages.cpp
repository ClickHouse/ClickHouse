#include "config.h"

#if USE_AWS_S3

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/IStorageDataLake.h>
#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Storages/ObjectStorage/S3/Configuration.h>


namespace DB
{

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

void registerStorageIceberg(StorageFactory & factory)
{
    factory.registerStorage(
        "Iceberg",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), false);

            return StorageIceberg::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

#endif

#if USE_PARQUET
void registerStorageDeltaLake(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), false);

            return StorageDeltaLake::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}
#endif

void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage(
        "Hudi",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), false);

            return StorageHudi::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
