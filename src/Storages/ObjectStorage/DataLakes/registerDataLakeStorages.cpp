#include "config.h"

#if USE_AWS_S3

#    include <Storages/ObjectStorage/Azure/Configuration.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/IStorageDataLake.h>
#    include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#    include <Storages/ObjectStorage/Local/Configuration.h>
#    include <Storages/ObjectStorage/S3/Configuration.h>

#if USE_HDFS
#    include <Storages/ObjectStorage/HDFS/Configuration.h>
#endif

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
                configuration, args.getContext(), args.table_id, args.columns, args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });

    factory.registerStorage(
        "IcebergS3",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3Configuration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), false);

            return StorageIceberg::create(
                configuration, args.getContext(), args.table_id, args.columns, args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });

    factory.registerStorage(
        "IcebergAzure",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageAzureConfiguration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), true);

            return StorageIceberg::create(
                configuration, args.getContext(), args.table_id, args.columns, args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::AZURE,
        });

    factory.registerStorage(
        "IcebergLocal",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageLocalConfiguration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), false);

            return StorageIceberg::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::FILE,
        });

#if USE_HDFS
    factory.registerStorage(
        "IcebergHDFS",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageHDFSConfiguration>();
            StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getLocalContext(), false);

            return StorageIceberg::create(
                configuration, args.getContext(), args.table_id, args.columns,
                args.constraints, args.comment, std::nullopt, args.mode);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::HDFS,
        });
#endif
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
