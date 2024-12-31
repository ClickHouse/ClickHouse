#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Poco/Logger.h>
#include "Common/logger_useful.h"
#include "Storages/ObjectStorage/StorageObjectStorageSettings.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

// LocalObjectStorage is only supported for Iceberg Datalake operations where Avro format is required. For regular file access, use FileStorage instead.
#if USE_AWS_S3 || USE_AZURE_BLOB_STORAGE || USE_HDFS || USE_AVRO

std::shared_ptr<StorageObjectStorage>
createStorageObjectStorage(const StorageFactory::Arguments & args, StorageObjectStorage::ConfigurationPtr configuration, ContextPtr context)
{
    auto & engine_args = args.engine_args;
    if (engine_args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");


    auto queue_settings = std::make_unique<StorageObjectStorageSettings>();

    queue_settings->loadFromQuery(*args.storage_def);

    StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, context, false, std::move(queue_settings));

    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    std::optional<FormatSettings> format_settings;
    if (args.storage_def->settings)
    {
        Settings settings = context->getSettingsCopy();

        // Apply changes from SETTINGS clause, with validation.
        settings.applyChanges(args.storage_def->settings->changes);

        format_settings = getFormatSettings(context, settings);
    }
    else
    {
        format_settings = getFormatSettings(context);
    }

    ASTPtr partition_by;
    if (args.storage_def->partition_by)
        partition_by = args.storage_def->partition_by->clone();

    return std::make_shared<StorageObjectStorage>(
        configuration,
        configuration->createObjectStorage(context, /* is_readonly */ false),
        args.getContext(),
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        args.mode,
        /* distributed_processing */ false,
        partition_by);
}

#endif
}

#if USE_AZURE_BLOB_STORAGE
void registerStorageAzure(StorageFactory & factory)
{
    factory.registerStorage("AzureBlobStorage", [](const StorageFactory::Arguments & args)
    {
        auto configuration = std::make_shared<StorageAzureConfiguration>();
        return createStorageObjectStorage(args, configuration, args.getLocalContext());
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::AZURE,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    });
}
#endif

#if USE_AWS_S3
void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [=](const StorageFactory::Arguments & args)
    {
        auto configuration = std::make_shared<StorageS3Configuration>();
        return createStorageObjectStorage(args, configuration, args.getLocalContext());
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::S3,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    });
}

void registerStorageS3(StorageFactory & factory)
{
    registerStorageS3Impl("S3", factory);
}

void registerStorageCOS(StorageFactory & factory)
{
    registerStorageS3Impl("COSN", factory);
}

void registerStorageOSS(StorageFactory & factory)
{
    registerStorageS3Impl("OSS", factory);
}

#endif

#if USE_HDFS
void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage("HDFS", [=](const StorageFactory::Arguments & args)
    {
        auto configuration = std::make_shared<StorageHDFSConfiguration>();
        return createStorageObjectStorage(args, configuration, args.getLocalContext());
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessType::HDFS,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    });
}
#endif

void registerStorageObjectStorage(StorageFactory & factory)
{
#if USE_AWS_S3
    registerStorageS3(factory);
    registerStorageCOS(factory);
    registerStorageOSS(factory);
#endif
#if USE_AZURE_BLOB_STORAGE
    registerStorageAzure(factory);
#endif
#if USE_HDFS
    registerStorageHDFS(factory);
#endif
    UNUSED(factory);
}

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

void registerStorageIceberg(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        "Iceberg",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3IcebergConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });

    factory.registerStorage(
        "IcebergS3",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3IcebergConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
#    endif
#    if USE_AZURE_BLOB_STORAGE
    factory.registerStorage(
        "IcebergAzure",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageAzureIcebergConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::AZURE,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
#    endif
#    if USE_HDFS
    factory.registerStorage(
        "IcebergHDFS",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageHDFSIcebergConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::HDFS,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
#    endif
    factory.registerStorage(
        "IcebergLocal",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageLocalIcebergConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::FILE,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
}

#endif


#if USE_PARQUET
void registerStorageDeltaLake(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        "DeltaLake",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3DeltaLakeConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
#    endif
    UNUSED(factory);
}
#endif

void registerStorageHudi(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        "Hudi",
        [&](const StorageFactory::Arguments & args)
        {
            auto configuration = std::make_shared<StorageS3HudiConfiguration>();
            return createStorageObjectStorage(args, configuration, args.getLocalContext());
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
#endif
    UNUSED(factory);
}
}
