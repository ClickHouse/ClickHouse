#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>
#include <Storages/StorageFactory.h>
#include <Poco/Logger.h>
#include <Disks/DiskType.h>

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
createStorageObjectStorage(const StorageFactory::Arguments & args, ObjectStorageType type)
{
    const auto context = args.getLocalContext();
    auto [configuration, table_options] = StorageObjectStorageConfiguration::initialize(type, args.engine_args, context, false, &args.table_id);

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

    ASTPtr order_by;
    if (args.storage_def->order_by)
        order_by = args.storage_def->order_by->clone();

    ContextMutablePtr context_copy = Context::createCopy(args.getContext());
    Settings settings_copy = args.getLocalContext()->getSettingsCopy();
    context_copy->setSettings(settings_copy);
    return std::make_shared<StorageObjectStorage>(
        configuration,
        // We only want to perform write actions (e.g. create a container in Azure) when the table is being created,
        // and we want to avoid it when we load the table after a server restart.
        configuration->createObjectStorage(context, /* is_readonly */ args.mode != LoadingStrictnessLevel::CREATE, std::nullopt),
        context_copy, /// Use global context.
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        args.mode,
        /* distributed_processing */ false,
        partition_by,
        order_by);
}

#endif
}

#if USE_AZURE_BLOB_STORAGE
void registerStorageAzure(StorageFactory & factory)
{
    factory.registerStorage(AzureDefinition::storage_engine_name, [](const StorageFactory::Arguments & args)
    {
        return createStorageObjectStorage(args, ObjectStorageType::Azure);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::AZURE,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    });
}
#endif

#if USE_AWS_S3
void registerStorageS3Impl(const String & name, StorageFactory & factory)
{
    factory.registerStorage(name, [=](const StorageFactory::Arguments & args)
    {
        return createStorageObjectStorage(args, ObjectStorageType::S3);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::S3,
        .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
    });
}

void registerStorageS3(StorageFactory & factory)
{
    registerStorageS3Impl(S3Definition::storage_engine_name, factory);
}

void registerStorageCOS(StorageFactory & factory)
{
    registerStorageS3Impl(COSNDefinition::storage_engine_name, factory);
}

void registerStorageOSS(StorageFactory & factory)
{
    registerStorageS3Impl(OSSDefinition::storage_engine_name, factory);
}

void registerStorageGCS(StorageFactory & factory)
{
    registerStorageS3Impl(GCSDefinition::storage_engine_name, factory);
}

#endif

#if USE_HDFS
void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage(HDFSDefinition::storage_engine_name, [=](const StorageFactory::Arguments & args)
    {
        return createStorageObjectStorage(args, ObjectStorageType::HDFS);
    },
    {
        .supports_settings = true,
        .supports_sort_order = true, // for partition by
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::HDFS,
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
    registerStorageGCS(factory);
#endif
#if USE_AZURE_BLOB_STORAGE
    registerStorageAzure(factory);
#endif
#if USE_HDFS
    registerStorageHDFS(factory);
#endif
    UNUSED(factory);
}

}
