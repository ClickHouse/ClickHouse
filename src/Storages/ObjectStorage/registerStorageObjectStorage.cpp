#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>

namespace DB
{

#if USE_AWS_S3 || USE_AZURE_BLOB_STORAGE || USE_HDFS

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static std::shared_ptr<StorageObjectStorage> createStorageObjectStorage(
    const StorageFactory::Arguments & args,
    StorageObjectStorage::ConfigurationPtr configuration,
    ContextPtr context)
{
    auto & engine_args = args.engine_args;
    if (engine_args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

    StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, context, false);

    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    std::optional<FormatSettings> format_settings;
    if (args.storage_def->settings)
    {
        FormatFactorySettings user_format_settings;

        // Apply changed settings from global context, but ignore the
        // unknown ones, because we only have the format settings here.
        const auto & changes = context->getSettingsRef().changes();
        for (const auto & change : changes)
        {
            if (user_format_settings.has(change.name))
                user_format_settings.set(change.name, change.value);
        }

        // Apply changes from SETTINGS clause, with validation.
        user_format_settings.applyChanges(args.storage_def->settings->changes);
        format_settings = getFormatSettings(context, user_format_settings);
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
        configuration->createObjectStorage(context, /* is_readonly */false),
        args.getContext(),
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        /* distributed_processing */ false,
        partition_by);
}

#endif

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

}
