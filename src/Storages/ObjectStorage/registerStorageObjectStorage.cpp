#include <Storages/ObjectStorage/AzureConfiguration.h>
#include <Storages/ObjectStorage/S3Configuration.h>
#include <Storages/ObjectStorage/HDFSConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/StorageFactory.h>
#include <Formats/FormatFactory.h>

namespace DB
{

static void initializeConfiguration(
    StorageObjectStorageConfiguration & configuration,
    ASTs & engine_args,
    ContextPtr local_context,
    bool with_table_structure)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
        configuration.fromNamedCollection(*named_collection);
    else
        configuration.fromAST(engine_args, local_context, with_table_structure);
}

template <typename StorageSettings>
static std::shared_ptr<StorageObjectStorage<StorageSettings>> createStorageObjectStorage(
    const StorageFactory::Arguments & args,
    typename StorageObjectStorage<StorageSettings>::ConfigurationPtr configuration,
    const String & engine_name,
    ContextPtr context)
{
    auto & engine_args = args.engine_args;
    if (engine_args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

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

    return std::make_shared<StorageObjectStorage<StorageSettings>>(
        configuration,
        configuration->createOrUpdateObjectStorage(context),
        engine_name,
        args.getContext(),
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        /* distributed_processing */ false,
        partition_by);
}

#if USE_AZURE_BLOB_STORAGE
void registerStorageAzure(StorageFactory & factory)
{
    factory.registerStorage("AzureBlobStorage", [](const StorageFactory::Arguments & args)
    {
        auto context = args.getLocalContext();
        auto configuration = std::make_shared<StorageAzureBlobConfiguration>();
        initializeConfiguration(*configuration, args.engine_args, context, false);
        return createStorageObjectStorage<AzureStorageSettings>(args, configuration, "Azure", context);
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
        auto context = args.getLocalContext();
        auto configuration = std::make_shared<StorageS3Configuration>();
        initializeConfiguration(*configuration, args.engine_args, context, false);
        return createStorageObjectStorage<S3StorageSettings>(args, configuration, name, context);
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
    return registerStorageS3Impl("S3", factory);
}

void registerStorageCOS(StorageFactory & factory)
{
    return registerStorageS3Impl("COSN", factory);
}

void registerStorageOSS(StorageFactory & factory)
{
    return registerStorageS3Impl("OSS", factory);
}

#endif

#if USE_HDFS
void registerStorageHDFS(StorageFactory & factory)
{
    factory.registerStorage("HDFS", [=](const StorageFactory::Arguments & args)
    {
        auto context = args.getLocalContext();
        auto configuration = std::make_shared<StorageHDFSConfiguration>();
        initializeConfiguration(*configuration, args.engine_args, context, false);
        return createStorageObjectStorage<HDFSStorageSettings>(args, configuration, "HDFS", context);
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
}

}
