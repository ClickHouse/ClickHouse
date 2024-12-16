#include "config.h"

#include <Storages/StorageFactory.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Formats/FormatFactory.h>

#if USE_AWS_S3
#include <IO/S3Common.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename Configuration>
StoragePtr createQueueStorage(const StorageFactory::Arguments & args)
{
    auto & engine_args = args.engine_args;
    if (engine_args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

    auto configuration = std::make_shared<Configuration>();
    StorageObjectStorage::Configuration::initialize(*configuration, args.engine_args, args.getContext(), false);

    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    std::optional<FormatSettings> format_settings;

    auto queue_settings = std::make_unique<ObjectStorageQueueSettings>();
    if (args.storage_def->settings)
    {
        queue_settings->loadFromQuery(*args.storage_def);
        FormatFactorySettings user_format_settings;

        // Apply changed settings from global context, but ignore the
        // unknown ones, because we only have the format settings here.
        const auto & changes = args.getContext()->getSettingsRef().changes();
        for (const auto & change : changes)
        {
            if (user_format_settings.has(change.name))
                user_format_settings.set(change.name, change.value);

            args.storage_def->settings->changes.removeSetting(change.name);
        }

        for (const auto & change : args.storage_def->settings->changes)
        {
            if (user_format_settings.has(change.name))
                user_format_settings.applyChange(change);
        }
        format_settings = getFormatSettings(args.getContext(), user_format_settings);
    }
    else
    {
        format_settings = getFormatSettings(args.getContext());
    }

    return std::make_shared<StorageObjectStorageQueue>(
        std::move(queue_settings),
        std::move(configuration),
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        args.getContext(),
        format_settings,
        args.storage_def,
        args.mode);
}

#if USE_AWS_S3
void registerStorageS3Queue(StorageFactory & factory)
{
    factory.registerStorage(
        "S3Queue",
        [](const StorageFactory::Arguments & args)
        {
            return createQueueStorage<StorageS3Configuration>(args);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}
#endif

#if USE_AZURE_BLOB_STORAGE
void registerStorageAzureQueue(StorageFactory & factory)
{
    factory.registerStorage(
        "AzureQueue",
        [](const StorageFactory::Arguments & args)
        {
            return createQueueStorage<StorageAzureConfiguration>(args);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessType::AZURE,
        });
}
#endif
}
