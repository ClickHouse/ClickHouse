#include "config.h"

#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>
#include <Common/Macros.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>

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
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsString s3queue_default_zookeeper_path;
    extern const SettingsBool allow_experimental_object_storage_queue_hive_partitioning;
}

namespace ObjectStorageQueueSetting
{
    extern const ObjectStorageQueueSettingsBool use_hive_partitioning;
}

template <typename Configuration>
StoragePtr createQueueStorage(const StorageFactory::Arguments & args)
{
    auto & engine_args = args.engine_args;
    if (engine_args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

    auto configuration = std::make_shared<Configuration>();
    StorageObjectStorageConfiguration::initialize(*configuration, args.engine_args, args.getContext(), false, &args.table_id);

    // Use format settings from global server context + settings from
    // the SETTINGS clause of the create query. Settings from current
    // session and user are ignored.
    std::optional<FormatSettings> format_settings;

    const bool is_attach = args.mode > LoadingStrictnessLevel::CREATE;

    if (!is_attach && args.storage_def->settings)
    {
        if (auto * path_setting = args.storage_def->settings->changes.tryGet("keeper_path"))
        {
            auto database = DatabaseCatalog::instance().tryGetDatabase(args.table_id.database_name);
            const String database_engine = database ? database->getEngineName() : "";

            bool is_on_cluster = args.getLocalContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
            bool is_replicated_database = args.getLocalContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
                database_engine == "Replicated";

            /// Allow implicit {uuid} macros only for keeper_path in ON CLUSTER queries
            /// and if UUID was explicitly passed in CREATE TABLE (like for ATTACH)
            bool allow_uuid_macro = is_on_cluster || is_replicated_database || args.query.attach || args.query.has_uuid;

            String path = path_setting->safeGet<String>();

            Macros::MacroExpansionInfo info;
            info.table_id = args.table_id;
            if (!allow_uuid_macro)
                info.table_id.uuid = UUIDHelpers::Nil;

            /// Make sure that {uuid} macro is allowed, if present.
            args.getContext()->getMacros()->expand(path, info);

            /// Actually expand all the macros except {uuid} macro.
            info.expand_special_macros_only = true;
            path = args.getContext()->getMacros()->expand(path, info);

            args.storage_def->settings->changes.setSetting("keeper_path", Field(path));
        }
    }

    auto queue_settings = std::make_unique<ObjectStorageQueueSettings>();
    if (args.storage_def->settings)
    {
        queue_settings->loadFromQuery(*args.storage_def, is_attach, args.table_id);

        Settings settings = args.getContext()->getSettingsCopy();
        settings.applyChanges(args.storage_def->settings->changes);
        format_settings = getFormatSettings(args.getContext(), settings);
    }
    else
    {
        format_settings = getFormatSettings(args.getContext());
    }

    if ((*queue_settings)[ObjectStorageQueueSetting::use_hive_partitioning])
    {
        if (!is_attach &&
            !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_object_storage_queue_hive_partitioning])
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Experimental 'use_hive_partitioning' setting is not enabled "
                            "(the setting 'allow_experimental_object_storage_queue_hive_partitioning')");
        }
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
        args.mode,
        /* keep_data_in_keeper */ false);
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
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = ObjectStorageQueueSettings::hasBuiltin,
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
            .source_access_type = AccessTypeObjects::Source::AZURE,
            .has_builtin_setting_fn = ObjectStorageQueueSettings::hasBuiltin,
        });
}
#endif
}
