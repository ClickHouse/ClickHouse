#include <Core/FormatFactorySettings.h>
#include <Core/Settings.h>
#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/GlueCatalog.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/StorageDataLake.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StorageIceberg.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
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

namespace Setting
{
    extern const SettingsBool write_full_path_in_iceberg_metadata;
}

namespace DataLakeStorageSetting
{
    extern DataLakeStorageSettingsString disk;
    extern DataLakeStorageSettingsDatabaseDataLakeCatalogType storage_catalog_type;
    extern DataLakeStorageSettingsString storage_catalog_url;
    extern DataLakeStorageSettingsString storage_warehouse;
    extern DataLakeStorageSettingsString storage_catalog_credential;
    extern DataLakeStorageSettingsString storage_auth_scope;
    extern DataLakeStorageSettingsString storage_auth_header;
    extern DataLakeStorageSettingsString storage_oauth_server_uri;
    extern DataLakeStorageSettingsBool storage_oauth_server_use_request_body;
    extern DataLakeStorageSettingsString object_storage_endpoint;
    extern DataLakeStorageSettingsString storage_aws_access_key_id;
    extern DataLakeStorageSettingsString storage_aws_secret_access_key;
    extern DataLakeStorageSettingsString storage_region;
    extern DataLakeStorageSettingsString storage_aws_role_arn;
    extern DataLakeStorageSettingsString storage_aws_role_session_name;
}

namespace
{

[[maybe_unused]] std::shared_ptr<DataLake::ICatalog> getCatalogFromSettings(
    const DataLakeStorageSettings & dl_settings, ContextPtr context, bool is_attach)
{
#if USE_AWS_S3 && USE_AVRO
    if (dl_settings[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::GLUE)
    {
        auto catalog_parameters = DataLake::CatalogSettings{
            .storage_endpoint = dl_settings[DataLakeStorageSetting::object_storage_endpoint].value,
            .aws_access_key_id = dl_settings[DataLakeStorageSetting::storage_aws_access_key_id].value,
            .aws_secret_access_key = dl_settings[DataLakeStorageSetting::storage_aws_secret_access_key].value,
            .region = dl_settings[DataLakeStorageSetting::storage_region].value,
            .aws_role_arn = dl_settings[DataLakeStorageSetting::storage_aws_role_arn].value,
            .aws_role_session_name = dl_settings[DataLakeStorageSetting::storage_aws_role_session_name].value
        };

        return std::make_shared<DataLake::GlueCatalog>(
            dl_settings[DataLakeStorageSetting::storage_catalog_url].value,
            context,
            catalog_parameters,
            /* table_engine_definition */nullptr);
    }
    if (dl_settings[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::ICEBERG_REST
        || (is_attach && dl_settings[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::NONE
            && !dl_settings[DataLakeStorageSetting::storage_catalog_url].value.empty()))
    {
        return std::make_shared<DataLake::RestCatalog>(
            dl_settings[DataLakeStorageSetting::storage_warehouse].value,
            dl_settings[DataLakeStorageSetting::storage_catalog_url].value,
            dl_settings[DataLakeStorageSetting::storage_catalog_credential].value,
            dl_settings[DataLakeStorageSetting::storage_auth_scope].value,
            dl_settings[DataLakeStorageSetting::storage_auth_header],
            dl_settings[DataLakeStorageSetting::storage_oauth_server_uri].value,
            dl_settings[DataLakeStorageSetting::storage_oauth_server_use_request_body].value,
            context);
    }
#endif
    UNUSED(dl_settings, context, is_attach);
    return nullptr;
}

#if USE_AWS_S3 || USE_AZURE_BLOB_STORAGE || USE_HDFS || USE_AVRO

template <typename DataLakeMetadata>
std::shared_ptr<StorageDataLake<DataLakeMetadata>>
createStorageDataLake(const StorageFactory::Arguments & args, StorageObjectStorageConfigurationPtr configuration, DataLakeStorageSettingsPtr storage_settings)
{
    const auto context = args.getLocalContext();
    const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
        ? (*storage_settings)[DataLakeStorageSetting::disk].value
        : "";
    StorageObjectStorageConfiguration::initialize(*configuration, args.engine_args, context, false, &args.table_id, disk_name);
    if (configuration->format == "auto")
        configuration->format = "Parquet";

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
    return std::make_shared<StorageDataLake<DataLakeMetadata>>(
        configuration,
        configuration->createObjectStorage(context, /* is_readonly */ args.mode != LoadingStrictnessLevel::CREATE, std::nullopt),
        context_copy,
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        args.mode,
        storage_settings,
        storage_settings ? getCatalogFromSettings(*storage_settings, context, args.query.attach) : nullptr,
        /* distributed_processing */ false,
        partition_by,
        order_by);
}

#endif

[[maybe_unused]] static DataLakeStorageSettingsPtr getDataLakeStorageSettings(const ASTStorage & storage_def)
{
    auto storage_settings = std::make_shared<DataLakeStorageSettings>();
    if (storage_def.settings)
        storage_settings->loadFromQuery(*storage_def.settings);
    return storage_settings;
}

}

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

void registerStorageIceberg(StorageFactory & factory)
{
    factory.registerStorage(
        IcebergDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
#if USE_AWS_S3
                    case ObjectStorageType::S3:
                        configuration = std::make_shared<StorageS3Configuration>();
                        break;
#endif
#if USE_AZURE_BLOB_STORAGE
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzureConfiguration>();
                        break;
#endif
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalConfiguration>();
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unsupported disk type for {}: {}",
                            IcebergDefinition::storage_engine_name,
                            disk->getObjectStorage()->getType());
                }
            }
            else
#if USE_AWS_S3
                configuration = std::make_shared<StorageS3Configuration>();
#endif
            if (configuration == nullptr)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "This storage configuration is not available at this build");
            }
            return createStorageDataLake<IcebergMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            /// This source access type is probably a bug which was overlooked and we do not know how to fix it simply, so we keep it as it is.
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#if USE_AWS_S3
    factory.registerStorage(
        IcebergS3Definition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                case ObjectStorageType::S3:
                    configuration = std::make_shared<StorageS3Configuration>();
                    break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", IcebergS3Definition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3Configuration>();
            return createStorageDataLake<IcebergMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#    endif
#    if USE_AZURE_BLOB_STORAGE
    factory.registerStorage(
        IcebergAzureDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                case ObjectStorageType::Azure:
                    configuration = std::make_shared<StorageAzureConfiguration>();
                    break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", IcebergAzureDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageAzureConfiguration>();
            return createStorageDataLake<IcebergMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::AZURE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#    endif
#    if USE_HDFS
    factory.registerStorage(
        IcebergHDFSDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            auto configuration = std::make_shared<StorageHDFSConfiguration>();
            return createStorageDataLake<IcebergMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::HDFS,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#    endif
    factory.registerStorage(
        IcebergLocalDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalConfiguration>();
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", IcebergLocalDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageLocalConfiguration>();
            return createStorageDataLake<IcebergMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_sort_order = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::FILE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
}

#endif


#if USE_PARQUET && USE_DELTA_KERNEL_RS
void registerStorageDeltaLake(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        DeltaLakeDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::S3:
                    {
                        configuration = std::make_shared<StorageS3Configuration>();
                        break;
                    }
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzureConfiguration>();
                        break;
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalConfiguration>();
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3Configuration>();

            return createStorageDataLake<DeltaLakeMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
    factory.registerStorage(
        DeltaLakeS3Definition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                case ObjectStorageType::S3:
                {
                    configuration = std::make_shared<StorageS3Configuration>();
                    break;
                }
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeS3Definition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageS3Configuration>();

            return createStorageDataLake<DeltaLakeMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#    endif
#    if USE_AZURE_BLOB_STORAGE
    factory.registerStorage(
        DeltaLakeAzureDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Azure:
                        configuration = std::make_shared<StorageAzureConfiguration>();
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeAzureDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageAzureConfiguration>();
            return createStorageDataLake<DeltaLakeMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::AZURE,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#    endif
    factory.registerStorage(
        DeltaLakeLocalDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            const auto disk_name = storage_settings && (*storage_settings)[DataLakeStorageSetting::disk].changed
                ? (*storage_settings)[DataLakeStorageSetting::disk].value
                : "";

            StorageObjectStorageConfigurationPtr configuration;
            if (!disk_name.empty())
            {
                auto disk = Context::getGlobalContextInstance()->getDisk(disk_name);
                switch (disk->getObjectStorage()->getType())
                {
                    case ObjectStorageType::Local:
                        configuration = std::make_shared<StorageLocalConfiguration>();
                        break;
                    default:
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported disk type for {}: {}", DeltaLakeLocalDefinition::storage_engine_name, disk->getObjectStorage()->getType());
                }
            }
            else
                configuration = std::make_shared<StorageLocalConfiguration>();
            return createStorageDataLake<DeltaLakeMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::FILE,
            .has_builtin_setting_fn = StorageObjectStorageSettings::hasBuiltin,
        });
}
#endif

void registerStorageHudi(StorageFactory & factory)
{
#if USE_AWS_S3
    factory.registerStorage(
        HudiDefinition::storage_engine_name,
        [&](const StorageFactory::Arguments & args)
        {
            const auto storage_settings = getDataLakeStorageSettings(*args.storage_def);
            auto configuration = std::make_shared<StorageS3Configuration>();
            return createStorageDataLake<HudiMetadata>(args, configuration, storage_settings);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::S3,
            .has_builtin_setting_fn = DataLakeStorageSettings::hasBuiltin,
        });
#endif
    UNUSED(factory);
}
}
