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
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageDefinitions.h>
#include <Storages/DataLakes/StorageIceberg.h>
#include <Storages/DataLakes/StorageDeltaLake.h>
#include <Storages/DataLakes/StorageHudi.h>
#include <Storages/DataLakes/StoragePaimon.h>
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
    extern const DataLakeStorageSettingsString disk;
    extern DataLakeStorageSettingsDatabaseDataLakeCatalogType storage_catalog_type;
    extern DataLakeStorageSettingsString object_storage_endpoint;
    extern DataLakeStorageSettingsString storage_aws_access_key_id;
    extern DataLakeStorageSettingsString storage_aws_secret_access_key;
    extern DataLakeStorageSettingsString storage_region;
    extern DataLakeStorageSettingsString storage_aws_role_arn;
    extern DataLakeStorageSettingsString storage_aws_role_session_name;
    extern DataLakeStorageSettingsString storage_catalog_url;
    extern DataLakeStorageSettingsString storage_warehouse;
    extern DataLakeStorageSettingsString storage_catalog_credential;
    extern DataLakeStorageSettingsString storage_auth_scope;
    extern DataLakeStorageSettingsString storage_auth_header;
    extern DataLakeStorageSettingsString storage_oauth_server_uri;
    extern DataLakeStorageSettingsBool storage_oauth_server_use_request_body;
}

[[maybe_unused]] static std::shared_ptr<DataLake::ICatalog> getCatalogFromSettings(
    [[maybe_unused]] const DataLakeStorageSettings & settings,
    [[maybe_unused]] ContextPtr context,
    [[maybe_unused]] bool is_attach)
{
#if USE_AWS_S3 && USE_AVRO
    if (settings[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::GLUE)
    {
        auto catalog_parameters = DataLake::CatalogSettings{
            .storage_endpoint = settings[DataLakeStorageSetting::object_storage_endpoint].value,
            .aws_access_key_id = settings[DataLakeStorageSetting::storage_aws_access_key_id].value,
            .aws_secret_access_key = settings[DataLakeStorageSetting::storage_aws_secret_access_key].value,
            .region = settings[DataLakeStorageSetting::storage_region].value,
            .aws_role_arn = settings[DataLakeStorageSetting::storage_aws_role_arn].value,
            .aws_role_session_name = settings[DataLakeStorageSetting::storage_aws_role_session_name].value
        };

        return std::make_shared<DataLake::GlueCatalog>(
            settings[DataLakeStorageSetting::storage_catalog_url].value,
            context,
            catalog_parameters,
            /* table_engine_definition */nullptr
        );
    }
    if (settings[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::ICEBERG_REST ||
        (is_attach && settings[DataLakeStorageSetting::storage_catalog_type].value == DatabaseDataLakeCatalogType::NONE
            && !settings[DataLakeStorageSetting::storage_catalog_url].value.empty()))
    {
        return std::make_shared<DataLake::RestCatalog>(
            settings[DataLakeStorageSetting::storage_warehouse].value,
            settings[DataLakeStorageSetting::storage_catalog_url].value,
            settings[DataLakeStorageSetting::storage_catalog_credential].value,
            settings[DataLakeStorageSetting::storage_auth_scope].value,
            settings[DataLakeStorageSetting::storage_auth_header],
            settings[DataLakeStorageSetting::storage_oauth_server_uri].value,
            settings[DataLakeStorageSetting::storage_oauth_server_use_request_body].value,
            context);
    }
#endif
    return nullptr;
}

namespace
{

// LocalObjectStorage is only supported for Iceberg Datalake operations where Avro format is required. For regular file access, use FileStorage instead.
#if USE_AWS_S3 || USE_AZURE_BLOB_STORAGE || USE_HDFS || USE_AVRO

template <typename StorageType = StorageObjectStorage>
std::shared_ptr<StorageType>
createStorageObjectStorage(
    const StorageFactory::Arguments & args,
    StorageObjectStorageConfigurationPtr configuration,
    std::shared_ptr<DataLake::ICatalog> catalog = nullptr)
{
    const auto context = args.getLocalContext();
    StorageObjectStorageConfiguration::initialize(*configuration, args.engine_args, context, false, &args.table_id);

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

    /// If no catalog was provided, try getting one from the configuration
    /// (for backward compat with DataLakeConfiguration).
    if (!catalog)
        catalog = configuration->getCatalog(context, args.query.attach);

    ContextMutablePtr context_copy = Context::createCopy(args.getContext());
    Settings settings_copy = args.getLocalContext()->getSettingsCopy();
    context_copy->setSettings(settings_copy);
    return std::make_shared<StorageType>(
        configuration,
        configuration->createObjectStorage(context, /* is_readonly */ args.mode != LoadingStrictnessLevel::CREATE, std::nullopt),
        context_copy,
        args.table_id,
        args.columns,
        args.constraints,
        args.comment,
        format_settings,
        args.mode,
        catalog,
        args.query.if_not_exists,
        /* is_datalake_query*/ false,
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
        auto configuration = std::make_shared<StorageAzureConfiguration>();
        return createStorageObjectStorage(args, configuration);
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
        auto configuration = std::make_shared<StorageS3Configuration>();
        return createStorageObjectStorage(args, configuration);
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
        auto configuration = std::make_shared<StorageHDFSConfiguration>();
        return createStorageObjectStorage(args, configuration);
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

[[maybe_unused]] static DataLakeStorageSettingsPtr getDataLakeStorageSettings(const ASTStorage & storage_def)
{
    auto storage_settings = std::make_shared<DataLakeStorageSettings>();
    if (storage_def.settings)
        storage_settings->loadFromQuery(*storage_def.settings);
    return storage_settings;
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageIceberg>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageIceberg>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageIceberg>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageIceberg>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageIceberg>(args, configuration, catalog);
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

            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageDeltaLake>(args, configuration, catalog);
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

            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageDeltaLake>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageDeltaLake>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageDeltaLake>(args, configuration, catalog);
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
            auto catalog = getCatalogFromSettings(*storage_settings, args.getLocalContext(), args.query.attach);
            return createStorageObjectStorage<StorageHudi>(args, configuration, catalog);
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
