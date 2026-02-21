#include <algorithm>
#include <array>
#include <memory>
#include <Databases/DataLake/DatabaseDataLake.h>
#include <Core/SettingsEnums.h>
#include <Databases/DataLake/HiveCatalog.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>


#if USE_AVRO && USE_PARQUET

#include <Access/Common/HTTPAuthenticationScheme.h>
#include <Core/Settings.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DataLake/UnityCatalog.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/DataLake/GlueCatalog.h>
#include <Databases/DataLake/PaimonRestCatalog.h>
#include <DataTypes/DataTypeString.h>

#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/StorageNull.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <Formats/FormatFactory.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Common/FailPoint.h>

namespace DB
{
namespace DatabaseDataLakeSetting
{
    extern const DatabaseDataLakeSettingsDatabaseDataLakeCatalogType catalog_type;
    extern const DatabaseDataLakeSettingsString warehouse;
    extern const DatabaseDataLakeSettingsString catalog_credential;
    extern const DatabaseDataLakeSettingsString auth_header;
    extern const DatabaseDataLakeSettingsString auth_scope;
    extern const DatabaseDataLakeSettingsString storage_endpoint;
    extern const DatabaseDataLakeSettingsString oauth_server_uri;
    extern const DatabaseDataLakeSettingsBool oauth_server_use_request_body;
    extern const DatabaseDataLakeSettingsBool vended_credentials;
    extern const DatabaseDataLakeSettingsString aws_access_key_id;
    extern const DatabaseDataLakeSettingsString aws_secret_access_key;
    extern const DatabaseDataLakeSettingsString region;
    extern const DatabaseDataLakeSettingsString aws_role_arn;
    extern const DatabaseDataLakeSettingsString aws_role_session_name;
    extern const DatabaseDataLakeSettingsString onelake_tenant_id;
    extern const DatabaseDataLakeSettingsString onelake_client_id;
    extern const DatabaseDataLakeSettingsString onelake_client_secret;
    extern const DatabaseDataLakeSettingsString dlf_access_key_id;
    extern const DatabaseDataLakeSettingsString dlf_access_key_secret;
    extern const DatabaseDataLakeSettingsString google_project_id;
    extern const DatabaseDataLakeSettingsString google_service_account;
    extern const DatabaseDataLakeSettingsString google_metadata_service;
    extern const DatabaseDataLakeSettingsString google_adc_client_id;
    extern const DatabaseDataLakeSettingsString google_adc_client_secret;
    extern const DatabaseDataLakeSettingsString google_adc_refresh_token;
    extern const DatabaseDataLakeSettingsString google_adc_quota_project_id;
    extern const DatabaseDataLakeSettingsString google_adc_credentials_file;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_database_iceberg;
    extern const SettingsBool allow_experimental_database_unity_catalog;
    extern const SettingsBool allow_experimental_database_glue_catalog;
    extern const SettingsBool allow_experimental_database_hms_catalog;
    extern const SettingsBool allow_experimental_database_paimon_rest_catalog;
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsBool database_datalake_require_metadata_access;

}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char lightweight_show_tables[];
}

DatabaseDataLake::DatabaseDataLake(
    const std::string & database_name_,
    const std::string & url_,
    const DatabaseDataLakeSettings & settings_,
    ASTPtr database_engine_definition_,
    ASTPtr table_engine_definition_,
    UUID uuid)
    : IDatabase(database_name_)
    , url(url_)
    , settings(settings_)
    , database_engine_definition(database_engine_definition_)
    , table_engine_definition(table_engine_definition_)
    , log(getLogger("DatabaseDataLake(" + database_name_ + ")"))
    , db_uuid(uuid)
{
    validateSettings();
}

void DatabaseDataLake::validateSettings()
{
    if (settings[DatabaseDataLakeSetting::catalog_type].value == DB::DatabaseDataLakeCatalogType::GLUE)
    {
        if (settings[DatabaseDataLakeSetting::region].value.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`region` setting cannot be empty for Glue Catalog. "
                "Please specify 'SETTINGS region=<region_name>' in the CREATE DATABASE query");
    }
    else if (settings[DatabaseDataLakeSetting::warehouse].value.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "`warehouse` setting cannot be empty. "
            "Please specify 'SETTINGS warehouse=<warehouse_name>' in the CREATE DATABASE query");
    }
}

std::shared_ptr<DataLake::ICatalog> DatabaseDataLake::getCatalog() const
{
    if (settings[DatabaseDataLakeSetting::catalog_type].value == DatabaseDataLakeCatalogType::NONE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unspecified catalog type");

    if (catalog_impl)
        return catalog_impl;

    auto catalog_parameters = DataLake::CatalogSettings{
        .storage_endpoint = settings[DatabaseDataLakeSetting::storage_endpoint].value,
        .aws_access_key_id = settings[DatabaseDataLakeSetting::aws_access_key_id].value,
        .aws_secret_access_key = settings[DatabaseDataLakeSetting::aws_secret_access_key].value,
        .region = settings[DatabaseDataLakeSetting::region].value,
        .aws_role_arn = settings[DatabaseDataLakeSetting::aws_role_arn].value,
        .aws_role_session_name = settings[DatabaseDataLakeSetting::aws_role_session_name].value,
    };

    switch (settings[DatabaseDataLakeSetting::catalog_type].value)
    {
        case DB::DatabaseDataLakeCatalogType::ICEBERG_REST:
        {
            catalog_impl = std::make_shared<DataLake::RestCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::catalog_credential].value,
                settings[DatabaseDataLakeSetting::auth_scope].value,
                settings[DatabaseDataLakeSetting::auth_header],
                settings[DatabaseDataLakeSetting::oauth_server_uri].value,
                settings[DatabaseDataLakeSetting::oauth_server_use_request_body].value,
                Context::getGlobalContextInstance());
            break;
        }
        case DB::DatabaseDataLakeCatalogType::ICEBERG_ONELAKE:
        {
            catalog_impl = std::make_shared<DataLake::OneLakeCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::onelake_tenant_id].value,
                settings[DatabaseDataLakeSetting::onelake_client_id].value,
                settings[DatabaseDataLakeSetting::onelake_client_secret].value,
                settings[DatabaseDataLakeSetting::auth_scope].value,
                settings[DatabaseDataLakeSetting::oauth_server_uri].value,
                settings[DatabaseDataLakeSetting::oauth_server_use_request_body].value,
                Context::getGlobalContextInstance());
            break;
        }
        case DB::DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE:
        {
            std::string google_project_id = settings[DatabaseDataLakeSetting::google_project_id].value;
            std::string google_service_account = settings[DatabaseDataLakeSetting::google_service_account].value;
            std::string google_metadata_service = settings[DatabaseDataLakeSetting::google_metadata_service].value;
            std::string google_adc_client_id = settings[DatabaseDataLakeSetting::google_adc_client_id].value;
            std::string google_adc_client_secret = settings[DatabaseDataLakeSetting::google_adc_client_secret].value;
            std::string google_adc_refresh_token = settings[DatabaseDataLakeSetting::google_adc_refresh_token].value;
            std::string google_adc_quota_project_id = settings[DatabaseDataLakeSetting::google_adc_quota_project_id].value;

            if (settings[DatabaseDataLakeSetting::google_adc_credentials_file].changed)
            {
                try
                {
                    const std::string & credentials_file_path = settings[DatabaseDataLakeSetting::google_adc_credentials_file].value;
                    DB::ReadBufferFromFile file_buf(credentials_file_path);
                    std::string json_str;
                    DB::readStringUntilEOF(json_str, file_buf);

                    Poco::JSON::Parser parser;
                    Poco::Dynamic::Var json = parser.parse(json_str);
                    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

                    if (object->has("type"))
                    {
                        String type = object->get("type").extract<String>();
                        if (type != "authorized_user")
                        {
                            throw DB::Exception(
                                DB::ErrorCodes::BAD_ARGUMENTS,
                                "Unsupported credentials type '{}' in Google ADC credentials file. Expected 'authorized_user'",
                                type);
                        }
                    }

                    if (google_adc_client_id.empty() && object->has("client_id"))
                        google_adc_client_id = object->get("client_id").extract<String>();
                    if (google_adc_client_secret.empty() && object->has("client_secret"))
                        google_adc_client_secret = object->get("client_secret").extract<String>();
                    if (google_adc_refresh_token.empty() && object->has("refresh_token"))
                        google_adc_refresh_token = object->get("refresh_token").extract<String>();
                    if (google_adc_quota_project_id.empty() && object->has("quota_project_id"))
                        google_adc_quota_project_id = object->get("quota_project_id").extract<String>();
                    if (google_project_id.empty() && object->has("project_id"))
                        google_project_id = object->get("project_id").extract<String>();
                }
                catch (const DB::Exception & e)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::BAD_ARGUMENTS,
                        "Failed to load Google ADC credentials from file '{}': {}",
                        settings[DatabaseDataLakeSetting::google_adc_credentials_file].value,
                        e.message());
                }
            }

            catalog_impl = std::make_shared<DataLake::BigLakeCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                google_project_id,
                google_service_account,
                google_metadata_service,
                google_adc_client_id,
                google_adc_client_secret,
                google_adc_refresh_token,
                google_adc_quota_project_id,
                Context::getGlobalContextInstance());
            break;
        }
        case DB::DatabaseDataLakeCatalogType::UNITY:
        {
            catalog_impl = std::make_shared<DataLake::UnityCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::catalog_credential].value,
                Context::getGlobalContextInstance());
            break;
        }

        case DB::DatabaseDataLakeCatalogType::GLUE:
        {
            catalog_impl = std::make_shared<DataLake::GlueCatalog>(
                url,
                Context::getGlobalContextInstance(),
                catalog_parameters,
                table_engine_definition);
            break;
        }
        case DB::DatabaseDataLakeCatalogType::ICEBERG_HIVE:
        {
#if USE_HIVE
            catalog_impl = std::make_shared<DataLake::HiveCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                Context::getGlobalContextInstance());
            break;
#else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot use 'hive' database engine: ClickHouse was compiled without USE_HIVE built option");
#endif
        }
        case DB::DatabaseDataLakeCatalogType::NONE:
        {
            catalog_impl = nullptr;
            break;
        }
        case DB::DatabaseDataLakeCatalogType::PAIMON_REST:
        {
            if (!settings[DatabaseDataLakeSetting::catalog_credential].value.empty())
            {
                catalog_impl = std::make_shared<DataLake::PaimonRestCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                DataLake::PaimonToken(settings[DatabaseDataLakeSetting::catalog_credential].value),
                settings[DatabaseDataLakeSetting::region].value,
                Context::getGlobalContextInstance());
            }
            else if (!settings[DatabaseDataLakeSetting::dlf_access_key_id].value.empty()
                && !settings[DatabaseDataLakeSetting::dlf_access_key_secret].value.empty()
                && !settings[DatabaseDataLakeSetting::region].value.empty())
            {
                catalog_impl = std::make_shared<DataLake::PaimonRestCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                DataLake::PaimonToken(settings[DatabaseDataLakeSetting::dlf_access_key_id].value, settings[DatabaseDataLakeSetting::dlf_access_key_secret].value),
                settings[DatabaseDataLakeSetting::region].value,
                Context::getGlobalContextInstance());
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Paimon catalog requires either catalog_credential or (dlf_access_key_id, dlf_access_key_secret and region)");
            }
            break;
        }
    }
    return catalog_impl;
}

std::shared_ptr<StorageObjectStorageConfiguration> DatabaseDataLake::getConfiguration(
    DatabaseDataLakeStorageType type,
    DataLakeStorageSettingsPtr storage_settings) const
{
    /// TODO: add tests for azure, local storage types.

    auto catalog = getCatalog();
    switch (catalog->getCatalogType())
    {
        case DatabaseDataLakeCatalogType::ICEBERG_ONELAKE:
        {
            switch (type)
            {
#if USE_AZURE_BLOB_STORAGE
                case DB::DatabaseDataLakeStorageType::Azure:
                {
                    return std::make_shared<StorageAzureIcebergConfiguration>(storage_settings);
                }
#endif
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Iceberg OneLake catalog",
                                    type);
            }
        }
        case DatabaseDataLakeCatalogType::ICEBERG_HIVE:
        case DatabaseDataLakeCatalogType::ICEBERG_REST:
        case DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                case DB::DatabaseDataLakeStorageType::Azure:
                {
                    return std::make_shared<StorageAzureIcebergConfiguration>(storage_settings);
                }
#endif
#if USE_HDFS
                case DB::DatabaseDataLakeStorageType::HDFS:
                {
                    return std::make_shared<StorageHDFSIcebergConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Local:
                {
                    return std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                }
                /// Fake storage in case when catalog store not only
                /// primary-type tables (DeltaLake or Iceberg), but for
                /// examples something else like INFORMATION_SCHEMA.
                /// Such tables are unreadable, but at least we can show
                /// them in SHOW CREATE TABLE, as well we can show their
                /// schema.
                /// We use local as substitution for fake because it has 0
                /// dependencies and the most lightweight
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                }
#if !USE_AWS_S3 || !USE_AZURE_BLOB_STORAGE || !USE_HDFS
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Iceberg Rest catalog",
                                    type);
#endif
            }
        }
        case DatabaseDataLakeCatalogType::UNITY:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3DeltaLakeConfiguration>(storage_settings);
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                case DB::DatabaseDataLakeStorageType::Azure:
                {
                    return std::make_shared<StorageAzureDeltaLakeConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Local:
                {
                    return std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
                }
                /// Fake storage in case when catalog store not only
                /// primary-type tables (DeltaLake or Iceberg), but for
                /// examples something else like INFORMATION_SCHEMA.
                /// Such tables are unreadable, but at least we can show
                /// them in SHOW CREATE TABLE, as well we can show their
                /// schema.
                /// We use local as substitution for fake because it has 0
                /// dependencies and the most lightweight
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalDeltaLakeConfiguration>(storage_settings);
                }
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Unity catalog",
                                    type);
            }
        }
        case DatabaseDataLakeCatalogType::GLUE:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3IcebergConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalIcebergConfiguration>(storage_settings);
                }
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Glue catalog",
                                    type);
            }
        }
        case DatabaseDataLakeCatalogType::PAIMON_REST:
        {
            switch (type)
            {
#if USE_AWS_S3
                case DB::DatabaseDataLakeStorageType::S3:
                {
                    return std::make_shared<StorageS3PaimonConfiguration>(storage_settings);
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                case DB::DatabaseDataLakeStorageType::Azure:
                {
                    return std::make_shared<StorageAzurePaimonConfiguration>(storage_settings);
                }
#endif
#if USE_HDFS
                case DB::DatabaseDataLakeStorageType::HDFS:
                {
                    return std::make_shared<StorageHDFSPaimonConfiguration>(storage_settings);
                }
#endif
                case DB::DatabaseDataLakeStorageType::Local:
                {
                    return std::make_shared<StorageLocalPaimonConfiguration>(storage_settings);
                }
                case DB::DatabaseDataLakeStorageType::Other:
                {
                    return std::make_shared<StorageLocalPaimonConfiguration>(storage_settings);
                }
#if !USE_AWS_S3 || !USE_AZURE_BLOB_STORAGE || !USE_HDFS
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Server does not contain support for storage type {} for Iceberg Rest catalog",
                                    type);
#endif
            }
        }
        case DatabaseDataLakeCatalogType::NONE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unspecified catalog type");
    }
}

std::string DatabaseDataLake::getStorageEndpointForTable(const DataLake::TableMetadata & table_metadata) const
{
    auto endpoint_from_settings = settings[DatabaseDataLakeSetting::storage_endpoint].value;
    if (endpoint_from_settings.empty())
        return table_metadata.getLocation();
    return table_metadata.getLocationWithEndpoint(endpoint_from_settings);
}

bool DatabaseDataLake::empty() const
{
    return getCatalog()->empty();
}

bool DatabaseDataLake::isTableExist(const String & name, ContextPtr /* context_ */) const
{
    const auto [namespace_name, table_name] = DataLake::parseTableName(name);
    return getCatalog()->existsTable(namespace_name, table_name);
}

StoragePtr DatabaseDataLake::tryGetTable(const String & name, ContextPtr context_)  const
{
    return tryGetTableImpl(name, context_, false, false);
}

StoragePtr DatabaseDataLake::tryGetTableImpl(const String & name, ContextPtr context_, bool lightweight, bool ignore_if_not_iceberg) const
{
    auto catalog = getCatalog();
    auto table_metadata = DataLake::TableMetadata().withSchema().withLocation().withDataLakeSpecificProperties();

    /// This is added to test that lightweight queries like 'SHOW TABLES' dont end up fetching the table
    fiu_do_on(FailPoints::lightweight_show_tables,
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    });

    const bool with_vended_credentials = settings[DatabaseDataLakeSetting::vended_credentials].value;
    if (with_vended_credentials)
        table_metadata = table_metadata.withStorageCredentials();

    auto [namespace_name, table_name] = DataLake::parseTableName(name);

    if (!catalog->tryGetTableMetadata(namespace_name, table_name, table_metadata))
        return nullptr;
    if (ignore_if_not_iceberg && !table_metadata.isDefaultReadableTable())
        return nullptr;

    if (!lightweight && !table_metadata.isDefaultReadableTable())
    {
        throw Exception::createRuntime(ErrorCodes::DATALAKE_DATABASE_ERROR, table_metadata.getReasonWhyTableIsUnreadable());
    }

    /// Take database engine definition AST as base.
    ASTStorage * storage = table_engine_definition->as<ASTStorage>();
    ASTs args = storage->engine->arguments->children;

    if (table_metadata.hasLocation())
    {
        /// Replace Iceberg Catalog endpoint with storage path endpoint of requested table.
        auto table_endpoint = getStorageEndpointForTable(table_metadata);
        LOG_DEBUG(log, "Table endpoint {}", table_endpoint);
        if (table_endpoint.starts_with(DataLake::FILE_PATH_PREFIX))
            table_endpoint = table_endpoint.substr(DataLake::FILE_PATH_PREFIX.length());
        if (args.empty())
            args.emplace_back(make_intrusive<ASTLiteral>(table_endpoint));
        else
            args[0] = make_intrusive<ASTLiteral>(table_endpoint);
    }

    /// We either fetch storage credentials from catalog
    /// or get storage credentials from database engine arguments
    /// in CREATE query (e.g. in `args`).
    /// Vended credentials can be disabled in catalog itself,
    /// so we have a separate setting to know whether we should even try to fetch them.
    if (args.size() == 1)
    {
        std::array<DatabaseDataLakeCatalogType, 3> vended_credentials_catalogs = {DatabaseDataLakeCatalogType::ICEBERG_ONELAKE, DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE, DatabaseDataLakeCatalogType::PAIMON_REST};
        if (table_metadata.hasStorageCredentials())
        {
            LOG_DEBUG(log, "Getting credentials");
            auto storage_credentials = table_metadata.getStorageCredentials();
            if (storage_credentials)
            {
                LOG_DEBUG(log, "Has credentials");
                storage_credentials->addCredentialsToEngineArgs(args);
            }
            else
            {
                LOG_DEBUG(log, "Has no credentials");
            }
        }
        else if (!lightweight && table_metadata.requiresCredentials() && std::find(vended_credentials_catalogs.begin(), vended_credentials_catalogs.end(), catalog->getCatalogType()) == vended_credentials_catalogs.end())
        {
            throw Exception(
               ErrorCodes::BAD_ARGUMENTS,
               "Either vended credentials need to be enabled "
               "or storage credentials need to be specified in database engine arguments in CREATE query");
        }
    }

    LOG_TEST(log, "Using table endpoint: {}", args[0]->as<ASTLiteral>()->value.safeGet<String>());

    const auto columns = ColumnsDescription(table_metadata.getSchema());

    DatabaseDataLakeStorageType storage_type = DatabaseDataLakeStorageType::Other;
    auto storage_type_from_catalog = catalog->getStorageType();
    if (storage_type_from_catalog.has_value())
    {
        storage_type = storage_type_from_catalog.value();
    }
    else
    {
        if (table_metadata.hasLocation() || !lightweight)
            storage_type = table_metadata.getStorageType();
    }

    auto storage_settings = std::make_shared<DataLakeStorageSettings>();
    storage_settings->loadFromSettingsChanges(settings.allChanged());

    if (auto table_specific_properties = table_metadata.getDataLakeSpecificProperties();
        table_specific_properties.has_value())
    {
        auto metadata_location = table_specific_properties->iceberg_metadata_file_location;
        if (!metadata_location.empty())
        {
            metadata_location = table_metadata.getMetadataLocation(metadata_location);
        }

        (*storage_settings)[DB::DataLakeStorageSetting::iceberg_metadata_file_path] = metadata_location;
    }

    const auto configuration = getConfiguration(storage_type, storage_settings);

    /// HACK: Hacky-hack to enable lazy load
    ContextMutablePtr context_copy = Context::createCopy(context_);
    Settings settings_copy = context_copy->getSettingsCopy();
    settings_copy[Setting::use_hive_partitioning] = false;
    context_copy->setSettings(settings_copy);

    if (catalog->getCatalogType() == DatabaseDataLakeCatalogType::ICEBERG_ONELAKE)
    {
#if USE_AZURE_BLOB_STORAGE
        auto azure_configuration = std::static_pointer_cast<StorageAzureIcebergConfiguration>(configuration);
        if (!azure_configuration)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is not azure type for one lake catalog");
        auto rest_catalog = std::static_pointer_cast<DataLake::OneLakeCatalog>(catalog);
        if (!rest_catalog)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Catalog is not equals to one lake");
        azure_configuration->setInitializationAsOneLake(
            rest_catalog->getClientId(),
            rest_catalog->getClientSecret(),
            rest_catalog->getTenantId()
        );
#else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Server does not contain support for storage type Azure for Iceberg OneLake catalog");
#endif
    }

    /// with_table_structure = false: because there will be
    /// no table structure in table definition AST.
    StorageObjectStorageConfiguration::initialize(*configuration, args, context_copy, /* with_table_structure */false);

    const auto & query_settings = context_->getSettingsRef();

    const auto parallel_replicas_cluster_name = query_settings[Setting::cluster_for_parallel_replicas].toString();
    const auto can_use_parallel_replicas = !parallel_replicas_cluster_name.empty()
        && query_settings[Setting::parallel_replicas_for_cluster_engines]
        && context_->canUseTaskBasedParallelReplicas()
        && !context_->isDistributed();

    const auto is_secondary_query = context_->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;

    if (can_use_parallel_replicas && !is_secondary_query)
    {
        auto storage_id = StorageID(getDatabaseName(), name);
        auto storage_cluster = std::make_shared<StorageObjectStorageCluster>(
            parallel_replicas_cluster_name,
            configuration,
            configuration->createObjectStorage(context_copy, /* is_readonly */ false, catalog->getCredentialsConfigurationCallback(storage_id)),
            storage_id,
            columns,
            ConstraintsDescription{},
            nullptr,
            context_,
            /// Use is_table_function = true,
            /// because this table is actually stateless like a table function.
            /* is_table_function */true);

        storage_cluster->startup();
        return storage_cluster;
    }

    bool can_use_distributed_iterator =
        context_->getClientInfo().collaborate_with_initiator &&
        can_use_parallel_replicas;

    return std::make_shared<StorageObjectStorage>(
        configuration,
        configuration->createObjectStorage(context_copy, /* is_readonly */ false, catalog->getCredentialsConfigurationCallback(StorageID(getDatabaseName(), name))),
        context_copy,
        StorageID(getDatabaseName(), name),
        /* columns */columns,
        /* constraints */ConstraintsDescription{},
        /* comment */"",
        getFormatSettings(context_copy),
        LoadingStrictnessLevel::CREATE,
        getCatalog(),
        /* if_not_exists*/true,
        /* is_datalake_query*/true,
        /* distributed_processing */can_use_distributed_iterator,
        /* partition_by */nullptr,
        /* order_by */nullptr,
        /// Use is_table_function = true,
        /// because this table is actually stateless like a table function.
        /* is_table_function */true,
        /* lazy_init */true);
}

void DatabaseDataLake::dropTable( /// NOLINT
    ContextPtr context_,
    const String & name,
    bool /*sync*/)
{
    auto table = tryGetTable(name, context_);
    if (table)
        table->drop();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot drop table {} because it does not exist", name);
}

DatabaseTablesIteratorPtr DatabaseDataLake::getTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded) const
{
    Tables tables;
    auto catalog = getCatalog();
    DB::Names iceberg_tables;

    /// Do not throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some datalake error.
    try
    {
        iceberg_tables = catalog->getTables();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    auto & pool = Context::getGlobalContextInstance()->getIcebergCatalogThreadpool();

    std::vector<std::shared_ptr<std::promise<StoragePtr>>> promises;
    std::vector<std::future<StoragePtr>> futures;
    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        try
        {
            promises.emplace_back(std::make_shared<std::promise<StoragePtr>>());
            futures.emplace_back(promises.back()->get_future());

            pool.scheduleOrThrow(
                [this, table_name, skip_not_loaded, context_, promise=promises.back()]() mutable
                {
                    StoragePtr storage = nullptr;
                    try
                    {
                        LOG_INFO(log, "Get table information for table {}", table_name);
                        storage = tryGetTableImpl(table_name, context_, false, skip_not_loaded);
                    }
                    catch (...)
                    {
                        if (context_->getSettingsRef()[Setting::database_datalake_require_metadata_access])
                        {
                            auto error_code = getCurrentExceptionCode();
                            auto error_message = getCurrentExceptionMessage(true, false, true, true);
                            auto enhanced_message = fmt::format(
                                "Received error {} while fetching table metadata for existing table '{}'. "
                                "If you want this error to be ignored, use database_datalake_require_metadata_access=0. Error: {}",
                                error_code,
                                table_name,
                                error_message);
                            promise->set_exception(std::make_exception_ptr(Exception::createRuntime(
                                error_code,
                                enhanced_message)));
                            return;
                        }
                        else
                            tryLogCurrentException(log, fmt::format("Ignoring table {}", table_name));
                    }
                    promise->set_value(storage);
                });
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to schedule task");
            pool.wait();

            throw;
        }
    }

    for (const auto & future : futures)
        future.wait();

    size_t future_index = 0;
    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;

        [[maybe_unused]] bool inserted = tables.emplace(table_name, futures[future_index].get()).second;
        chassert(inserted);
        future_index++;
    }
    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}

std::vector<LightWeightTableDetails> DatabaseDataLake::getLightweightTablesIterator(
    ContextPtr /*context_*/,
    const FilterByNameFunction & filter_by_table_name,
    bool /*skip_not_loaded*/) const
{
    auto catalog = getCatalog();
    DB::Names iceberg_tables;
    std::vector<LightWeightTableDetails> result;

    /// Do not throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some datalake error.
    try
    {
        iceberg_tables = catalog->getTables();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    for (const auto & table_name : iceberg_tables)
    {
        if (filter_by_table_name && !filter_by_table_name(table_name))
            continue;
        result.emplace_back(table_name);
    }

    return result;
}

ASTPtr DatabaseDataLake::getCreateDatabaseQueryImpl() const
{
    const auto & create_query = make_intrusive<ASTCreateQuery>();
    create_query->setDatabase(database_name);
    create_query->set(create_query->storage, database_engine_definition);
    create_query->uuid = db_uuid;
    return create_query;
}

ASTPtr DatabaseDataLake::getCreateTableQueryImpl(
    const String & name,
    ContextPtr /* context_ */,
    bool throw_on_error) const
{
    auto catalog = getCatalog();
    auto table_metadata = DataLake::TableMetadata().withLocation().withSchema();

    const auto [namespace_name, table_name] = DataLake::parseTableName(name);

    if (!catalog->tryGetTableMetadata(namespace_name, table_name, table_metadata))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Table `{}` doesn't exist", name);
        return {};
    }

    auto create_table_query = make_intrusive<ASTCreateQuery>();
    auto table_storage_define = table_engine_definition->clone();

    auto * storage = table_storage_define->as<ASTStorage>();
    storage->engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
    if (!table_metadata.isDefaultReadableTable())
        storage->engine->name = DataLake::FAKE_TABLE_ENGINE_NAME_FOR_UNREADABLE_TABLES;

    storage->settings = {};

    create_table_query->set(create_table_query->storage, table_storage_define);

    auto columns_declare_list = make_intrusive<ASTColumns>();
    auto columns_expression_list = make_intrusive<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_table_query->set(create_table_query->columns_list, columns_declare_list);

    create_table_query->setTable(name);
    create_table_query->setDatabase(getDatabaseName());

    for (const auto & column_type_and_name : table_metadata.getSchema())
    {
        LOG_DEBUG(log, "Processing column {}", column_type_and_name.name);
        const auto column_declaration = make_intrusive<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->setType(makeASTDataType(column_type_and_name.type->getName()));
        columns_expression_list->children.emplace_back(column_declaration);
    }

    auto storage_engine_arguments = storage->engine->arguments;
    if (table_metadata.isDefaultReadableTable())
    {
        auto table_endpoint = getStorageEndpointForTable(table_metadata);
        if (table_endpoint.starts_with(DataLake::FILE_PATH_PREFIX))
            table_endpoint = table_endpoint.substr(DataLake::FILE_PATH_PREFIX.length());

        LOG_DEBUG(log, "Table endpoint {}", table_endpoint);
        if (storage_engine_arguments->children.empty())
            storage_engine_arguments->children.emplace_back();

        storage_engine_arguments->children[0] = make_intrusive<ASTLiteral>(table_endpoint);
    }
    else
    {
        storage_engine_arguments->children.clear();
    }

    return create_table_query;
}

void registerDatabaseDataLake(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        const auto * database_engine_define = args.create_query.storage;
        const auto & database_engine_name = args.engine_name;

        DatabaseDataLakeSettings database_settings;
        if (database_engine_define->settings)
            database_settings.loadFromQuery(*database_engine_define, args.create_query.attach);

        auto catalog_type = database_settings[DB::DatabaseDataLakeSetting::catalog_type].value;
        /// Glue catalog is one per region, so it's fully identified by aws keys and region
        /// There is no URL you need to provide in constructor, even if we would want it
        ///  will be something like https://aws.amazon.com.
        ///
        ///  NOTE: it's still possible to provide endpoint argument for Glue. It's used for fake
        ///  mock glue catalog in tests only.
        bool requires_arguments = catalog_type != DatabaseDataLakeCatalogType::GLUE;

        const ASTFunction * function_define = database_engine_define->engine;

        ASTs engine_args;
        if (requires_arguments && !function_define->arguments)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);
        }

        if (function_define->arguments)
        {
            engine_args = function_define->arguments->children;
            if (requires_arguments && engine_args.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine `{}` must have arguments", database_engine_name);
        }

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.context);

        std::string url;
        if (!engine_args.empty())
            url = engine_args[0]->as<ASTLiteral>()->value.safeGet<String>();

        auto engine_for_tables = database_engine_define->clone();
        ASTFunction * engine_func = engine_for_tables->as<ASTStorage &>().engine;
        if (engine_func->arguments == nullptr)
        {
            engine_func->arguments = make_intrusive<ASTExpressionList>();
        }

        switch (catalog_type)
        {
            case DatabaseDataLakeCatalogType::ICEBERG_ONELAKE:
            case DatabaseDataLakeCatalogType::ICEBERG_REST:
            case DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_iceberg])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Iceberg Rest catalog is beta. "
                                    "To allow its usage, enable setting allow_database_iceberg");
                }

                engine_func->name = "Iceberg";
                break;
            }
            case DatabaseDataLakeCatalogType::GLUE:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_glue_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Glue catalog is beta. "
                                    "To allow its usage, enable setting allow_database_glue_catalog");
                }

                engine_func->name = "Iceberg";
                break;
            }
            case DatabaseDataLakeCatalogType::UNITY:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_unity_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DataLake database with Unity catalog catalog is beta. "
                                    "To allow its usage, enable setting allow_database_unity_catalog");
                }

                engine_func->name = "DeltaLake";
                break;
            }
            case DatabaseDataLakeCatalogType::ICEBERG_HIVE:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_hms_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Iceberg Hive catalog is experimental. "
                                    "To allow its usage, enable setting allow_experimental_database_hms_catalog");
                }

                engine_func->name = "Iceberg";
                break;
            }
            case DatabaseDataLakeCatalogType::PAIMON_REST:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_paimon_rest_catalog])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Paimon Rest catalog is experimental. "
                                    "To allow its usage, enable setting allow_experimental_database_paimon_rest_catalog");
                }

                engine_func->name = "Paimon";
                break;
            }
            case DatabaseDataLakeCatalogType::NONE:
                break;
        }

        return std::make_shared<DatabaseDataLake>(
            args.database_name,
            url,
            database_settings,
            database_engine_define->clone(),
            std::move(engine_for_tables),
            args.uuid);
    };
    factory.registerDatabase("DataLakeCatalog", create_fn, { .supports_arguments = true, .supports_settings = true });
}

}

#endif
