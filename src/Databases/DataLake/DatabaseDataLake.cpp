#include "config.h"

#include <algorithm>
#include <array>
#include <memory>
#include <Databases/DataLake/DatabaseDataLake.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <Databases/DataLake/HiveCatalog.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/StaticStorageCredentials.h>
#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/URI.h>


#if USE_AVRO && USE_PARQUET

#include <Core/Settings.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/DataLake/UnityCatalog.h>
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/DataLake/GlueCatalog.h>
#include <Databases/DataLake/PaimonRestCatalog.h>
#if USE_AWS_S3 && USE_SSL
#include <Databases/DataLake/S3TablesCatalog.h>
#endif
#include <DataTypes/DataTypeString.h>

#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/StorageNull.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Core/ServerSettings.h>
#include <Common/logger_useful.h>

#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTSetQuery.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ColumnDefault.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/FailPoint.h>
#include <Common/HTTPHeaderFilter.h>
#include <base/EnumReflection.h>

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
    extern const DatabaseDataLakeSettingsString default_base_location;
    extern const DatabaseDataLakeSettingsS3UriStyle storage_uri_style;
    extern const DatabaseDataLakeSettingsString oauth_server_uri;
    extern const DatabaseDataLakeSettingsBool oauth_server_use_request_body;
    extern const DatabaseDataLakeSettingsBool vended_credentials;
    extern const DatabaseDataLakeSettingsString aws_access_key_id;
    extern const DatabaseDataLakeSettingsString aws_secret_access_key;
    extern const DatabaseDataLakeSettingsString region;
    extern const DatabaseDataLakeSettingsString aws_role_arn;
    extern const DatabaseDataLakeSettingsString aws_role_session_name;
    extern const DatabaseDataLakeSettingsString aws_external_id;
    extern const DatabaseDataLakeSettingsString onelake_tenant_id;
    extern const DatabaseDataLakeSettingsString onelake_client_id;
    extern const DatabaseDataLakeSettingsString onelake_client_secret;
    extern const DatabaseDataLakeSettingsString onelake_bearer_token;
    extern const DatabaseDataLakeSettingsString onelake_refresh_token;
    extern const DatabaseDataLakeSettingsBool onelake_use_blob_endpoint;
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
    extern const DatabaseDataLakeSettingsBool force_add_bucket;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_database_iceberg;
    extern const SettingsBool allow_experimental_database_unity_catalog;
    extern const SettingsBool allow_experimental_database_glue_catalog;
    extern const SettingsBool allow_experimental_database_hms_catalog;
    extern const SettingsBool allow_experimental_database_paimon_rest_catalog;
    extern const SettingsBool use_hive_partitioning;
    extern const SettingsBool log_queries;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsBool database_datalake_require_metadata_access;
    extern const SettingsBool data_lake_delete_data_on_drop;
    extern const SettingsBool s3_allow_server_credentials_in_user_queries;
    extern const SettingsBool show_data_lake_catalogs_in_system_tables;
    extern const SettingsString iceberg_metadata_compression_method;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsString iceberg_metadata_file_path;
    extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace ServerSetting
{
    extern const ServerSettingsBool s3_load_table_anonymously_if_credentials_restricted;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_DENIED;
    extern const int TABLE_ALREADY_EXISTS;
}

namespace FailPoints
{
    extern const char lightweight_show_tables[];
    extern const char datalake_try_get_table_return_nullptr[];
    extern const char datalake_try_get_table_throw[];
    extern const char datalake_get_tables_throw[];
}

namespace
{

/// In refresh-token mode a single token, minted with `auth_scope`, serves both the OneLake
/// catalog and Azure storage requests, so the scope must be the Azure storage audience.
constexpr auto ONELAKE_STORAGE_AUTH_SCOPE = "https://storage.azure.com/.default";

String getLocationSchemeForTableCreation(const std::shared_ptr<DataLake::ICatalog> & catalog)
{
    if (auto storage_type = catalog->getStorageType(); storage_type.has_value())
        return DataLake::storageTypeToScheme(*storage_type);

    /// Fall back only for catalogs whose backing storage is fixed.
    /// REST/Hive/Glue/Paimon/Unity can be backed by anything, so we refuse to guess.
    switch (catalog->getCatalogType())
    {
        case DatabaseDataLakeCatalogType::ICEBERG_ONELAKE:
            return "abfss"; /// Azure-only
        case DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE:
            return "s3"; /// GCS via S3 API
        case DatabaseDataLakeCatalogType::ICEBERG_REST:
        case DatabaseDataLakeCatalogType::S3_TABLES:
        case DatabaseDataLakeCatalogType::ICEBERG_DELTA_SHARING:
        case DatabaseDataLakeCatalogType::ICEBERG_HORIZON:
        case DatabaseDataLakeCatalogType::ICEBERG_HIVE:
        case DatabaseDataLakeCatalogType::GLUE:
        case DatabaseDataLakeCatalogType::PAIMON_REST:
        case DatabaseDataLakeCatalogType::UNITY:
        case DatabaseDataLakeCatalogType::NONE:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot determine storage scheme for CREATE TABLE for catalog type '{}': the catalog does not "
                "report a backing storage type. Set `default_base_location` on the database or configure "
                "the catalog to expose `default-base-location`.",
                catalog->getCatalogType());
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected catalog type in CREATE TABLE location scheme resolution");
}

/// Translate the database-layer `TablesFilter` into the catalog-layer
/// `TableNameFilter` so the catalog can restrict which namespaces it lists.
DataLake::TableNameFilter toCatalogTableNameFilter(const TablesFilter & tables_filter)
{
    switch (tables_filter.kind)
    {
        case TablesFilter::Kind::None:
            return {DataLake::TableNameFilter::Kind::All, {}};
        case TablesFilter::Kind::Equals:
            return {DataLake::TableNameFilter::Kind::Equals, tables_filter.pattern};
        case TablesFilter::Kind::Like:
            return {DataLake::TableNameFilter::Kind::Like, tables_filter.pattern};
    }
    return {DataLake::TableNameFilter::Kind::All, {}};
}

}

DatabaseDataLake::DatabaseDataLake(
    const std::string & database_name_,
    const std::string & url_,
    const DatabaseDataLakeSettings & settings_,
    ASTPtr database_engine_definition_,
    ASTPtr table_engine_definition_,
    UUID uuid,
    bool allow_server_credentials_in_user_queries_,
    bool is_loading_from_existing_metadata_,
    bool lazy_init)
    : IDatabase(database_name_)
    , url(url_)
    , database_settings(std::make_unique<const DatabaseDataLakeSettings>(settings_))
    , database_engine_definition(database_engine_definition_)
    , table_engine_definition(table_engine_definition_)
    , log(getLogger("DatabaseDataLake(" + database_name_ + ")"))
    , allow_server_credentials_in_user_queries(allow_server_credentials_in_user_queries_)
    , is_loading_from_existing_metadata(is_loading_from_existing_metadata_)
    , db_uuid(uuid)
{
    validateSettings();
    /// On ATTACH (server startup / user `ATTACH DATABASE`) or internal creates (restore),
    ///  defer catalog construction to first use: building it can perform network I/O or credential validation
    ///  that must not block startup. On CREATE build eagerly so misconfiguration (including a restricted
    ///  server-credential catalog) is reported immediately.
    if (!lazy_init)
    {
        std::lock_guard lock(catalog_mutex);
        initializeOrLeaveUnavailable();
    }
}

void DatabaseDataLake::validateSettings()
{
    const auto settings_version = database_settings.get();
    const DatabaseDataLakeSettings & settings = *settings_version;

    if (settings[DatabaseDataLakeSetting::catalog_type].value == DB::DatabaseDataLakeCatalogType::GLUE)
    {
        if (settings[DatabaseDataLakeSetting::region].value.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`region` setting cannot be empty for Glue catalog. "
                "Please specify 'SETTINGS region=<region_name>' in the CREATE DATABASE query");
    }
    else if (settings[DatabaseDataLakeSetting::catalog_type].value == DB::DatabaseDataLakeCatalogType::S3_TABLES)
    {
        if (settings[DatabaseDataLakeSetting::region].value.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`region` setting cannot be empty for S3 Tables catalog. "
                "Please specify 'SETTINGS region=<region_name>' in the CREATE DATABASE query");

        if (settings[DatabaseDataLakeSetting::warehouse].value.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`warehouse` setting cannot be empty for S3 Tables catalog. "
                "Please specify 'SETTINGS warehouse=<table_bucket_arn>' in the CREATE DATABASE query");
    }
    else if (settings[DatabaseDataLakeSetting::warehouse].value.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "`warehouse` setting cannot be empty. "
            "Please specify 'SETTINGS warehouse=<warehouse_name>' in the CREATE DATABASE query");
    }
}

void DatabaseDataLake::initialize() const
{
    /// Caller holds `catalog_mutex`: this runs either from the constructor (CREATE, eager)
    /// or from `getCatalog` on first access (ATTACH, lazy).
    const auto settings_version = database_settings.get();
    const DatabaseDataLakeSettings & settings = *settings_version;

    if (settings[DatabaseDataLakeSetting::catalog_type].value == DatabaseDataLakeCatalogType::NONE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unspecified catalog type");

    auto catalog_parameters = DataLake::CatalogSettings{
        .storage_endpoint = settings[DatabaseDataLakeSetting::storage_endpoint].value,
        .aws_access_key_id = settings[DatabaseDataLakeSetting::aws_access_key_id].value,
        .aws_secret_access_key = settings[DatabaseDataLakeSetting::aws_secret_access_key].value,
        .region = settings[DatabaseDataLakeSetting::region].value,
        .aws_role_arn = settings[DatabaseDataLakeSetting::aws_role_arn].value,
        .aws_role_session_name = settings[DatabaseDataLakeSetting::aws_role_session_name].value,
        .aws_external_id = settings[DatabaseDataLakeSetting::aws_external_id].value,
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
        case DB::DatabaseDataLakeCatalogType::ICEBERG_DELTA_SHARING:
        {
            /// Databricks Delta Sharing speaks plain Iceberg REST; it differs only in having flat
            /// (single-level) namespaces, which `DeltaSharingCatalog` reports via its catalog type.
            catalog_impl = std::make_shared<DataLake::DeltaSharingCatalog>(
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
        case DB::DatabaseDataLakeCatalogType::ICEBERG_HORIZON:
        {
            /// Snowflake Horizon embeds Polaris and speaks Iceberg REST, but authenticates with
            /// PAT/JWT as OAuth client_secret (optionally without client_id) and scope
            /// `session:role:<ROLE>`. `HorizonCatalog` accepts bare secrets as credentials.
            catalog_impl = std::make_shared<DataLake::HorizonCatalog>(
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
            /// The default `auth_scope` value targets Iceberg REST catalogs; for OneLake the
            /// token audience is Azure storage unless the user overrides it explicitly.
            const std::string onelake_auth_scope = settings[DatabaseDataLakeSetting::auth_scope].changed
                ? settings[DatabaseDataLakeSetting::auth_scope].value
                : ONELAKE_STORAGE_AUTH_SCOPE;
            catalog_impl = std::make_shared<DataLake::OneLakeCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::onelake_tenant_id].value,
                settings[DatabaseDataLakeSetting::onelake_client_id].value,
                settings[DatabaseDataLakeSetting::onelake_client_secret].value,
                settings[DatabaseDataLakeSetting::onelake_bearer_token].value,
                settings[DatabaseDataLakeSetting::onelake_refresh_token].value,
                onelake_auth_scope,
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
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "reading google credentials from file is deprecated");
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
                Context::getGlobalContextInstance(),
                allow_server_credentials_in_user_queries);
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
#if USE_AWS_S3 && USE_AVRO
            catalog_impl = std::make_shared<DataLake::GlueCatalog>(
                url,
                Context::getGlobalContextInstance(),
                catalog_parameters,
                table_engine_definition,
                allow_server_credentials_in_user_queries);
            break;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cannot use Glue catalog: ClickHouse was compiled without AWS S3 or Avro support");
#endif
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
        case DB::DatabaseDataLakeCatalogType::S3_TABLES:
        {
#if USE_AWS_S3 && USE_SSL
            catalog_impl = std::make_shared<DataLake::S3TablesCatalog>(
                settings[DatabaseDataLakeSetting::warehouse].value,
                url,
                settings[DatabaseDataLakeSetting::region].value,
                catalog_parameters,
                Context::getGlobalContextInstance(),
                allow_server_credentials_in_user_queries);
#else
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Amazon S3 Tables catalog requires ClickHouse built with USE_AWS_S3 and USE_SSL");
#endif
            break;
        }
    }
}

std::shared_ptr<DataLake::ICatalog> DatabaseDataLake::getCatalog() const
{
    std::lock_guard lock(catalog_mutex);
    /// Lazily build the catalog on first access for databases attached at startup (see ctor).
    if (!catalog_impl)
    {
        initializeOrLeaveUnavailable();
        if (!catalog_impl)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "DataLakeCatalog database is inaccessible: its catalog uses server-managed credentials that are "
                "restricted for user queries and could not be resolved when the database was loaded from metadata. "
#if CLICKHOUSE_CLOUD
                "Recreate the database with explicit catalog credentials (for a Glue catalog, an IAM role via "
                "aws_role_arn = '...'; for a BigLake catalog, a Google ADC triple). Reason: {}",
#else
                "Provide explicit credentials, or enable `s3_allow_server_credentials_in_user_queries`. Reason: {}",
#endif
                catalog_unavailable_reason);
    }
    return catalog_impl;
}

void DatabaseDataLake::initializeOrLeaveUnavailable() const
{
    try
    {
        initialize();
    }
    catch (const Exception & e)
    {
        /// On metadata load, a catalog that resolves the now-restricted server identity must not abort startup:
        /// leave it unavailable (`getCatalog` reports the reason on every query), mirroring S3/S3Queue tables.
        if (is_loading_from_existing_metadata && e.code() == ErrorCodes::ACCESS_DENIED
            && Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::s3_load_table_anonymously_if_credentials_restricted])
        {
            LOG_WARNING(
                log,
                "Loading this DataLakeCatalog database without a working catalog client: it resolves "
                "server-managed credentials that are restricted for user queries "
                "(s3_allow_server_credentials_in_user_queries = 0). The database will be inaccessible until "
                "its credentials resolve to a permitted source. Set the server setting "
                "s3_load_table_anonymously_if_credentials_restricted = 0 to fail loading instead. Reason: {}",
                e.message());
            resetCatalog(e.message());
        }
        else
            throw;
    }
}

void DatabaseDataLake::resetCatalog(String reason) const
{
    catalog_impl = nullptr;
    catalog_unavailable_reason = std::move(reason);
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
        case DatabaseDataLakeCatalogType::S3_TABLES:
        case DatabaseDataLakeCatalogType::ICEBERG_DELTA_SHARING:
        case DatabaseDataLakeCatalogType::ICEBERG_HORIZON:
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
    const auto settings_version = database_settings.get();
    const DatabaseDataLakeSettings & settings = *settings_version;

    auto endpoint_from_settings = settings[DatabaseDataLakeSetting::storage_endpoint].value;
    if (endpoint_from_settings.empty())
        return table_metadata.getLocation();
    return table_metadata.getLocationWithEndpoint(endpoint_from_settings, settings[DatabaseDataLakeSetting::storage_uri_style]);
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
    const auto settings_version = database_settings.get();
    const DatabaseDataLakeSettings & settings = *settings_version;

    auto catalog = getCatalog();
    auto table_metadata = DataLake::TableMetadata().withSchema().withLocation().withDataLakeSpecificProperties();
    if (settings[DatabaseDataLakeSetting::force_add_bucket])
        table_metadata.withForceAddBucket();

    /// This is added to test that lightweight queries like 'SHOW TABLES' dont end up fetching the table
    fiu_do_on(FailPoints::lightweight_show_tables,
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    });

    fiu_do_on(FailPoints::datalake_try_get_table_return_nullptr,
    {
        return nullptr;
    });

    /// Simulate a per-table metadata resolution failure (throws), so tests can exercise the
    /// graceful-degradation path in getTablesIteratorWithHint that keeps the table in the
    /// listing with a null storage object instead of aborting the whole scan.
    fiu_do_on(FailPoints::datalake_try_get_table_throw,
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Injected metadata resolution failure for table '{}'", name);
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

    /// We either fetch storage credentials from catalog
    /// or get storage credentials from database settings
    /// or get storage credentials from database engine arguments
    /// in CREATE query (e.g. in `args`).
    /// Vended credentials can be disabled in catalog itself,
    /// so we have a separate setting to know whether we should even try to fetch them.
    /// Some catalogs manage their own AWS credential provider chain (e.g. Glue uses the
    /// database `aws_*` settings to authenticate to the catalog API and to drive STS
    /// assume-role / instance-profile / web-identity providers, refreshed via
    /// `getCredentialsConfigurationCallback`). For such catalogs the `aws_*` settings are
    /// not authoritative static table-storage credentials: consuming them here would build
    /// the S3 client from the raw key pair without the assumed-role/session-token identity
    /// and would also suppress the provider-chain refresh callback below. So we only fall
    /// back to static credentials for catalogs whose refresh callback vends storage
    /// credentials (Unity/REST), which is exactly the case this fallback targets.
    const bool catalog_manages_provider_chain = catalog->getCatalogType() == DatabaseDataLakeCatalogType::GLUE;

    bool static_credentials_applied = false;
    if (args.size() == 1)
    {
        std::array<DatabaseDataLakeCatalogType, 3> vended_credentials_catalogs = {DatabaseDataLakeCatalogType::ICEBERG_ONELAKE, DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE, DatabaseDataLakeCatalogType::PAIMON_REST};

        std::shared_ptr<DataLake::IStorageCredentials> static_credentials;
        if (!catalog_manages_provider_chain)
            static_credentials = DataLake::tryGetStaticStorageCredentials(storage_type, settings);

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
        else if (static_credentials)
        {
            LOG_TRACE(log, "Using static credentials from database settings");
            static_credentials->addCredentialsToEngineArgs(args);
            static_credentials_applied = true;
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

    auto storage_settings = std::make_shared<DataLakeStorageSettings>();
    storage_settings->loadFromSettingsChanges(settings.allChanged());

    if (auto table_specific_properties = table_metadata.getDataLakeSpecificProperties();
        table_specific_properties.has_value())
    {
        auto metadata_location = table_specific_properties->iceberg_metadata_file_location;
        if (!metadata_location.empty())
        {
            metadata_location = table_metadata.getMetadataLocation(metadata_location);
            (*storage_settings)[DB::DataLakeStorageSetting::iceberg_metadata_file_path] = metadata_location;
        }
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
        const auto auth = rest_catalog->getStateSnapshot();
        /// In refresh-token mode the storage layer asks the catalog client for a valid
        /// access token on every request; the catalog renews it transparently.
        AzureBlobStorage::TokenProviderCredential::TokenProvider access_token_provider;
        if (!auth->refresh_token.empty())
            access_token_provider = [onelake_catalog = rest_catalog] { return onelake_catalog->getCurrentAccessToken(); };
        azure_configuration->setInitializationAsOneLake(
            auth->client_id,
            auth->client_secret,
            auth->tenant_id,
            auth->bearer_token,
            std::move(access_token_provider),
            settings[DatabaseDataLakeSetting::onelake_use_blob_endpoint].value
        );
#else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Server does not contain support for storage type Azure for Iceberg OneLake catalog");
#endif
    }

    if (catalog->getCatalogType() == DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE)
    {
#if USE_AWS_S3
        auto s3_configuration = std::dynamic_pointer_cast<StorageS3Configuration>(configuration);
        if (!s3_configuration)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is not S3 type for BigLake catalog");
        auto biglake_catalog = std::static_pointer_cast<DataLake::BigLakeCatalog>(catalog);
        s3_configuration->setInitializationAsBigLake(
            biglake_catalog->getGoogleADCClientId(),
            biglake_catalog->getGoogleADCClientSecret(),
            biglake_catalog->getGoogleADCRefreshToken()
        );
#else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Server does not contain support for storage type S3 for Iceberg BigLake catalog");
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

    /// When we applied static credentials from database settings, they are authoritative:
    /// do not let a catalog-vended refresh callback (e.g. Unity/REST `requestReadCredentials`)
    /// silently re-fetch credentials and override them. The same holds when the user disabled
    /// `vended_credentials` and no static credentials were applied (e.g. relying on default or
    /// environment S3 auth): the object storage layer invokes the refresh callback after an
    /// auth error, so a catalog-vended callback would silently fall back to vended credentials
    /// and defeat the setting. Provider-chain refresh callbacks (e.g. Glue STS/role) are not
    /// credential vending, so they remain active regardless of the `vended_credentials` setting
    /// to keep refreshing temporary credentials on long reads.
    auto get_credentials_refresh_callback = [&](const StorageID & storage_id) -> DataLake::ICatalog::CredentialsRefreshCallback
    {
        if (static_credentials_applied)
            return std::nullopt;
        if (!with_vended_credentials && !catalog_manages_provider_chain)
            return std::nullopt;
        return catalog->getCredentialsConfigurationCallback(storage_id);
    };

    const auto catalog_uuid = table_metadata.getTableUUID();
    const UUID table_uuid = catalog_uuid ? parseFromString<UUID>(*catalog_uuid) : UUIDHelpers::Nil;

    if (can_use_parallel_replicas && !is_secondary_query)
    {
        auto storage_id = StorageID(getDatabaseName(), name, table_uuid);
        auto storage_cluster = std::make_shared<StorageObjectStorageCluster>(
            parallel_replicas_cluster_name,
            configuration,
            configuration->createObjectStorage(context_copy, /* is_readonly */ false, get_credentials_refresh_callback(storage_id)),
            storage_id,
            columns,
            ConstraintsDescription{},
            nullptr,
            context_,
            /// Use is_table_function = true,
            /// because this table is actually stateless like a table function.
            /* is_table_function */true);

        if (context_->hasQueryContext() && context_->getSettingsRef()[Setting::log_queries])
            context_->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Storage, storage_cluster->getName());

        storage_cluster->startup();
        return storage_cluster;
    }

    /// Unlike table functions (s3, url, etc.), DataLake tables are queried as
    /// `SELECT * FROM catalog.table` — the query sent to shards cannot be rewritten
    /// into a Cluster table function variant. So when the initiator created a
    /// StorageObjectStorageCluster (the branch above) and the shard is collaborating
    /// with it, we need distributed_processing=true to use the task iterator.
    const bool distributed_processing =
        context_->getClientInfo().collaborate_with_initiator
        && can_use_parallel_replicas;

    auto result_storage = std::make_shared<StorageObjectStorage>(
        configuration,
        configuration->createObjectStorage(context_copy, /* is_readonly */ false, get_credentials_refresh_callback(StorageID(getDatabaseName(), name, table_uuid))),
        context_copy,
        StorageID(getDatabaseName(), name, table_uuid),
        /* columns */columns,
        /* constraints */ConstraintsDescription{},
        /* comment */"",
        getFormatSettings(context_copy),
        LoadingStrictnessLevel::CREATE,
        getCatalog(),
        /* if_not_exists*/true,
        /* is_datalake_query*/true,
        distributed_processing,
        /* partition_by */nullptr,
        /* order_by */nullptr,
        /// Use is_table_function = true,
        /// because this table is actually stateless like a table function.
        /* is_table_function */true,
        /* lazy_init */true);

    if (context_->hasQueryContext() && context_->getSettingsRef()[Setting::log_queries])
        context_->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Storage, result_storage->getName());

    return result_storage;
}

void DatabaseDataLake::validateCreateTableEngine(const String & engine_name) const
{
    /// `Iceberg` selects its backend from the optional `disk` setting. The setting is resolved by the
    /// storage factory, after this database-level validation, so it cannot be safely checked here.
    /// A fixed-backend catalog must not accept it: it could create a table that the catalog reopens
    /// with a different backend.
    if (engine_name == "Iceberg" && getCatalog()->getStorageType().has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The generic 'Iceberg' engine is not supported for a DataLakeCatalog with a fixed storage backend. "
            "Use the matching backend-specific Iceberg engine instead");

    /// Unrecognized names pin no backend and are accepted here; they are rejected by the storage factory.
    std::optional<DatabaseDataLakeStorageType> engine_backend;
    if (engine_name == "IcebergS3")
        engine_backend = DatabaseDataLakeStorageType::S3;
    else if (engine_name == "IcebergAzure")
        engine_backend = DatabaseDataLakeStorageType::Azure;
    else if (engine_name == "IcebergHDFS")
        engine_backend = DatabaseDataLakeStorageType::HDFS;
    else if (engine_name == "IcebergLocal")
        engine_backend = DatabaseDataLakeStorageType::Local;

    if (!engine_backend.has_value())
        return;

    /// A catalog without a fixed backend reopens the table using its own location, so any backend fits.
    auto catalog_storage_type = getCatalog()->getStorageType();
    if (!catalog_storage_type.has_value() || *catalog_storage_type == *engine_backend)
        return;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Table engine '{}' uses the {} storage backend, but this DataLakeCatalog stores tables on {}. "
        "The table would be reopened with the catalog's storage backend and become unreadable "
        "immediately after creation. Use a matching Iceberg engine or the generic 'Iceberg' engine",
        engine_name, *engine_backend, *catalog_storage_type);
}

void DatabaseDataLake::createTable(
    ContextPtr context_,
    const String & name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    /// Engine-clause path: the storage's own initialization (IcebergMetadata::createInitial)
    /// already wrote metadata and registered the table in the catalog; a path there that
    /// registers nothing throws `TABLE_ALREADY_EXISTS` instead of returning.
    if (table)
        return;

    auto catalog = getCatalog();
    const auto & create = query->as<ASTCreateQuery &>();
    const auto [namespace_name, table_name] = DataLake::parseTableName(name);

    ColumnsDescription columns;
    if (create.columns_list && create.columns_list->columns)
    {
        for (const auto & child : create.columns_list->columns->children)
        {
            const auto * col_decl = child->as<ASTColumnDeclaration>();
            if (!col_decl || !col_decl->getType())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid column declaration in CREATE TABLE");

            if (col_decl->default_specifier != ColumnDefaultSpecifier::Empty)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Column '{}': {} is not yet supported by DataLakeCatalog table creation",
                    col_decl->name,
                    toString(col_decl->default_specifier));

            if (col_decl->getComment() || col_decl->getCodec() || col_decl->getTTL()
                || col_decl->getStatisticsDesc() || col_decl->getSettings()
                || col_decl->primary_key_specifier)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Column '{}': COMMENT, CODEC, TTL, STATISTICS, SETTINGS, and PRIMARY KEY are not supported by DataLakeCatalog table creation",
                    col_decl->name);

            columns.add(ColumnDescription(col_decl->name, DataTypeFactory::instance().get(col_decl->getType())));
        }
    }

    if (columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create table without columns");

    if (create.columns_list
        && ((create.columns_list->indices && !create.columns_list->indices->children.empty())
            || (create.columns_list->constraints && !create.columns_list->constraints->children.empty())
            || (create.columns_list->projections && !create.columns_list->projections->children.empty())
            || create.columns_list->primary_key
            || create.columns_list->primary_key_from_columns))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DataLakeCatalog CREATE TABLE does not support PRIMARY KEY, indices, constraints, or projections");

    ASTPtr partition_by;
    ASTPtr order_by;
    if (create.storage)
    {
        if (create.storage->primary_key || create.storage->sample_by
            || create.storage->ttl_table || create.storage->unique_key
            || create.storage->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DataLakeCatalog CREATE TABLE supports only PARTITION BY and ORDER BY; "
                "PRIMARY KEY, SAMPLE BY, TTL, UNIQUE KEY, and engine SETTINGS are not supported");

        if (create.storage->partition_by)
            partition_by = create.storage->partition_by->clone();
        if (create.storage->order_by)
            order_by = create.storage->order_by->clone();
    }

    const auto settings_version = database_settings.get();
    const DatabaseDataLakeSettings & settings = *settings_version;

    String base_location = catalog->getDefaultBaseLocation();
    if (base_location.empty())
        base_location = settings[DatabaseDataLakeSetting::default_base_location].value;

    String location;
    if (!base_location.empty())
    {
        if (auto catalog_storage_type = catalog->getStorageType(); catalog_storage_type.has_value()
            && DataLake::parseStorageTypeFromLocation(base_location) != *catalog_storage_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`default_base_location` uses the {} storage backend, but this DataLakeCatalog stores tables on {}. "
                "The table would be reopened with the catalog's storage backend and become unreadable "
                "immediately after creation",
                DataLake::parseStorageTypeFromLocation(base_location), *catalog_storage_type);

        while (base_location.ends_with('/'))
            base_location.pop_back();
        location = fmt::format("{}/{}/{}", base_location, namespace_name, table_name);
    }
    else
    {
        const auto storage_endpoint = settings[DatabaseDataLakeSetting::storage_endpoint].value;
        if (storage_endpoint.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "CREATE TABLE in DataLakeCatalog requires `default_base_location` or `storage_endpoint`");
        location = DataLake::constructTableLocation(
            getLocationSchemeForTableCreation(catalog), storage_endpoint, namespace_name, table_name,
            settings[DatabaseDataLakeSetting::storage_uri_style]);
    }

    auto [metadata_content, metadata_str] = Iceberg::createEmptyMetadataFile(
        location,
        columns,
        partition_by,
        order_by,
        context_);

    /// Catalogs that write the initial metadata file themselves (they get an empty `metadata_path`) must
    /// honour `iceberg_metadata_compression_method` too, otherwise the native CREATE TABLE path would
    /// diverge from the explicit Iceberg engine path, which applies it in `IcebergMetadata::createInitial`.
    const auto compression_method_str = context_->getSettingsRef()[Setting::iceberg_metadata_compression_method].value;
    const auto compression_method = chooseCompressionMethod(compression_method_str, compression_method_str);

    /// Register the namespace before `createTable`, which requires it to exist and, for catalogs that
    /// write the initial metadata file themselves, must not be preceded by any file written to storage
    /// (see `ICatalog::createTable`). Do it after all local validation, so a rejected CREATE leaves no
    /// trace in the catalog.
    /// `location` is the table location (base/namespace/table). The namespace's default location must
    /// point at the namespace base (base/namespace), not at this first table's directory; otherwise
    /// later tables created in the same namespace without an explicit location could be placed under
    /// the first table's directory. Strip the trailing table-name segment to get the namespace base.
    String namespace_location = location;
    if (const String table_suffix = "/" + table_name; namespace_location.ends_with(table_suffix))
        namespace_location.resize(namespace_location.size() - table_suffix.size());
    catalog->createNamespaceIfNotExists(namespace_name, namespace_location);

    const bool created = catalog->createTable(
        namespace_name, table_name, /* metadata_path */ "", metadata_content, compression_method, create.if_not_exists);
    if (!created)
    {
        /// `IF NOT EXISTS`, and the catalog answered that the table is already there: it is shared, so
        /// another client can create the same name between the existence check in `doCreateTable` and
        /// this call. Nothing was created here, and the caller must be able to tell - report it exactly
        /// like the local existence check does, so that `CREATE TABLE IF NOT EXISTS ... AS SELECT` does
        /// not insert the selected rows into the table the other client created.
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
            "Table {}.{} already exists in the catalog", namespace_name, table_name);
    }

    LOG_INFO(log, "Created table {}.{}", namespace_name, table_name);
}

void DatabaseDataLake::dropTable( /// NOLINT
    ContextPtr context_,
    const String & name,
    bool /*sync*/,
    bool if_exists)
{
    auto catalog = getCatalog();
    const auto [namespace_name, table_name] = DataLake::parseTableName(name);

    bool purge = context_->getSettingsRef()[Setting::data_lake_delete_data_on_drop];
    catalog->dropTable(namespace_name, table_name, purge, if_exists);

    /// A catalog-side drop removes remote metadata and, when purge is set, can request deletion of the
    /// underlying data. Log it at an operational level so accidental drops/purges leave an audit trail.
    LOG_INFO(log, "Dropped table {}.{} from DataLakeCatalog (purge={})", namespace_name, table_name, purge);
}

DatabaseTablesIteratorPtr DatabaseDataLake::getTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded) const
{
    /// General-purpose iterator. Consumers such as StorageMerge dereference the storage
    /// object of every row unconditionally, so a null-storage row would hang or crash them.
    /// Keep the original contract: propagate the error when metadata access is required,
    /// otherwise drop the unresolved table. Null-storage rows are confined to
    /// getTablesIteratorWithHint (system.tables), which null-guards every consumer.
    return getTablesIteratorImpl(
        context_, filter_by_table_name, skip_not_loaded, /*tables_filter*/ {}, /*keep_unresolved_tables*/ false);
}

DatabaseTablesIteratorPtr DatabaseDataLake::getTablesIteratorWithHint(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded,
    const TablesFilter & tables_filter) const
{
    /// system.tables path: keep a row for a table whose metadata cannot be resolved, with a
    /// null storage object, so metadata-dependent columns degrade to defaults instead of the
    /// whole scan aborting. StorageSystemTables null-guards every storage-dependent column.
    return getTablesIteratorImpl(
        context_, filter_by_table_name, skip_not_loaded, tables_filter, /*keep_unresolved_tables*/ true);
}

DatabaseTablesIteratorPtr DatabaseDataLake::getTablesIteratorImpl(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded,
    const TablesFilter & tables_filter,
    bool keep_unresolved_tables) const
{
    Tables tables;
    DataLake::CatalogTables catalog_tables;

    /// Do not throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some datalake error.
    try
    {
        fiu_do_on(FailPoints::datalake_get_tables_throw,
        {
            throw Exception(ErrorCodes::DATALAKE_DATABASE_ERROR, "Injected catalog listing failure");
        });

        catalog_tables = getCatalog()->getTables(toCatalogTableNameFilter(tables_filter));
    }
    catch (...)
    {
        if (context_->getSettingsRef()[Setting::show_data_lake_catalogs_in_system_tables])
            throw;
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    /// Skip tables ClickHouse cannot read (Delta/raw files in mixed catalogs like Glue/Unity)
    /// and apply the name filter once, matching getLightweightTablesIterator (SHOW TABLES).
    DB::Names iceberg_tables;
    for (const auto & catalog_table : catalog_tables)
    {
        if (!catalog_table.is_readable)
            continue;
        if (filter_by_table_name && !filter_by_table_name(catalog_table.name))
            continue;
        iceberg_tables.push_back(catalog_table.name);
    }

    auto & pool = Context::getGlobalContextInstance()->getIcebergCatalogThreadpool();

    std::vector<std::shared_ptr<std::promise<StoragePtr>>> promises;
    std::vector<std::future<StoragePtr>> futures;
    for (const auto & table_name : iceberg_tables)
    {
        try
        {
            promises.emplace_back(std::make_shared<std::promise<StoragePtr>>());
            futures.emplace_back(promises.back()->get_future());

            pool.scheduleOrThrow(
                [this, table_name, skip_not_loaded, context_, keep_unresolved_tables, promise=promises.back()]() mutable
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
                            if (keep_unresolved_tables)
                            {
                                /// system.tables path: a single table's metadata failing to
                                /// resolve must not abort the whole listing nor silently omit
                                /// the table. Keep a row for it with a null storage object;
                                /// metadata-dependent columns come back as defaults/NULL. Direct
                                /// access (SELECT ... FROM db.table) still surfaces the real error.
                                LOG_WARNING(
                                    log,
                                    "Received error {} while fetching table metadata for existing table '{}'. "
                                    "Keeping it in the listing with unresolved metadata. Error: {}",
                                    error_code,
                                    table_name,
                                    error_message);
                            }
                            else
                            {
                                /// General-purpose path: consumers dereference the storage
                                /// unconditionally, so propagate the error rather than hand
                                /// them a null-storage row.
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

        /// futures[future_index].get() rethrows for the general-purpose path when metadata
        /// access is required (see the per-table catch above), preserving the original
        /// abort-on-error contract for consumers that dereference the storage unconditionally.
        auto table_ptr = futures[future_index].get();
        future_index++;

        /// For the system.tables path keep a row even when the storage could not be resolved
        /// (table_ptr is null), so the table still shows up with default/NULL metadata columns
        /// instead of being dropped or aborting the scan. For every other consumer drop the
        /// unresolved table (require_metadata_access=0 case) to keep the iterator's contract
        /// that every row has a valid storage object.
        if (!keep_unresolved_tables && !table_ptr)
            continue;

        [[maybe_unused]] bool inserted = tables.emplace(table_name, table_ptr).second;
        chassert(inserted);
    }
    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}

std::vector<LightWeightTableDetails> DatabaseDataLake::getLightweightTablesIterator(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool skip_not_loaded) const
{
    return getLightweightTablesIteratorWithHint(context_, filter_by_table_name, skip_not_loaded, /*tables_filter*/ {});
}

std::vector<LightWeightTableDetails> DatabaseDataLake::getLightweightTablesIteratorWithHint(
    ContextPtr context_,
    const FilterByNameFunction & filter_by_table_name,
    bool /*skip_not_loaded*/,
    const TablesFilter & tables_filter) const
{
    DataLake::CatalogTables catalog_tables;
    std::vector<LightWeightTableDetails> result;

    /// Do not throw here, because this might be, for example, a query to system.tables.
    /// It must not fail on case of some datalake error.
    try
    {
        fiu_do_on(FailPoints::datalake_get_tables_throw,
        {
            throw Exception(ErrorCodes::DATALAKE_DATABASE_ERROR, "Injected catalog listing failure");
        });

        catalog_tables = getCatalog()->getTables(toCatalogTableNameFilter(tables_filter));
    }
    catch (...)
    {
        if (context_->getSettingsRef()[Setting::show_data_lake_catalogs_in_system_tables])
            throw;
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    for (const auto & catalog_table : catalog_tables)
    {
        /// Skip tables ClickHouse cannot read, so SHOW TABLES stays consistent with the
        /// full getTablesIterator path without a per-table metadata fetch.
        if (!catalog_table.is_readable)
            continue;
        if (filter_by_table_name && !filter_by_table_name(catalog_table.name))
            continue;
        result.emplace_back(catalog_table.name);
    }

    return result;
}

VectorWithMemoryTracking<String> DatabaseDataLake::getAllTableNames(ContextPtr /*context*/) const
{
    VectorWithMemoryTracking<String> result;

    /// Do not throw here, because this is called from the typo-hint path
    /// (IDatabase::getTable -> TableNameHints -> getAllRegisteredNames) which
    /// must not fail even when the catalog is temporarily unreachable.
    try
    {
        DataLake::CatalogTables tables = getCatalog()->getTables();
        result.reserve(tables.size());
        for (auto & table : tables)
        {
            /// Only suggest tables ClickHouse can actually read.
            if (!table.is_readable)
                continue;
            result.push_back(std::move(table.name));
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
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

void DatabaseDataLake::checkDatabase() const
{
    auto catalog = getCatalog();
    /// This function checks if we can access catalog and get tables list.
    /// We do not check if there are tables in catalog, because even if catalog is empty, it still can be valid and working.
    std::ignore = catalog->empty();


    LOG_TEST(log, "Database '{}' is OK", getDatabaseName());
}

void DatabaseDataLake::applySettingsChanges(const SettingsChanges & settings_changes, ContextPtr /*query_context*/)
{
    const auto current_settings = database_settings.get();

    /// This check in some sense duplicate check in ICatalog, because it's a valid case when
    /// catalog can be unitilized here, and we actually use alter to "resurrect it". For example provide
    /// proper credentials with settings.
    DataLake::CatalogSettingsAlterValidatorFactory::instance().validate(*current_settings, settings_changes);

    auto new_settings = std::make_unique<DatabaseDataLakeSettings>(*current_settings);
    new_settings->applyChanges(settings_changes);

    ASTPtr new_engine_definition;
    {
        std::lock_guard lock(mutex);
        new_engine_definition = database_engine_definition->clone();
    }
    auto * storage = new_engine_definition->as<ASTStorage>();
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database engine definition of database {} is not a storage AST", getDatabaseName());

    if (storage->settings)
    {
        auto & stored_changes = storage->settings->changes;
        for (const auto & change : settings_changes)
        {
            /// cleanup duplicates
            std::erase_if(stored_changes, [&](const auto & prev) { return prev.name == change.name; });
            stored_changes.push_back(change);
        }
    }
    else
    {
        auto storage_settings_ast = make_intrusive<ASTSetQuery>();
        storage_settings_ast->is_standalone = false;
        storage_settings_ast->changes = settings_changes;
        storage->set(storage->settings, storage_settings_ast);
    }

    std::shared_ptr<DataLake::ICatalog> local_catalog_snapshot;
    {
        std::lock_guard lock(catalog_mutex);
        local_catalog_snapshot = catalog_impl;
    }

    /// Prepare the new catalog state without publishing it: validation, the eager token
    /// fetch and the config reload may throw, and then nothing has changed yet.
    DataLake::ICatalog::PreparedSettingsChangesPtr prepared_catalog_changes;
    if (local_catalog_snapshot)
        prepared_catalog_changes = local_catalog_snapshot->prepareSettingsChanges(settings_changes);

    /// Persist the new metadata before publishing anything: if the write fails, the live
    /// state is untouched and matches the old metadata on disk. The create query is built
    /// from the patched definition because the live one is not swapped yet.
    auto new_create_query = make_intrusive<ASTCreateQuery>();
    new_create_query->setDatabase(getDatabaseName());
    new_create_query->set(new_create_query->storage, new_engine_definition);
    new_create_query->uuid = db_uuid;
    DatabaseCatalog::instance().updateMetadataFile(getDatabaseName(), new_create_query);

    /// Publish. Nothing below throws.
    if (local_catalog_snapshot)
        local_catalog_snapshot->commitSettingsChanges(std::move(prepared_catalog_changes));
    database_settings.set(std::move(new_settings));
    {
        std::lock_guard lock(mutex);
        database_engine_definition = new_engine_definition;
    }
    if (!local_catalog_snapshot)
    {
        /// The catalog was not built when the ALTER started. If a concurrent query
        /// built it meanwhile, it used the old settings: drop it so the next access
        /// rebuilds it with the new ones. Also clear a recorded construction failure
        /// (e.g. credentials lost on RESTORE) for the same reason.
        std::lock_guard lock(catalog_mutex);
        resetCatalog(/* reason */ "");
    }
}

ASTPtr DatabaseDataLake::getCreateTableQueryImpl(
    const String & name,
    ContextPtr /* context_ */,
    bool throw_on_error) const
{
    const auto settings_version = database_settings.get();
    const DatabaseDataLakeSettings & settings = *settings_version;

    auto catalog = getCatalog();
    auto table_metadata = DataLake::TableMetadata().withLocation().withSchema();
    if (settings[DatabaseDataLakeSetting::force_add_bucket])
        table_metadata.withForceAddBucket();

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

void registerDatabaseDataLake(DatabaseFactory & factory);
void registerDatabaseDataLake(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        const auto * database_engine_define = args.create_query.storage;
        const auto & database_engine_name = args.engine_name;

        DatabaseDataLakeSettings database_settings;
        if (database_engine_define->settings)
            database_settings.loadFromQuery(*database_engine_define, args.create_query.attach);

        const auto & auth_header_str = database_settings[DatabaseDataLakeSetting::auth_header].value;
        /// Validate `auth_header` on CREATE only (matches the `allow_experimental_database_*`
        /// gates below, which also self-skip on attach). An already-persisted database whose
        /// `auth_header` was accepted by an older version must still attach at startup, so a
        /// single misconfigured database cannot block the server from starting. The malformed
        /// header is then reported lazily on first use of the database.
        if (!args.create_query.attach && !auth_header_str.empty())
        {
            /// Only headers with a valid `name: value` format are accepted.
            auto pos = auth_header_str.find(':');
            if (pos != std::string::npos)
            {
                DB::HTTPHeaderEntries header_entries{{auth_header_str.substr(0, pos), auth_header_str.substr(pos + 1)}};
                args.context->getGlobalContext()->getHTTPHeaderFilter().checkAndNormalizeHeaders(header_entries);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid auth header format. Expected 'HeaderName: HeaderValue'");
            }
        }

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

        if (database_engine_name == "Iceberg"
            && catalog_type != DatabaseDataLakeCatalogType::ICEBERG_REST
            && catalog_type != DatabaseDataLakeCatalogType::S3_TABLES
            && catalog_type != DatabaseDataLakeCatalogType::ICEBERG_BIGLAKE
            && catalog_type != DatabaseDataLakeCatalogType::ICEBERG_ONELAKE)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Engine `Iceberg` must use `rest`, `s3tables`, `biglake`, or `onelake` catalog type only");
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
            case DatabaseDataLakeCatalogType::ICEBERG_DELTA_SHARING:
            case DatabaseDataLakeCatalogType::ICEBERG_HORIZON:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_iceberg])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with Iceberg Rest catalog is beta. "
                                    "To allow its usage, enable setting allow_database_iceberg");
                }

                if (!args.create_query.attach && catalog_type == DatabaseDataLakeCatalogType::ICEBERG_HORIZON)
                {
                    const bool has_credential = !database_settings[DatabaseDataLakeSetting::catalog_credential].value.empty();
                    const bool has_auth_header = !database_settings[DatabaseDataLakeSetting::auth_header].value.empty();
                    if (has_credential == has_auth_header)
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Horizon catalog requires exactly one authentication method: "
                            "`catalog_credential` (PAT or key-pair JWT) "
                            "or `auth_header` (Authorization: Bearer <token>)");
                    }

                    /// Horizon scopes are Snowflake session roles, not Polaris principal roles.
                    const auto & scope = database_settings[DatabaseDataLakeSetting::auth_scope].value;
                    if (!has_auth_header && !scope.starts_with("session:role:"))
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Horizon catalog with `catalog_credential` requires `auth_scope` in the form "
                            "`session:role:<ROLE>` (got '{}'). When using a pre-exchanged bearer token via "
                            "`auth_header`, scope is optional",
                            scope);
                    }

                    if (database_settings[DatabaseDataLakeSetting::warehouse].value.empty())
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Horizon catalog requires `warehouse` set to the Snowflake database name "
                            "(usually uppercase, e.g. ICEBERG_TEST_DB)");
                    }
                }

                if (!args.create_query.attach && catalog_type == DatabaseDataLakeCatalogType::ICEBERG_ONELAKE)
                {
                    /// Require exactly one auth method: a bearer token, a refresh token (needs a client id,
                    /// a secret only for confidential app registrations), or a client id + secret pair.
                    const bool has_bearer = !database_settings[DatabaseDataLakeSetting::onelake_bearer_token].value.empty();
                    const bool has_refresh = !database_settings[DatabaseDataLakeSetting::onelake_refresh_token].value.empty();
                    const bool has_client_id = !database_settings[DatabaseDataLakeSetting::onelake_client_id].value.empty();
                    const bool has_client_secret = !database_settings[DatabaseDataLakeSetting::onelake_client_secret].value.empty();

                    const auto throw_invalid_auth = []
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "OneLake catalog requires exactly one authentication method: `onelake_bearer_token`, "
                            "or `onelake_refresh_token` with `onelake_client_id` (and `onelake_client_secret` "
                            "for confidential app registrations), or both `onelake_client_id` and `onelake_client_secret`");
                    };

                    if (has_bearer)
                    {
                        if (has_refresh || has_client_id || has_client_secret)
                            throw_invalid_auth();
                    }
                    else if (has_refresh)
                    {
                        if (!has_client_id)
                            throw_invalid_auth();

                        /// The refresh token grant always sends parameters in the request body,
                        /// no matter whether it goes to the default Entra ID token endpoint or to
                        /// a custom `oauth_server_uri`; the query-parameter flavor selected by
                        /// this setting is not implemented for it.
                        if (!database_settings[DatabaseDataLakeSetting::oauth_server_use_request_body].value)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "`oauth_server_use_request_body = 0` is not supported together with `onelake_refresh_token`: "
                                "the refresh token grant always sends parameters in the request body, "
                                "regardless of `oauth_server_uri`");

                        /// In refresh-token mode the storage layer reuses the catalog access token,
                        /// so a scope accepted by the catalog but not by Azure storage would pass
                        /// CREATE and then fail on the first table read.
                        if (database_settings[DatabaseDataLakeSetting::auth_scope].changed
                            && database_settings[DatabaseDataLakeSetting::auth_scope].value != ONELAKE_STORAGE_AUTH_SCOPE)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "`onelake_refresh_token` requires `auth_scope` to be the Azure storage audience '{}' "
                                "(or unset), because the same access token is used for both the catalog and Azure "
                                "storage requests. Got `auth_scope` = '{}'",
                                ONELAKE_STORAGE_AUTH_SCOPE,
                                database_settings[DatabaseDataLakeSetting::auth_scope].value);
                    }
                    else if (!has_client_id || !has_client_secret)
                        throw_invalid_auth();
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
            case DatabaseDataLakeCatalogType::S3_TABLES:
            {
                if (!args.create_query.attach
                    && !args.context->getSettingsRef()[Setting::allow_experimental_database_iceberg])
                {
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                    "DatabaseDataLake with S3 Tables catalog (Iceberg REST) is beta. "
                                    "To allow its usage, enable setting allow_database_iceberg");
                }

                engine_func->name = "Iceberg";
                break;
            }
            case DatabaseDataLakeCatalogType::NONE:
                break;
        }

        /// The catalog client is cached for every later query, so capture the restriction now from the CREATE
        /// query rather than relying on the live per-session setting.
        const bool allow_server_credentials_in_user_queries
            = args.context->getSettingsRef()[Setting::s3_allow_server_credentials_in_user_queries];

        /// A database is replayed from its stored `ATTACH DATABASE` statement with plain `ATTACH` on startup
        /// (unlike tables, which use `FORCE_ATTACH`), so `isLoadingFromExistingMetadata` is too narrow. Treat an
        /// internal attach (server startup / restore) as a metadata load so a now-restricted catalog is left
        /// unavailable instead of aborting startup; a user `ATTACH DATABASE` stays fail-closed and is rejected.
        const bool is_loading_from_existing_metadata = args.internal && args.mode >= LoadingStrictnessLevel::ATTACH;

        return std::make_shared<DatabaseDataLake>(
            args.database_name,
            url,
            database_settings,
            database_engine_define->clone(),
            std::move(engine_for_tables),
            args.uuid,
            allow_server_credentials_in_user_queries,
            is_loading_from_existing_metadata,
            /// Internal creates (`RESTORE DATABASE`) shouldn't do network I/O.
            /// We don't want an unreachable or unauthorized catalog to block replica startup.
            /*lazy_init=*/args.create_query.attach || args.internal);
    };
    /// TODO: DataLakeCatalog is polymorphic — underlying source (S3, Azure, HDFS, etc.) depends
    /// on the catalog type chosen at runtime. Consider adding source_access_type once a mechanism
    /// for runtime-dependent or composite source checks exist.
    factory.registerDatabase("DataLakeCatalog", create_fn, {
        .supports_arguments = true,
        .supports_settings = true,
        .is_external = true,
    }, Documentation{
        .description = R"DOCS_MD(
The `DataLakeCatalog` database engine enables you to connect ClickHouse to external
data catalogs and query open table format data without the need for data duplication.
This transforms ClickHouse into a powerful query engine that works seamlessly with
your existing data lake infrastructure.

## Supported catalogs {#supported-catalogs}

The `DataLakeCatalog` engine supports the following data catalogs:

- **AWS Glue Catalog** - For Iceberg tables in AWS environments
- **Databricks Unity Catalog** - For Delta Lake and Iceberg tables
- **Hive Metastore** - Traditional Hadoop ecosystem catalog
- **REST Catalogs** - Any catalog supporting the Iceberg REST specification

## Creating a database {#creating-a-database}

You will need to enable the relevant settings below to use the `DataLakeCatalog` engine:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

Databases with the `DataLakeCatalog` engine can be created using the following syntax:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

The following settings are supported:

| Setting                 | Description                                                                             |
|-------------------------|-----------------------------------------------------------------------------------------|
| `catalog_type`          | Type of catalog: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg), `delta_sharing` (Iceberg, flat namespaces), `horizon` (Snowflake Horizon Iceberg REST) |
| `warehouse`             | The warehouse/database name to use in the catalog.                                      |
| `catalog_credential`    | Authentication credential for the catalog (e.g., API key or token)                      |
| `auth_header`           | Custom HTTP header for authentication with the catalog service                          |
| `auth_scope`            | OAuth2 scope for authentication (if using OAuth)                                        |
| `storage_endpoint`      | Endpoint URL for the underlying storage                                                 |
| `default_base_location` | Base URI for new tables when the catalog does not report `default-base-location`. New tables are placed under `<default_base_location>/<namespace>/<table>` (e.g. `s3://warehouse/data`) |
| `oauth_server_uri`      | URI of the OAuth2 authorization server for authentication                               |
| `vended_credentials`    | Boolean indicating whether to use vended credentials from the catalog (supports AWS S3 and Azure ADLS Gen2) |
| `aws_access_key_id`     | AWS access key ID for S3/Glue access (if not using vended credentials)                  |
| `aws_secret_access_key` | AWS secret access key for S3/Glue access (if not using vended credentials)              |
| `aws_role_arn`          | ARN of the IAM role to assume for AWS/Glue access. When set, ClickHouse uses AWS STS `AssumeRole` with base credentials from `aws_access_key_id` and `aws_secret_access_key` when both are provided, or from the default AWS credential chain otherwise (the role must trust the identity the server runs under). |
| `aws_role_session_name` | Session name used for the AWS STS `AssumeRole` call. Optional; defaults to `ClickHouseSession`. |
| `aws_external_id`       | External ID passed to AWS STS `AssumeRole`, matching the `sts:ExternalId` condition on the role's trust policy. Use this when the role is owned by a third party, such as ClickHouse Cloud. |
| `region`                | AWS region for the service (e.g., `us-east-1`)                                          |
| `dlf_access_key_id`     | Access key ID for DLF access                                                            |
| `dlf_access_key_secret` | Access key Secret for DLF access                                                        |
| `force_add_bucket`      | When constructing object-storage URLs from the catalog-provided table location and `storage_endpoint`, prepend the bucket/container name even if the endpoint already contains it. Default: `false`. Set to `true` for catalogs that hand back paths without the bucket and require it to be added at the URL-construction step (Polaris-style paths). |

## Creating tables {#creating-tables}

An Iceberg table in a `DataLakeCatalog` database can be created directly from ClickHouse.

:::note
`CREATE TABLE` and `DROP TABLE` require a catalog that can perform catalog mutations. They are supported
for Iceberg REST catalogs (including OneLake, BigLake, and Delta Sharing) and for the AWS Glue catalog.
Other catalog types (Unity, Hive Metastore, Paimon REST) are read-only and reject these statements.
:::

The location of a newly created table comes from `default_base_location` (a full `s3://bucket/prefix`) when
set, otherwise the bucket is derived from `storage_endpoint`. With `storage_uri_style = 'virtual_hosted'` the
bucket cannot be derived from the endpoint unambiguously, so `default_base_location` is required for
`CREATE TABLE`.

The table name must be quoted with backticks and include the namespace separated by a dot:

```sql
CREATE TABLE catalog_db.`namespace.table_name`
(
    id Int64,
    name String,
    value Float64
)
PARTITION BY id
ORDER BY name
SETTINGS allow_database_iceberg = 1;
```

Iceberg accepts only a fixed set of partition transforms, so `PARTITION BY`
must use one of the following expressions:

| Expression                    | Iceberg transform |
|-------------------------------|-------------------|
| `<column>`                    | `identity`        |
| `toYearNumSinceEpoch(<col>)`  | `year`            |
| `toMonthNumSinceEpoch(<col>)` | `month`           |
| `toRelativeDayNum(<col>)`     | `day`             |
| `toRelativeHourNum(<col>)`    | `hour`            |
| `icebergTruncate(N, <col>)`   | `truncate[N]`     |
| `icebergBucket(N, <col>)`     | `bucket[N]`       |

Composite partitioning is supported via `PARTITION BY (expr1, expr2, ...)`.
Other expressions (e.g. `toYYYYMM`, `intDiv`) are rejected at `CREATE TABLE`.

Only the column names and types, `PARTITION BY`, and `ORDER BY` are persisted into the Iceberg
table metadata. Anything else — the storage clauses `PRIMARY KEY`, `SAMPLE BY`, `TTL`, and
`UNIQUE KEY`; indices, constraints, and projections; and the column modifiers `DEFAULT`,
`MATERIALIZED`, `ALIAS`, `EPHEMERAL`, `COMMENT`, `CODEC`, `TTL`, `STATISTICS`, and `SETTINGS` —
is rejected rather than silently dropped. This applies both with and without an explicit
`ENGINE` clause. Engine `SETTINGS` are accepted only together with an explicit Iceberg engine,
where they are the engine's storage settings (e.g. `iceberg_format_version`).

You can also create an Iceberg table that inherits the schema of an existing table:

```sql
CREATE TABLE catalog_db.`namespace.table_name`
AS other_db.source_table
SETTINGS allow_database_iceberg = 1;
```

If the source table's `PARTITION BY` and `ORDER BY` use only the expressions
listed above, they are copied into the new Iceberg table.

## Dropping tables {#dropping-tables}

Tables can be dropped from a `DataLakeCatalog` database.
`DROP TABLE` sends a delete request to the remote catalog, which removes
the table entry from the catalog.

```sql
DROP TABLE catalog_db.`namespace.table_name`
```

By default, ClickHouse does not request the catalog to delete the underlying data. In order to do it, use the `data_lake_delete_data_on_drop` setting:

```sql
DROP TABLE catalog_db.`namespace.table_name`
SETTINGS data_lake_delete_data_on_drop = 1
```

:::note
Whether data files are actually deleted depends on the catalog itself.
The `purgeRequested` flag is sent to the catalog, but the catalog may choose to ignore it.
For the Glue catalog, `DROP TABLE` only removes the catalog entry and does not delete the underlying data
files, so `DROP TABLE` with `data_lake_delete_data_on_drop = 1` is rejected instead of silently leaving the
data behind.
:::

## Examples {#examples}

See below sections for examples of using the `DataLakeCatalog` engine:

* [Unity Catalog](/guides/use-cases/data-warehousing/unity-catalog)
* [Glue Catalog](/guides/use-cases/data-warehousing/glue-catalog)
* OneLake Catalog
    Can be used by enabling `allow_experimental_database_iceberg` or `allow_database_iceberg`.
```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```
    To authenticate without sharing a client secret, set `onelake_bearer_token` to a pre-obtained
    bearer token (scoped to https://storage.azure.com) instead of
    `onelake_client_id`/`onelake_client_secret`. ClickHouse does not refresh the token, so the
    database must be recreated after it expires.
)DOCS_MD",
        .syntax = "ENGINE = DataLakeCatalog('catalog_url'[, 'user', 'password']) SETTINGS catalog_type = '...'",
        .related = {}});
}

}

#endif
