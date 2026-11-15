#include <Databases/DataLake/GlueCatalog.h>
#include <Poco/JSON/Object.h>
#include <Core/ServerSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#if USE_AWS_S3 && USE_AVRO

#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetDatabaseRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>
#include <aws/glue/model/CreateTableRequest.h>
#include <aws/glue/model/DeleteTableRequest.h>
#include <aws/glue/model/DeleteDatabaseRequest.h>
#include <aws/glue/model/CreateDatabaseRequest.h>
#include <aws/glue/model/UpdateTableRequest.h>
#include <aws/glue/model/TableInput.h>
#include <aws/glue/model/StorageDescriptor.h>
#include <aws/glue/model/Column.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpResponse.h>

#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>


#include <Databases/DataLake/Common.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3Settings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/FailPoint.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/ProxyConfigurationResolverProvider.h>

namespace DB::ErrorCodes
{
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int FAULT_INJECTED;
}

namespace DB::FailPoints
{
    extern const char check_database_datalake_negative[];
    extern const char glue_catalog_pause_before_update_table[];
}

namespace DB::Setting
{
    extern const SettingsUInt64 s3_max_redirects;
    extern const SettingsBool iceberg_time_as_int64;
    extern const SettingsUInt64 s3_retry_attempts;
    extern const SettingsBool s3_slow_all_threads_after_network_error;
    extern const SettingsBool enable_s3_requests_logging;
    extern const SettingsUInt64 s3_connect_timeout_ms;
    extern const SettingsUInt64 s3_request_timeout_ms;
    extern const SettingsBool iceberg_list_only_v_prefixed_metadata_files;
}

namespace DB::ServerSetting
{
    extern const ServerSettingsUInt64 s3_max_redirects;
    extern const ServerSettingsUInt64 s3_retry_attempts;
}

namespace DataLake
{

namespace
{

/// The generated Glue error mapper marks every modeled exception as not retryable, including server
/// faults such as `InternalServiceException` (HTTP 500), so a transient status is retried regardless.
class GlueRetryStrategy : public Aws::Client::DefaultRetryStrategy
{
public:
    /// NOLINTNEXTLINE(google-runtime-int)
    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors> & error, long attempted_retries) const override
    {
        if (attempted_retries >= m_maxRetries)
            return false;
        if (DB::CurrentThread::isInitialized() && DB::CurrentThread::get().isQueryCanceled())
            return false;
        return error.ShouldRetry() || Aws::Http::IsRetryableHttpResponseCode(error.GetResponseCode());
    }
};

/// A Glue table is readable only if it is an Iceberg table. `table_type` is available in
/// both the bulk `GetTables` listing and `tryGetTableMetadata`, so the two stay consistent.
bool isReadableGlueTable(const Aws::Glue::Model::Table & table)
{
    const auto & parameters = table.GetParameters();
    auto it = parameters.find("table_type");
    const std::string table_type = it != parameters.end() ? it->second : "";
    return Poco::toUpper(table_type) == "ICEBERG";
}

}

GlueCatalog::GlueCatalog(
    const String & endpoint,
    DB::ContextPtr context_,
    const CatalogSettings & settings_,
    DB::ASTPtr table_engine_definition_,
    bool allow_server_credentials_in_user_queries_)
    : ICatalog("")
    , DB::WithContext(context_)
    , log(getLogger("GlueCatalog(" + settings_.region + ")"))
    , region(settings_.region)
    , settings(settings_)
    , table_engine_definition(table_engine_definition_)
{
    DB::S3::CredentialsConfiguration creds_config;
    creds_config.use_environment_credentials = true;
    creds_config.role_arn = settings.aws_role_arn;
    creds_config.role_session_name = settings.aws_role_session_name;
    creds_config.external_id = settings.aws_external_id;

    /// A Glue catalog is created by user SQL, so it must not reuse the server's credentials unless allowed at
    /// CREATE time. The cached catalog uses the global context, so pass the value captured then, not the live setting.
    creds_config.forbid_implicit_credentials
        = getContext()->shouldRestrictUserQueryS3Credentials(allow_server_credentials_in_user_queries_);

    const auto & server_settings = getContext()->getGlobalContext()->getServerSettings();
    const DB::Settings & global_settings = getContext()->getGlobalContext()->getSettingsRef();

    int s3_max_redirects = static_cast<int>(server_settings[DB::ServerSetting::s3_max_redirects]);
    // just for compatibility with old setting
    if (global_settings.isChanged("s3_max_redirects"))
        s3_max_redirects = static_cast<int>(global_settings[DB::Setting::s3_max_redirects]);

    int s3_retry_attempts = static_cast<int>(server_settings[DB::ServerSetting::s3_retry_attempts]);
    // just for compatibility with old setting
    if (global_settings.isChanged("s3_retry_attempts"))
        s3_retry_attempts = static_cast<int>(global_settings[DB::Setting::s3_retry_attempts]);

    bool s3_slow_all_threads_after_network_error = global_settings[DB::Setting::s3_slow_all_threads_after_network_error];
    bool s3_slow_all_threads_after_retryable_error = false;
    bool enable_s3_requests_logging = global_settings[DB::Setting::enable_s3_requests_logging];

    DB::S3::PocoHTTPClientConfiguration poco_config = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        getContext()->getRemoteHostFilter(),
        s3_max_redirects,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = static_cast<unsigned>(s3_retry_attempts)},
        s3_slow_all_threads_after_network_error,
        s3_slow_all_threads_after_retryable_error,
        enable_s3_requests_logging,
        /* group = */ DB::HTTPConnectionGroupType::STORAGE,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});


    Aws::Glue::GlueClientConfiguration client_configuration;
    client_configuration.connectTimeoutMs = static_cast<unsigned>(global_settings[DB::Setting::s3_connect_timeout_ms]);
    client_configuration.requestTimeoutMs = static_cast<unsigned>(global_settings[DB::Setting::s3_request_timeout_ms]);
    client_configuration.region = region;
    client_configuration.retryStrategy = std::make_shared<GlueRetryStrategy>();
    auto endpoint_provider = std::make_shared<Aws::Glue::GlueEndpointProvider>();

    Aws::Auth::AWSCredentials credentials(settings_.aws_access_key_id, settings_.aws_secret_access_key);
    /// Only for testing when we are mocking glue
    if (!endpoint.empty())
    {
        client_configuration.endpointOverride = endpoint;
        endpoint_provider->OverrideEndpoint(endpoint);

        if (credentials.IsEmpty() && !creds_config.forbid_implicit_credentials)
        {
            /// Placeholder key for mocked moto glue (must be non-empty). Skipped under the restriction with no
            /// explicit credentials, so it does not mask the refusal that the test exercises.
            credentials.SetAWSAccessKeyId("testing");
            credentials.SetAWSSecretKey("testing");
        }

        Poco::URI uri(endpoint);
        if (uri.getScheme() == "http")
            poco_config.scheme = Aws::Http::Scheme::HTTP;
    }
    else
    {
        LOG_TRACE(log, "Creating AWS glue client with credentials empty {}, region '{}', endpoint '{}'", credentials.IsEmpty(), region, endpoint);
    }

    credentials_provider = DB::S3::getCredentialsProvider(poco_config, credentials, creds_config);
    glue_client = std::make_unique<Aws::Glue::GlueClient>(credentials_provider, endpoint_provider, client_configuration);

}

GlueCatalog::~GlueCatalog() = default;

DataLake::ICatalog::Namespaces GlueCatalog::getDatabases(const std::string & prefix, size_t limit) const
{
    DataLake::ICatalog::Namespaces result;
    Aws::Glue::Model::GetDatabasesRequest request;
    if (limit != 0)
        request.SetMaxResults(static_cast<int>(limit));

    LOG_TEST(log, "Getting databases for prefix '{}'", prefix);
    std::string next_token;
    do
    {
        request.SetNextToken(next_token);
        auto outcome = glue_client->GetDatabases(request);
        if (outcome.IsSuccess())
        {
            const auto & databases_result = outcome.GetResult();
            const std::vector<Aws::Glue::Model::Database> & dbs = databases_result.GetDatabaseList();
            LOG_TEST(log, "Success getting databases for prefix '{}', total dbs {}", prefix, dbs.size());
            for (const auto & db : dbs)
            {
                const auto & db_name = db.GetName();
                if (!db_name.starts_with(prefix))
                    continue;
                result.push_back(db_name);
                if (limit != 0 && result.size() >= limit)
                    break;
            }

            if (limit != 0 && result.size() >= limit)
                break;

            next_token = databases_result.GetNextToken();
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Exception calling GetTables {}", outcome.GetError().GetMessage());
        }
    }
    while (!next_token.empty());

    return result;
}

CatalogTables GlueCatalog::getTablesForDatabase(const std::string & db_name, size_t limit) const
{
    LOG_TEST(log, "Getting tables for database '{}' with limit {}", db_name, limit);
    CatalogTables result;
    Aws::Glue::Model::GetTablesRequest request;
    request.SetDatabaseName(db_name);
    if (limit != 0)
        request.SetMaxResults(static_cast<int>(limit));

    fiu_do_on(DB::FailPoints::check_database_datalake_negative,
    {
        throw DB::Exception(DB::ErrorCodes::FAULT_INJECTED, "Injecting fault when checking database");
    });

    std::string next_token;
    do
    {
        request.SetNextToken(next_token);
        auto outcome = glue_client->GetTables(request);
        if (outcome.IsSuccess())
        {
            const auto & tables_result = outcome.GetResult();
            const std::vector<Aws::Glue::Model::Table> & tables = tables_result.GetTableList();
            LOG_TEST(log, "Success getting table for database '{}', total tables {}", db_name, tables.size());
            for (const auto & table : tables)
            {
                /// For some reason glue allow to have empty tables
                /// without any columns. They are also empty in object
                /// storage, so just ignore them.
                if (table.GetStorageDescriptor().GetColumns().empty() && !isReadableGlueTable(table))
                    continue;

                if (limit != 0 && result.size() >= limit)
                    break;
                result.push_back(CatalogTable{
                    .name = db_name + "." + table.GetName(),
                    .is_readable = isReadableGlueTable(table),
                });
            }
            next_token = tables_result.GetNextToken();
        }
        else
        {
            /// The database is absent from Glue, so it contributes no tables. The error names the database, so
            /// any pages already collected belong to a database that is gone and are dropped too.
            if (outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND)
                return {};

            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Exception calling GetTables {}", outcome.GetError().GetMessage());
        }
        if (limit != 0 && result.size() >= limit)
            break;
    }
    while (!next_token.empty());

    return result;
}

CatalogTables GlueCatalog::getTables() const
{
    auto databases = getDatabases("");

    /// Glue has no call that lists the tables of every database, so this issues one `GetTables` request per
    /// database. Issuing them one after another makes `SHOW TABLES` take seconds to minutes in an account
    /// with hundreds of databases, so they run in parallel, like the per-namespace listing of `RestCatalog`.
    /// Every database writes to its own slot, which keeps the result in the order of `GetDatabases`.
    std::vector<CatalogTables> tables_by_database(databases.size());
    {
        auto & pool = getContext()->getIcebergCatalogThreadpool();
        DB::ThreadPoolCallbackRunnerLocal<void> runner(pool, DB::ThreadName::DATALAKE_GLUE_CATALOG);
        /// Reserve tracking capacity, so `enqueueAndKeepTrack` cannot schedule a job and then fail to track it.
        runner.reserve(databases.size());
        for (size_t i = 0; i < databases.size(); ++i)
            runner.enqueueAndKeepTrack([this, &databases, &tables_by_database, i] { tables_by_database[i] = getTablesForDatabase(databases[i]); });
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    CatalogTables result;
    for (auto & tables_in_database : tables_by_database)
        std::move(tables_in_database.begin(), tables_in_database.end(), std::back_inserter(result));
    return result;
}

DataLake::ICatalog::Namespaces GlueCatalog::getNamespaces() const
{
    /// Glue databases are flat — they cannot contain nested namespaces.
    return getDatabases("");
}

CatalogTables GlueCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    return getTablesForDatabase(namespace_name);
}

bool GlueCatalog::existsTable(const std::string & database_name, const std::string & table_name) const
{
    TableMetadata metadata;
    return tryGetTableMetadata(database_name, table_name, metadata);
}

bool GlueCatalog::tryGetTableMetadata(
    const std::string & database_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    Aws::Glue::Model::GetTableRequest request;
    request.SetDatabaseName(database_name);
    request.SetName(table_name);

    auto outcome = glue_client->GetTable(request);
    if (outcome.IsSuccess())
    {
        const auto & table_outcome = outcome.GetResult().GetTable();
        if (result.requiresLocation())
            result.setLocation(table_outcome.GetStorageDescriptor().GetLocation());

        std::string table_type;
        if (table_outcome.GetParameters().contains("table_type"))
            table_type = table_outcome.GetParameters().at("table_type");

        if (!isReadableGlueTable(table_outcome))
        {
            std::string message_part;
            if (!table_type.empty())
                message_part = "unsupported table_type '" + table_type + "'";
            else
                message_part = "no table_type";

            result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has {}. " \
                   "It means that it's unreadable with Glue catalog in ClickHouse, readable tables must have equalsIgnoreCase(table_type, '{}')",
                   database_name + "." + table_name, message_part, "ICEBERG"));
        }

        if (result.requiresCredentials())
            setCredentials(result);

        auto setup_specific_properties = [&]
        {
            const auto & table_params = table_outcome.GetParameters();
            if (table_params.contains("metadata_location"))
            {
                result.setDataLakeSpecificProperties(DataLakeSpecificProperties{.iceberg_metadata_file_location = table_params.at("metadata_location")});
            }
            else if (const auto & location = table_outcome.GetStorageDescriptor().GetLocation(); !location.empty())
            {
                String location_with_slash = location;
                if (!location_with_slash.ends_with('/'))
                    location_with_slash += '/';

                String resolved_metadata_path = resolveMetadataPathFromTableLocation(location_with_slash, result);
                if (resolved_metadata_path.empty())
                {
                    result.setTableIsNotReadable(fmt::format("Could not determine metadata_location of table `{}`. ",
                        database_name + "." + table_name));
                }
                else
                {
                    result.setDataLakeSpecificProperties(DataLakeSpecificProperties{.iceberg_metadata_file_location = resolved_metadata_path});
                }
            }
            else
            {
                result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has no metadata_location and Location. " \
                    "It means that it's unreadable with Glue catalog in ClickHouse, " \
                    "readable tables must have 'metadata_location' or `Location` in table metadata",
                    database_name + "." + table_name));
            }
        };

        if (result.requiresDataLakeSpecificProperties())
            setup_specific_properties();

        if (result.requiresSchema())
        {
            if (isReadableGlueTable(table_outcome))
            {
                /// Reject empty current Glue names even when Iceberg metadata supplies the types.
                size_t position = 0;
                for (const auto & column : table_outcome.GetStorageDescriptor().GetColumns())
                {
                    const auto & parameters = column.GetParameters();
                    if (auto it = parameters.find("iceberg.field.current"); it != parameters.end() && it->second == "false")
                        continue;

                    if (column.GetName().empty())
                        throw DB::Exception(
                            DB::ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                            "Column name in data lake catalog table schema cannot be empty (field at position {})",
                            position);
                    ++position;
                }

                if (!result.hasDataLakeSpecificProperties())
                    setup_specific_properties();

                if (auto properties = result.getDataLakeSpecificProperties();
                    properties && !properties->iceberg_metadata_file_location.empty())
                {
                    /// Glue types are lossy and nested names are unescaped, so the table storage infers the schema from `metadata.json`.
                    result.setSchema({});
                    return true;
                }
            }

            DB::NamesAndTypesList schema;
            auto columns = table_outcome.GetStorageDescriptor().GetColumns();
            for (const auto & column : columns)
            {
                const auto column_params = column.GetParameters();
                bool can_be_nullable = column_params.contains("iceberg.field.optional") && column_params.at("iceberg.field.optional") == "true";

                /// Skip field if it's not "current" (for example Renamed). No idea how someone can utilize "non current fields" but for some reason
                /// they are returned by Glue API. So if you do "RENAME COLUMN a to new_a" glue will return two fields: a and new_a.
                /// And a will be marked as "non current" field.
                if (column_params.contains("iceberg.field.current") && column_params.at("iceberg.field.current") == "false")
                    continue;

                String column_type = column.GetType();
                if (column_type == "timestamp_nano")
                    column_type = DB::Iceberg::f_timestamp_ns;

                schema.push_back({column.GetName(), getType(column_type, can_be_nullable, getContext()->getSettingsRef()[DB::Setting::iceberg_time_as_int64])});
            }
            result.setSchema(schema);
        }
    }
    else
    {
        if (outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND)
            return false; // Table does not exist

        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Exception calling GetTable for table {}: {}",
            database_name + "." + table_name, outcome.GetError().GetMessage());
    }

    return true;
}

void GlueCatalog::getTableMetadata(
    const std::string & database_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!tryGetTableMetadata(database_name, table_name, result))
    {
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Table {} does not exist in Glue catalog",
            database_name + "." + table_name);
    }
}

void GlueCatalog::setCredentials(TableMetadata & metadata) const
{
    auto storage_type = parseStorageTypeFromLocation(metadata.getLocation());

    if (storage_type == StorageType::S3)
    {
        auto credentials = credentials_provider->GetAWSCredentials();
        auto s3_creds = std::make_shared<S3Credentials>(credentials.GetAWSAccessKeyId(), credentials.GetAWSSecretKey(), credentials.GetSessionToken());
        metadata.setStorageCredentials(s3_creds);
    }
    else
    {
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Glue catalog support S3 backend for data storage only");
    }
}

ICatalog::CredentialsRefreshCallback GlueCatalog::getCredentialsConfigurationCallback(
    const DB::StorageID & storage_id, const TableMetadata & /* table_metadata */)
{
    /// The AWS SDK credentials provider chain (instance profile, STS assume-role,
    /// web-identity, etc.) refreshes its cached credentials internally before
    /// expiry. The bug we are fixing is that `setCredentials` captures the result
    /// of `GetAWSCredentials` once at table-load time and embeds the access key,
    /// secret, and session token as static literals in the storage args, so the
    /// S3 client is pinned to a snapshot that goes stale on long reads. This
    /// callback re-asks the same provider for current credentials each time
    /// `ReadBufferFromS3` reports an `ExpiredToken`, letting the read recover.
    return [this, storage_id]() -> std::shared_ptr<IStorageCredentials>
    {
        LOG_DEBUG(log, "Refreshing AWS credentials for {} after expired token", storage_id.getNameForLogs());
        auto credentials = credentials_provider->GetAWSCredentials();
        return std::make_shared<S3Credentials>(
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            credentials.GetSessionToken());
    };
}

bool GlueCatalog::empty() const
{
    auto all_databases = getDatabases("");
    for (const auto & db : all_databases)
    {
        if (!getTablesForDatabase(db, /* limit = */ 1).empty())
            return false;
    }
    return true;
}

GlueCatalog::ObjectStorageWithPath GlueCatalog::createObjectStorageForEarlyTableAccess(const String & s3_location, const TableMetadata & table_metadata) const
{
    DB::ASTStorage * storage = table_engine_definition->as<DB::ASTStorage>();
    DB::ASTs args = storage->engine->arguments->children;

    String storage_endpoint = !settings.storage_endpoint.empty() ? settings.storage_endpoint : encodeLocationForURI(s3_location);
    if (args.empty())
        args.emplace_back(DB::make_intrusive<DB::ASTLiteral>(storage_endpoint));
    else
        args[0] = DB::make_intrusive<DB::ASTLiteral>(storage_endpoint);

    if (args.size() == 1)
    {
        if (table_metadata.hasStorageCredentials())
        {
            table_metadata.getStorageCredentials()->addCredentialsToEngineArgs(args);
        }
        else
        {
            auto credentials = credentials_provider->GetAWSCredentials();
            DataLake::S3Credentials(credentials.GetAWSAccessKeyId(), credentials.GetAWSSecretKey(), credentials.GetSessionToken()).addCredentialsToEngineArgs(args);
        }
    }

    auto storage_settings = std::make_shared<DB::DataLakeStorageSettings>();
    storage_settings->loadFromSettingsChanges(settings.allChanged());
    auto configuration = std::make_shared<DB::StorageS3IcebergConfiguration>(storage_settings);
    DB::StorageObjectStorageConfiguration::initialize(*configuration, args, getContext(), false);

    auto object_storage = configuration->createObjectStorage(getContext(), true, {});

    /// Parse S3 path to extract bucket and table path
    String table_path = s3_location;
    if (table_path.starts_with("s3://"))
        table_path = table_path.substr(5);
    else if (table_path.starts_with("s3:/"))
        table_path = table_path.substr(4);

    std::size_t pos = table_path.find('/');
    String bucket_name;
    if (pos != std::string::npos)
    {
        bucket_name = table_path.substr(0, pos);
        table_path = table_path.substr(pos + 1);
    }

    if (table_path.ends_with('/'))
        table_path = table_path.substr(0, table_path.size() - 1);

    return {object_storage, bucket_name, table_path};
}

String GlueCatalog::resolveMetadataPathFromTableLocation(const String & table_location, const TableMetadata & table_metadata) const
{
    auto [object_storage, bucket_name, table_path] = createObjectStorageForEarlyTableAccess(table_location, table_metadata);

    auto storage_settings = std::make_shared<DB::DataLakeStorageSettings>();
    storage_settings->loadFromSettingsChanges(settings.allChanged());

    auto resolve_context = DB::Context::createCopy(getContext());
    DB::Settings resolve_settings = resolve_context->getSettingsCopy();
    resolve_settings[DB::Setting::iceberg_list_only_v_prefixed_metadata_files] = false;
    resolve_context->setSettings(resolve_settings);

    try
    {
        auto [metadata_version, metadata_path, compression_method, _] = DB::Iceberg::getLatestOrExplicitMetadataFileAndVersion(
            object_storage, table_path, *storage_settings, nullptr, resolve_context, log.get(), std::nullopt, DB::CompressionMethod::None);

        LOG_TRACE(log, "Resolved metadata path '{}' (version {}) for table location '{}'", metadata_path, metadata_version, table_location);

        return "s3://" + bucket_name + "/" + metadata_path;
    }
    catch (const DB::Exception & e)
    {
        LOG_TRACE(log, "Failed to resolve metadata path from table location '{}': {}", table_location, e.what());
        return "";
    }
}

void GlueCatalog::createNamespaceIfNotExists(const String & namespace_name) const
{
    Aws::Glue::Model::CreateDatabaseRequest create_request;
    Aws::Glue::Model::DatabaseInput db_input;
    db_input.SetName(namespace_name);
    create_request.SetDatabaseInput(db_input);

    auto outcome = glue_client->CreateDatabase(create_request);
    if (!outcome.IsSuccess() && outcome.GetError().GetErrorType() != Aws::Glue::GlueErrors::ALREADY_EXISTS)
    {
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Exception calling CreateDatabase for namespace {}: {}",
            namespace_name, outcome.GetError().GetMessage());
    }
}

std::optional<std::string> GlueCatalog::getDefaultTableLocation(
    const std::string & namespace_name,
    const std::string & table_name) const
{
    Aws::Glue::Model::GetDatabaseRequest request;
    request.SetName(namespace_name);

    auto outcome = glue_client->GetDatabase(request);
    if (!outcome.IsSuccess())
    {
        if (outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ENTITY_NOT_FOUND)
            return std::nullopt;

        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Exception calling GetDatabase for namespace {}: {}",
            namespace_name, outcome.GetError().GetMessage());
    }

    const auto & location_uri = outcome.GetResult().GetDatabase().GetLocationUri();
    if (location_uri.empty())
        return std::nullopt;

    return std::string(std::filesystem::path(location_uri) / table_name);
}

namespace
{

constexpr size_t MAX_UPDATE_TABLE_ATTEMPTS = 5;

/// Remove `metadata/vN.metadata.json` from the S3 path to get the table location.
String tableLocationFromMetadataPath(const String & metadata_path)
{
    return fs::path(metadata_path).parent_path().parent_path().string();
}

/// Glue limits `Column.Comment` to 255 characters (counted in UTF-16 code units) and allows no control characters
/// except tab, so a multiline or long Iceberg field `doc` would make `CreateTable` and `UpdateTable` fail.
/// The full `doc` stays in the Iceberg metadata; Glue only gets a display copy.
String glueColumnComment(const String & doc)
{
    static constexpr size_t max_comment_length = 255;

    String result;
    result.reserve(std::min(doc.size(), max_comment_length * 4));
    size_t length = 0;
    size_t pos = 0;
    while (pos < doc.size())
    {
        const auto lead = static_cast<unsigned char>(doc[pos]);
        size_t sequence_size = 1;
        if (lead >= 0xF0)
            sequence_size = 4;
        else if (lead >= 0xE0)
            sequence_size = 3;
        else if (lead >= 0xC0)
            sequence_size = 2;
        sequence_size = std::min(sequence_size, doc.size() - pos);

        /// Characters outside the Basic Multilingual Plane take two UTF-16 code units.
        const size_t units = sequence_size == 4 ? 2 : 1;
        if (length + units > max_comment_length)
            break;

        if (lead < 0x20 && lead != '\t')
            result += ' ';
        else
            result.append(doc, pos, sequence_size);

        length += units;
        pos += sequence_size;
    }
    return result;
}

/// `s3a://bucket/table/` -> `bucket/table`, because the scheme of a table location is not normalized.
std::string_view locationWithoutSchemeAndTrailingSlash(std::string_view location)
{
    if (auto pos = location.find("://"); pos != std::string_view::npos)
        location.remove_prefix(pos + 3);
    while (location.ends_with('/'))
        location.remove_suffix(1);
    return location;
}

/// Whether a table without `metadata_location` is still the table the commit was built on. `getTableMetadata` resolves
/// the base of such a table from the latest metadata file under its location, so it must be an Iceberg table at the same location.
bool isSamePointerlessTable(const Aws::Glue::Model::Table & table, const String & base_catalog_metadata_location)
{
    return isReadableGlueTable(table)
        && locationWithoutSchemeAndTrailingSlash(table.GetStorageDescriptor().GetLocation())
            == locationWithoutSchemeAndTrailingSlash(tableLocationFromMetadataPath(base_catalog_metadata_location));
}

}

Poco::JSON::Object::Ptr GlueCatalog::getCurrentSchema(const Poco::JSON::Object::Ptr & metadata_object)
{
    if (!metadata_object)
        return nullptr;

    /// Same order as `IcebergMetadata::parseTableSchema`: the deprecated v1 `schema` may be stale, so `schemas` wins.
    if (metadata_object->has(DB::Iceberg::f_schemas))
        return DB::Iceberg::parseTableSchemaV2Method(metadata_object).first;

    /// Format version 1 metadata may carry a single top-level `schema` instead.
    if (metadata_object->has(DB::Iceberg::f_schema))
        return metadata_object->getObject(DB::Iceberg::f_schema);

    return nullptr;
}

Aws::Vector<Aws::Glue::Model::Column> GlueCatalog::glueColumnsFromIcebergSchema(const Poco::JSON::Object::Ptr & schema)
{
    Aws::Vector<Aws::Glue::Model::Column> columns;
    if (!schema || !schema->has(DB::Iceberg::f_fields))
        return columns;

    auto fields = schema->getArray(DB::Iceberg::f_fields);
    columns.reserve(fields->size());
    for (size_t i = 0; i < fields->size(); ++i)
    {
        auto field = fields->getObject(static_cast<UInt32>(i));

        Aws::Glue::Model::Column column;
        column.SetName(field->getValue<String>(DB::Iceberg::f_name));
        column.SetType(icebergTypeToGlueType(field->get(DB::Iceberg::f_type)));
        if (field->has("doc") && !field->isNull("doc"))
            column.SetComment(glueColumnComment(field->getValue<String>("doc")));

        const bool required = field->has(DB::Iceberg::f_required) && field->getValue<bool>(DB::Iceberg::f_required);

        Aws::Map<Aws::String, Aws::String> column_parameters;
        column_parameters["iceberg.field.id"] = std::to_string(field->getValue<Int64>(DB::Iceberg::f_id));
        column_parameters["iceberg.field.optional"] = required ? "false" : "true";
        column_parameters["iceberg.field.current"] = "true";
        column.SetParameters(std::move(column_parameters));

        columns.push_back(std::move(column));
    }
    return columns;
}

void GlueCatalog::createTable(const String & namespace_name, const String & table_name, const String & new_metadata_path, Poco::JSON::Object::Ptr metadata_content) const
{
    Aws::Glue::Model::CreateTableRequest request;
    request.SetDatabaseName(namespace_name);

    Aws::Glue::Model::TableInput table_input;
    table_input.SetName(table_name);

    Aws::Glue::Model::StorageDescriptor sd;
    sd.SetLocation(tableLocationFromMetadataPath(new_metadata_path));
    /// Athena cannot read a Glue table without columns in its storage descriptor.
    sd.SetColumns(glueColumnsFromIcebergSchema(getCurrentSchema(metadata_content)));

    table_input.SetStorageDescriptor(sd);
    table_input.SetTableType("ICEBERG");

    Aws::Map<Aws::String, Aws::String> parameters;
    parameters["metadata_location"] = new_metadata_path;
    parameters["table_type"] = "ICEBERG";

    table_input.SetParameters(parameters);

    request.SetTableInput(table_input);

    auto response = glue_client->CreateTable(request);

    if (!response.IsSuccess())
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Can not create metadata in glue catalog: {}", response.GetError().GetMessage());
}

Aws::Glue::Model::TableInput GlueCatalog::buildUpdatedTableInput(
    const Aws::Glue::Model::Table & existing,
    const String & new_metadata_path,
    const Poco::JSON::Object::Ptr & new_schema,
    const Poco::JSON::Object::Ptr & new_metadata)
{
    Aws::Glue::Model::TableInput table_input;
    table_input.SetName(existing.GetName());

    /// Preserve existing Glue attributes when advancing the metadata pointer.
    Aws::Glue::Model::StorageDescriptor sd = existing.GetStorageDescriptor();
    Aws::Map<Aws::String, Aws::String> parameters = existing.GetParameters();
    if (!existing.GetDescription().empty())
        table_input.SetDescription(existing.GetDescription());
    if (!existing.GetOwner().empty())
        table_input.SetOwner(existing.GetOwner());
    if (existing.RetentionHasBeenSet())
        table_input.SetRetention(existing.GetRetention());
    if (existing.PartitionKeysHasBeenSet())
        table_input.SetPartitionKeys(existing.GetPartitionKeys());
    table_input.SetTableType(existing.GetTableType().empty() ? "ICEBERG" : existing.GetTableType());

    if (auto it = parameters.find("metadata_location"); it != parameters.end() && !it->second.empty())
        parameters["previous_metadata_location"] = it->second;

    sd.SetLocation(tableLocationFromMetadataPath(new_metadata_path));
    auto schema = new_schema ? new_schema : getCurrentSchema(new_metadata);
    if (!schema || !schema->has(DB::Iceberg::f_fields))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No current Iceberg schema in {}", new_metadata_path);
    sd.SetColumns(glueColumnsFromIcebergSchema(schema));
    table_input.SetStorageDescriptor(sd);

    parameters["metadata_location"] = new_metadata_path;
    parameters["table_type"] = "ICEBERG";
    table_input.SetParameters(parameters);

    return table_input;
}

bool GlueCatalog::commitTableUpdate(
    const String & namespace_name,
    const String & table_name,
    const String & new_metadata_path,
    const String & base_catalog_metadata_location,
    const Poco::JSON::Object::Ptr & new_schema,
    const Poco::JSON::Object::Ptr & new_metadata) const
{
    bool update_may_have_landed = false;
    for (size_t attempt = 0; attempt < MAX_UPDATE_TABLE_ATTEMPTS; ++attempt)
    {
        Aws::Glue::Model::GetTableRequest get_request;
        get_request.SetDatabaseName(namespace_name);
        get_request.SetName(table_name);
        auto get_outcome = glue_client->GetTable(get_request);
        if (!get_outcome.IsSuccess())
            throw DB::Exception(
                DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
                "Exception calling GetTable for table {}.{}: {}",
                namespace_name, table_name, get_outcome.GetError().GetMessage());

        const auto & existing = get_outcome.GetResult().GetTable();
        const auto & parameters = existing.GetParameters();
        auto current_it = parameters.find("metadata_location");
        const String current_metadata_path = current_it == parameters.end() ? "" : current_it->second;

        /// An earlier `UpdateTable` landed, but its response was lost and the retry was rejected by `VersionId`.
        if (current_metadata_path == new_metadata_path)
            return true;

        /// `new_metadata_path` is already published, so the pointer must still be at the base it was built on.
        /// Another pointer means a different commit has won or the table was recreated, even if its file has the
        /// same version. Then nothing references our file and `false` lets the caller rebuild on top of the winner.
        /// A missing pointer is a conflict too, unless the base was resolved from the location of the same pointerless table.
        const bool same_base = current_metadata_path.empty()
            ? isSamePointerlessTable(existing, base_catalog_metadata_location)
            : current_metadata_path == base_catalog_metadata_location;
        if (!same_base)
        {
            if (!update_may_have_landed)
            {
                LOG_DEBUG(log, "Glue table {}.{} points to {} instead of {}, commit of {} conflicts",
                    namespace_name, table_name, current_metadata_path, base_catalog_metadata_location, new_metadata_path);
                return false;
            }

            /// After an `UpdateTable` was sent, it may have landed and been built upon, so it must not be cleaned up.
            auto previous_it = parameters.find("previous_metadata_location");
            if (previous_it != parameters.end() && previous_it->second == new_metadata_path)
                return true;

            throw DB::Exception(
                DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
                "Unknown outcome of commit of {} to Glue table {}.{}: it now points to {}",
                new_metadata_path, namespace_name, table_name, current_metadata_path);
        }

        DB::FailPointInjection::pauseFailPoint(DB::FailPoints::glue_catalog_pause_before_update_table);

        Aws::Glue::Model::UpdateTableRequest request;
        request.SetDatabaseName(namespace_name);
        request.SetTableInput(buildUpdatedTableInput(existing, new_metadata_path, new_schema, new_metadata));
        /// Without it, a delayed retry could move the pointer back and drop the columns of a newer commit.
        if (existing.VersionIdHasBeenSet())
            request.SetVersionId(existing.GetVersionId());
        bool request_retried = false;
        request.SetRequestRetryHandler([&request_retried](const Aws::AmazonWebServiceRequest &) { request_retried = true; });

        auto response = glue_client->UpdateTable(request);
        if (response.IsSuccess())
            return true;

        if (response.GetError().GetErrorType() != Aws::Glue::GlueErrors::CONCURRENT_MODIFICATION)
            throw DB::Exception(
                DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
                "Can not update metadata of table {}.{} in Glue catalog: {}",
                namespace_name, table_name, response.GetError().GetMessage());

        /// A rejected single attempt did not land; after an SDK retry the rejection may come from our own landed attempt.
        update_may_have_landed = update_may_have_landed || request_retried;
        LOG_DEBUG(log, "Glue table {}.{} was modified concurrently, retrying commit of {}", namespace_name, table_name, new_metadata_path);
    }

    throw DB::Exception(
        DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
        "Can not update metadata of table {}.{} in Glue catalog: it was modified concurrently {} times",
        namespace_name, table_name, MAX_UPDATE_TABLE_ATTEMPTS);
}

bool GlueCatalog::updateMetadata(
    const String & namespace_name,
    const String & table_name,
    const String & new_metadata_path,
    const String & base_catalog_metadata_location,
    Poco::JSON::Object::Ptr /*new_snapshot*/,
    Poco::JSON::Object::Ptr new_metadata) const
{
    return commitTableUpdate(namespace_name, table_name, new_metadata_path, base_catalog_metadata_location, nullptr, new_metadata);
}

bool GlueCatalog::updateSchema(
    const String & namespace_name,
    const String & table_name,
    const String & new_metadata_path,
    const String & base_catalog_metadata_location,
    Poco::JSON::Object::Ptr new_schema,
    Int32 /*previous_schema_id*/) const
{
    return commitTableUpdate(namespace_name, table_name, new_metadata_path, base_catalog_metadata_location, new_schema, nullptr);
}

void GlueCatalog::dropTable(const String & namespace_name, const String & table_name, bool /*delete_data*/) const
{
    Aws::Glue::Model::DeleteTableRequest request;
    request.SetDatabaseName(namespace_name);
    request.SetName(table_name);

    auto response = glue_client->DeleteTable(request);

    if (!response.IsSuccess())
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Can not delete table from glue catalog: {}",
            response.GetError().GetMessage());
}

}

#endif
