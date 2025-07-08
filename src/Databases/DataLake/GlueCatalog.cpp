#include <Databases/DataLake/GlueCatalog.h>
#include <Poco/JSON/Object.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#if USE_AWS_S3 && USE_AVRO

#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>

#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
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


#include <IO/S3/Credentials.h>
#include <IO/S3/Client.h>
#include <IO/S3Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Databases/DataLake/Common.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATALAKE_DATABASE_ERROR;
}

namespace DB::Setting
{
    extern const SettingsUInt64 s3_max_connections;
    extern const SettingsUInt64 s3_max_redirects;
    extern const SettingsUInt64 s3_retry_attempts;
    extern const SettingsBool s3_slow_all_threads_after_network_error;
    extern const SettingsBool enable_s3_requests_logging;
    extern const SettingsUInt64 s3_connect_timeout_ms;
    extern const SettingsUInt64 s3_request_timeout_ms;
}

namespace DB::StorageObjectStorageSetting
{
    extern const StorageObjectStorageSettingsString iceberg_metadata_file_path;
}

namespace DB::DatabaseDataLakeSetting
{
    extern const DatabaseDataLakeSettingsString storage_endpoint;
    extern const DatabaseDataLakeSettingsString aws_access_key_id;
    extern const DatabaseDataLakeSettingsString aws_secret_access_key;
    extern const DatabaseDataLakeSettingsString region;
}

namespace CurrentMetrics
{
    extern const Metric MarkCacheBytes;
    extern const Metric MarkCacheFiles;
}

namespace DataLake
{

GlueCatalog::GlueCatalog(
    const String & endpoint,
    DB::ContextPtr context_,
    const DB::DatabaseDataLakeSettings & settings_,
    DB::ASTPtr table_engine_definition_)
    : ICatalog("")
    , DB::WithContext(context_)
    , log(getLogger("GlueCatalog(" + settings_[DB::DatabaseDataLakeSetting::region].value + ")"))
    , credentials(settings_[DB::DatabaseDataLakeSetting::aws_access_key_id].value, settings_[DB::DatabaseDataLakeSetting::aws_secret_access_key].value)
    , region(settings_[DB::DatabaseDataLakeSetting::region].value)
    , settings(settings_)
    , table_engine_definition(table_engine_definition_)
    , metadata_objects(CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, 1024)
{
    DB::S3::CredentialsConfiguration creds_config;
    creds_config.use_environment_credentials = true;

    const DB::Settings & global_settings = getContext()->getGlobalContext()->getSettingsRef();

    int s3_max_redirects = static_cast<int>(global_settings[DB::Setting::s3_max_redirects]);
    int s3_retry_attempts = static_cast<int>(global_settings[DB::Setting::s3_retry_attempts]);
    bool s3_slow_all_threads_after_network_error = global_settings[DB::Setting::s3_slow_all_threads_after_network_error];
    bool enable_s3_requests_logging = global_settings[DB::Setting::enable_s3_requests_logging];

    DB::S3::PocoHTTPClientConfiguration poco_config = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        getContext()->getRemoteHostFilter(),
        s3_max_redirects,
        s3_retry_attempts,
        s3_slow_all_threads_after_network_error,
        enable_s3_requests_logging,
        false,
        nullptr,
        nullptr
    );

    Aws::Glue::GlueClientConfiguration client_configuration;
    client_configuration.maxConnections = static_cast<unsigned>(global_settings[DB::Setting::s3_max_connections]);
    client_configuration.connectTimeoutMs = static_cast<unsigned>(global_settings[DB::Setting::s3_connect_timeout_ms]);
    client_configuration.requestTimeoutMs = static_cast<unsigned>(global_settings[DB::Setting::s3_request_timeout_ms]);
    client_configuration.region = region;
    auto endpoint_provider = std::make_shared<Aws::Glue::GlueEndpointProvider>();

    /// Only for testing when we are mocking glue
    if (!endpoint.empty())
    {
        client_configuration.endpointOverride = endpoint;
        endpoint_provider->OverrideEndpoint(endpoint);
        Aws::Auth::AWSCredentials fake_credentials_for_fake_catalog;
        if (credentials.IsEmpty())
        {
            /// You can specify any key for fake moto glue, it's just important
            /// for it not to be empty.
            fake_credentials_for_fake_catalog.SetAWSAccessKeyId("testing");
            fake_credentials_for_fake_catalog.SetAWSSecretKey("testing");
        }
        else
            fake_credentials_for_fake_catalog = credentials;

        glue_client = std::make_unique<Aws::Glue::GlueClient>(fake_credentials_for_fake_catalog, endpoint_provider, client_configuration);
    }
    else
    {
        LOG_TRACE(log, "Creating AWS glue client with credentials empty {}, region '{}', endpoint '{}'", credentials.IsEmpty(), region, endpoint);
        std::shared_ptr<DB::S3::S3CredentialsProviderChain> chain = std::make_shared<DB::S3::S3CredentialsProviderChain>(poco_config, credentials, creds_config);
        glue_client = std::make_unique<Aws::Glue::GlueClient>(chain, endpoint_provider, client_configuration);
    }

}

GlueCatalog::~GlueCatalog() = default;

DataLake::ICatalog::Namespaces GlueCatalog::getDatabases(const std::string & prefix, size_t limit) const
{
    DataLake::ICatalog::Namespaces result;
    Aws::Glue::Model::GetDatabasesRequest request;
    if (limit != 0)
        request.SetMaxResults(limit);

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

DB::Names GlueCatalog::getTablesForDatabase(const std::string & db_name, size_t limit) const
{
    LOG_TEST(log, "Getting tables for database '{}' with limit {}", db_name, limit);
    DB::Names result;
    Aws::Glue::Model::GetTablesRequest request;
    request.SetDatabaseName(db_name);
    if (limit != 0)
        request.SetMaxResults(limit);

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
                if (table.GetStorageDescriptor().GetColumns().empty())
                    continue;

                if (limit != 0 && result.size() >= limit)
                    break;
                result.push_back(db_name + "." + table.GetName());
            }
            next_token = tables_result.GetNextToken();
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Exception calling GetTables {}", outcome.GetError().GetMessage());
        }
        if (limit != 0 && result.size() >= limit)
            break;
    }
    while (!next_token.empty());

    return result;
}

DB::Names GlueCatalog::getTables() const
{
    auto databases = getDatabases("");
    DB::Names result;
    for (const auto & database : databases)
    {
        auto tables_in_database = getTablesForDatabase(database);
        result.insert(result.end(), tables_in_database.begin(), tables_in_database.end());
    }
    return result;
}

bool GlueCatalog::existsTable(const std::string & database_name, const std::string & table_name) const
{
    Aws::Glue::Model::GetTableRequest request;
    request.SetDatabaseName(database_name);
    request.SetName(table_name);

    auto outcome = glue_client->GetTable(request);
    return outcome.IsSuccess();
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

        if (table_type != "ICEBERG")
        {
            std::string message_part;
            if (!table_type.empty())
                message_part = "unsupported table_type '" + table_type + "'";
            else
                message_part = "no table_type";

            result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has {}. " \
                   "It means that it's unreadable with Glue catalog in ClickHouse, readable tables must have table_type == '{}'",
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
            else
            {
                 result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has no metadata_location. " \
                     "It means that it's unreadable with Glue catalog in ClickHouse, readable tables must have 'metadata_location' in table parameters",
                     database_name + "." + table_name));
            }
        };

        if (result.requiresDataLakeSpecificProperties())
            setup_specific_properties();

        if (result.requiresSchema())
        {
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
                if (column_type == "timestamp")
                {
                    if (!result.requiresDataLakeSpecificProperties())
                        setup_specific_properties();
                    if (classifyTimestampTZ(column.GetName(), result))
                        column_type = "timestamptz";
                }

                schema.push_back({column.GetName(), getType(column_type, can_be_nullable)});
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
        auto creds = std::make_shared<S3Credentials>(credentials.GetAWSAccessKeyId(), credentials.GetAWSSecretKey(), credentials.GetSessionToken());
        metadata.setStorageCredentials(creds);
    }
    else
    {
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Glue catalog support S3 backend for data storage only");
    }
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

bool GlueCatalog::classifyTimestampTZ(const String & column_name, const TableMetadata & table_metadata) const
{
    String metadata_path;
    if (auto table_specific_properties = table_metadata.getDataLakeSpecificProperties();
        table_specific_properties.has_value())
    {
        metadata_path = table_specific_properties->iceberg_metadata_file_location;
        if (metadata_path.starts_with("s3:/"))
            metadata_path = metadata_path.substr(5);

        // Delete bucket
        std::size_t pos = metadata_path.find('/');
        if (pos != std::string::npos)
            metadata_path = metadata_path.substr(pos + 1);
    }
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Metadata specific properties should be defined");

    if (!metadata_objects.get(metadata_path))
    {
        DB::ASTStorage * storage = table_engine_definition->as<DB::ASTStorage>();
        DB::ASTs args = storage->engine->arguments->children;

        auto table_endpoint = settings[DB::DatabaseDataLakeSetting::storage_endpoint].value;
        if (args.empty())
            args.emplace_back(std::make_shared<DB::ASTLiteral>(table_endpoint));
        else
            args[0] = std::make_shared<DB::ASTLiteral>(table_endpoint);

        if (args.size() == 1 && table_metadata.hasStorageCredentials())
        {
            auto storage_credentials = table_metadata.getStorageCredentials();
            if (storage_credentials)
                storage_credentials->addCredentialsToEngineArgs(args);
        }

        auto storage_settings = std::make_shared<DB::DataLakeStorageSettings>();
        storage_settings->loadFromSettingsChanges(settings.allChanged());
        auto configuration = std::make_shared<DB::StorageS3IcebergConfiguration>(storage_settings);
        DB::StorageObjectStorage::Configuration::initialize(*configuration, args, getContext(), false);

        auto object_storage = configuration->createObjectStorage(getContext(), true);
        const auto & read_settings = getContext()->getReadSettings();

        DB::StoredObject metadata_stored_object(metadata_path);
        auto read_buf = object_storage->readObject(metadata_stored_object, read_settings);
        String metadata_file;
        readString(metadata_file, *read_buf);

        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(metadata_file);
        auto metadata_object = result.extract<Poco::JSON::Object::Ptr>();
        metadata_objects.set(metadata_path, std::make_shared<Poco::JSON::Object::Ptr>(metadata_object));
    }
    auto metadata_object = *metadata_objects.get(metadata_path);
    auto current_schema_id = metadata_object->getValue<Int64>("current-schema-id");
    auto schemas = metadata_object->getArray(Iceberg::f_schemas);
    for (size_t i = 0; i < schemas->size(); ++i)
    {
        auto schema = schemas->getObject(static_cast<UInt32>(i));
        if (schema->getValue<Int64>("schema-id") == current_schema_id)
        {
            auto fields = schema->getArray(Iceberg::f_fields);
            for (size_t j = 0; j < fields->size(); ++j)
            {
                auto field = fields->getObject(static_cast<UInt32>(j));
                if (field->getValue<String>(Iceberg::f_name) == column_name)
                    return field->getValue<String>(Iceberg::f_type) == Iceberg::f_timestamptz;
            }
        }
    }

    return false;
}


}

#endif
