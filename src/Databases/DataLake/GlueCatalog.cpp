#include <Databases/DataLake/GlueCatalog.h>

#if USE_AWS_S3 && USE_AVRO

#include <aws/glue/GlueClient.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetDatabasesRequest.h>

#include <Common/Exception.h>
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
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
}

namespace DB::Setting
{
    extern const SettingsUInt64 s3_max_connections;
    extern const SettingsUInt64 s3_max_redirects;
    extern const SettingsUInt64 s3_retry_attempts;
    extern const SettingsBool enable_s3_requests_logging;
    extern const SettingsUInt64 s3_connect_timeout_ms;
    extern const SettingsUInt64 s3_request_timeout_ms;
}

namespace DB::StorageObjectStorageSetting
{
    extern const StorageObjectStorageSettingsString iceberg_metadata_file_path;
}

namespace
{

String trim(const String & str)
{
    size_t start = str.find_first_not_of(' ');
    size_t end = str.find_last_not_of(' ');
    return (start == String::npos || end == String::npos) ? "" : str.substr(start, end - start + 1);
}

std::vector<String> splitTypeArguments(const String & type_str)
{
    std::vector<String> args;
    int depth = 0;
    size_t start = 0;
    for (size_t i = 0; i < type_str.size(); i++)
    {
        if (type_str[i] == '<')
            depth++;
        else if (type_str[i] == '>')
            depth--;
        else if (type_str[i] == ',' && depth == 0)
        {
            args.push_back(trim(type_str.substr(start, i - start)));
            start = i + 1;
        }
    }
    args.push_back(trim(type_str.substr(start)));
    return args;
}

// Recursive function to parse types
DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix = "")
{
    String name = trim(type_name);

    if (name.starts_with("array<") && name.ends_with(">"))
    {
        String inner = name.substr(6, name.size() - 7);
        return std::make_shared<DB::DataTypeArray>(getType(inner, nullable));
    }

    if (name.starts_with("map<") && name.ends_with(">"))
    {
        String inner = name.substr(4, name.size() - 5);
        auto args = splitTypeArguments(inner);
        if (args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid data type {}", type_name);

        return std::make_shared<DB::DataTypeMap>(getType(args[0], false), getType(args[1], nullable));
    }

    if (name.starts_with("struct<") && name.ends_with(">"))
    {
        String inner = name.substr(7, name.size() - 8);
        auto args = splitTypeArguments(inner);

        std::vector<String> field_names;
        std::vector<DB::DataTypePtr> field_types;

        for (const auto & arg : args)
        {
            size_t colon = arg.find(':');
            if (colon == String::npos)
                throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid data type {}", type_name);

            String field_name = trim(arg.substr(0, colon));
            String field_type = trim(arg.substr(colon + 1));
            String full_field_name = prefix.empty() ? field_name : prefix + "." + field_name;

            field_names.push_back(full_field_name);
            field_types.push_back(getType(field_type, nullable, full_field_name));
        }
        return std::make_shared<DB::DataTypeTuple>(field_types, field_names);
    }

    return nullable ? DB::makeNullable(DB::IcebergSchemaProcessor::getSimpleType(name)) : DB::IcebergSchemaProcessor::getSimpleType(name);
}

}

namespace DataLake
{

GlueCatalog::GlueCatalog(
    const String & access_key_id,
    const String & secret_access_key,
    const String & region_,
    const String & endpoint,
    DB::ContextPtr context_)
    : ICatalog("")
    , DB::WithContext(context_)
    , log(getLogger("GlueCatalog(" + region_ + ")"))
    , credentials(access_key_id, secret_access_key)
    , region(region_)
{
    DB::S3::CredentialsConfiguration creds_config;
    creds_config.use_environment_credentials = true;

    const DB::Settings & global_settings = getContext()->getGlobalContext()->getSettingsRef();

    int s3_max_redirects = static_cast<int>(global_settings[DB::Setting::s3_max_redirects]);
    int s3_retry_attempts = static_cast<int>(global_settings[DB::Setting::s3_retry_attempts]);
    bool enable_s3_requests_logging = global_settings[DB::Setting::enable_s3_requests_logging];
    DB::S3::PocoHTTPClientConfiguration poco_config = DB::S3::ClientFactory::instance().createClientConfiguration(
        region,
        getContext()->getRemoteHostFilter(),
        s3_max_redirects,
        s3_retry_attempts,
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
    getTableMetadata(database_name, table_name, result);
    return true;
}

void GlueCatalog::getTableMetadata(
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

        if (result.requiresSchema())
        {
            DB::NamesAndTypesList schema;
            auto columns = table_outcome.GetStorageDescriptor().GetColumns();
            for (const auto & column : columns)
            {
                const auto column_params = column.GetParameters();
                bool can_be_nullable = column_params.contains("iceberg.field.optional") && column_params.at("iceberg.field.optional") == "true";
                schema.push_back({column.GetName(), getType(column.GetType(), can_be_nullable)});
            }
            result.setSchema(schema);
        }

        if (result.requiresCredentials())
            setCredentials(result);

        if (result.requiresDataLakeSpecificProperties())
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
        }
    }
    else
    {
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Exception calling GetTable for table {}: {}",
            database_name + "." + table_name, outcome.GetError().GetMessage());
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

}

#endif
