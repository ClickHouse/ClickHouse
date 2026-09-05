#include <Databases/DataLake/IcebergJdbcCatalog.h>

#include "config.h"

#if USE_LIBPQXX && USE_AVRO && USE_AWS_S3

#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Databases/DataLake/StaticStorageCredentials.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/JSON/Object.h>
#include <pqxx/pqxx>

#include <algorithm>


namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Setting
{
extern const SettingsBool allow_experimental_geo_types_in_iceberg;
}

namespace DB
{
namespace DatabaseDataLakeSetting
{
extern const DatabaseDataLakeSettingsString storage_endpoint;
extern const DatabaseDataLakeSettingsS3UriStyle storage_uri_style;
extern const DatabaseDataLakeSettingsBool force_add_bucket;
}
}

namespace CurrentMetrics
{
extern const Metric MarkCacheBytes;
extern const Metric MarkCacheFiles;
}

namespace DataLake
{

namespace
{

constexpr size_t JDBC_PG_POOL_SIZE = 16;
constexpr size_t JDBC_PG_POOL_WAIT_TIMEOUT_MS = 5000;
constexpr size_t JDBC_PG_MAX_TRIES = 3;
constexpr size_t JDBC_PG_CONNECTION_TIMEOUT_S = 10;

}

IcebergJdbcCatalog::IcebergJdbcCatalog(
    const std::string & catalog_name_,
    ConnectionParams params_,
    const DB::DatabaseDataLakeSettings & database_settings_,
    DB::ASTPtr table_engine_definition_,
    DB::ContextPtr context_)
    : ICatalog(catalog_name_)
    , DB::WithContext(context_)
    , params(std::move(params_))
    , database_settings(database_settings_)
    , table_engine_definition(table_engine_definition_)
    , log(getLogger("IcebergJdbcCatalog"))
    , metadata_objects(CurrentMetrics::MarkCacheBytes, CurrentMetrics::MarkCacheFiles, 1024)
{
    if (params.host.empty() || params.database.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "JDBC catalog requires host and database settings");

    /// Check before opening the pool, including after a lazy `ATTACH`.
    context_->getRemoteHostFilter().checkHostAndPort(params.host, std::to_string(params.port));

    DB::StoragePostgreSQL::Configuration pg_configuration;
    pg_configuration.host = params.host;
    pg_configuration.port = params.port;
    pg_configuration.username = params.user;
    pg_configuration.password = params.password;
    pg_configuration.database = params.database;
    pg_configuration.addresses.emplace_back(params.host, params.port);

    pool = std::make_shared<postgres::PoolWithFailover>(
        pg_configuration,
        JDBC_PG_POOL_SIZE,
        JDBC_PG_POOL_WAIT_TIMEOUT_MS,
        JDBC_PG_MAX_TRIES,
        /* auto_close_connection_ */ false,
        JDBC_PG_CONNECTION_TIMEOUT_S);

    /// Fail fast when this is not a JdbcCatalog database, instead of
    /// returning empty listings and confusing "table does not exist" errors.
    auto holder = pool->get();
    pqxx::nontransaction txn(holder->get());
    pqxx::result tables = txn.exec_params(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'iceberg_tables'",
        params.schema);
    if (tables.empty())
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "JDBC catalog: table 'iceberg_tables' not found in Postgres schema '{}'. "
            "Point 'jdbc_schema' at the schema holding the standard Iceberg JdbcCatalog tables",
            params.schema);
    }

    /// V1 adds the `iceberg_type` column distinguishing TABLE from VIEW rows;
    /// accept both schema versions.
    pqxx::result type_column = txn.exec_params(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'iceberg_tables' AND column_name = 'iceberg_type'",
        params.schema);
    has_iceberg_type = !type_column.empty();
}

String IcebergJdbcCatalog::qualified(const std::string & table) const
{
    /// Quote the settings-provided schema the way libpq would: wrap in
    /// double quotes, doubling embedded quotes. Table names are our own
    /// static SQL text.
    String quoted = params.schema;
    size_t pos = 0;
    while ((pos = quoted.find('"', pos)) != String::npos)
    {
        quoted.insert(pos, "\"");
        pos += 2;
    }
    return "\"" + quoted + "\".\"" + table + "\"";
}

String IcebergJdbcCatalog::tableTypePredicate() const
{
    if (!has_iceberg_type)
        return "";
    return " AND (iceberg_type = 'TABLE' OR iceberg_type IS NULL)";
}

bool IcebergJdbcCatalog::empty() const
{
    auto holder = pool->get();
    pqxx::nontransaction txn(holder->get());
    pqxx::result result = txn.exec_params(
        "SELECT 1 FROM " + qualified("iceberg_tables") + " WHERE catalog_name = $1" + tableTypePredicate() + " LIMIT 1",
        warehouse);
    return result.empty();
}

IcebergJdbcCatalog::Namespaces IcebergJdbcCatalog::getNamespaces() const
{
    auto holder = pool->get();
    pqxx::nontransaction txn(holder->get());
    /// Namespaces live in both tables: `iceberg_tables` only records
    /// namespaces that contain a table, `iceberg_namespace_properties` only
    /// ones with explicit properties. namespaces are dot-joined strings;
    /// expand every nesting level as its own dotted path, matching the
    /// ICatalog contract for hierarchical catalogs.
    pqxx::result result = txn.exec_params(
        "SELECT DISTINCT table_namespace AS ns FROM " + qualified("iceberg_tables") + " WHERE catalog_name = $1"
            + " UNION SELECT DISTINCT \"namespace\" AS ns FROM " + qualified("iceberg_namespace_properties") + " WHERE catalog_name = $1",
        warehouse);

    Namespaces namespaces;
    for (const auto & row : result)
    {
        const String dotted = row["ns"].as<std::string>();
        size_t start = 0;
        while (start < dotted.size())
        {
            size_t dot = dotted.find('.', start);
            size_t end = (dot == String::npos) ? dotted.size() : dot;
            /// Emit every nesting level (`a.b` yields `a` and `a.b`).
            namespaces.push_back(dotted.substr(0, end));
            if (dot == String::npos)
                break;
            start = dot + 1;
        }
    }
    std::sort(namespaces.begin(), namespaces.end());
    namespaces.erase(std::unique(namespaces.begin(), namespaces.end()), namespaces.end());
    return namespaces;
}

CatalogTables IcebergJdbcCatalog::getTables() const
{
    auto holder = pool->get();
    pqxx::nontransaction txn(holder->get());
    pqxx::result result = txn.exec_params(
        "SELECT table_namespace, table_name FROM " + qualified("iceberg_tables")
            + " WHERE catalog_name = $1" + tableTypePredicate(),
        warehouse);

    CatalogTables tables;
    for (const auto & row : result)
        tables.push_back(CatalogTable{.name = row["table_namespace"].as<std::string>() + "." + row["table_name"].as<std::string>()});
    return tables;
}

CatalogTables IcebergJdbcCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    auto holder = pool->get();
    pqxx::nontransaction txn(holder->get());
    pqxx::result result = txn.exec_params(
        "SELECT table_name FROM " + qualified("iceberg_tables")
            + " WHERE catalog_name = $1 AND table_namespace = $2" + tableTypePredicate(),
        warehouse,
        namespace_name);

    CatalogTables tables;
    for (const auto & row : result)
        tables.push_back(CatalogTable{.name = namespace_name + "." + row["table_name"].as<std::string>()});
    return tables;
}

bool IcebergJdbcCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    TableMetadata metadata;
    return tryGetTableMetadata(namespace_name, table_name, metadata);
}

bool IcebergJdbcCatalog::tryGetTableMetadata(
    const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    pqxx::result rows;
    {
        auto holder = pool->get();
        pqxx::nontransaction txn(holder->get());
        rows = txn.exec_params(
            "SELECT metadata_location FROM " + qualified("iceberg_tables")
                + " WHERE catalog_name = $1 AND table_namespace = $2 AND table_name = $3" + tableTypePredicate(),
            warehouse,
            namespace_name,
            table_name);
    }

    if (rows.empty() || rows[0]["metadata_location"].is_null())
        return false;

    const String metadata_location = rows[0]["metadata_location"].as<std::string>();

    if (result.requiresDataLakeSpecificProperties())
    {
        result.setDataLakeSpecificProperties(
            DataLakeSpecificProperties{.iceberg_metadata_file_location = metadata_location});
    }

    if (!result.requiresLocation() && !result.requiresSchema())
        return true;

    /// The standard schema stores only the metadata pointer; location and
    /// schema come from `metadata.json` itself, like the REST catalog reads.
    auto metadata_object = getMetadataJSON(metadata_location, result);
    if (!metadata_object)
        return true;

    if (result.requiresLocation())
    {
        if (metadata_object->has("location") && !metadata_object->isNull("location"))
            result.setLocation(metadata_object->get("location").extract<String>());
        else
            result.setTableIsNotReadable(fmt::format("Cannot read table {}.{}, because its metadata has no 'location'", namespace_name, table_name));
    }

    if (result.requiresSchema())
    {
        const bool allow_geo_parser
            = getContext()->getSettingsRef()[DB::Setting::allow_experimental_geo_types_in_iceberg].value;
        auto schema_processor = DB::Iceberg::IcebergSchemaProcessor(allow_geo_parser);
        auto id = DB::IcebergMetadata::parseTableSchema(metadata_object, schema_processor, log);
        auto schema = schema_processor.getClickHouseTableSchemaById(id);
        result.setSchema(*schema);
    }

    return true;
}

void IcebergJdbcCatalog::getTableMetadata(
    const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "JDBC catalog: table '{}.{}' does not exist",
            namespace_name,
            table_name);
    }
}

Poco::JSON::Object::Ptr IcebergJdbcCatalog::getMetadataJSON(const String & metadata_location, TableMetadata & result) const
{
    if (auto cached = metadata_objects.get(metadata_location))
        return *cached;

    if (!metadata_location.starts_with("s3://") && !metadata_location.starts_with("s3a://"))
    {
        result.setTableIsNotReadable(fmt::format(
            "Cannot read table metadata at '{}': JDBC catalog reads metadata files only from S3-family storage",
            metadata_location));
        return nullptr;
    }

    auto [object_storage, metadata_path] = createObjectStorageForMetadataAccess(metadata_location);
    auto compression_method = DB::Iceberg::getCompressionMethodFromMetadataFile(metadata_location);
    auto metadata_object = DB::Iceberg::getMetadataJSONObject(
        metadata_path, object_storage, nullptr, getContext(), log, compression_method, std::nullopt);
    metadata_objects.set(metadata_location, std::make_shared<Poco::JSON::Object::Ptr>(metadata_object));
    return metadata_object;
}

IcebergJdbcCatalog::ObjectStorageWithPath
IcebergJdbcCatalog::createObjectStorageForMetadataAccess(const String & metadata_location) const
{
    DB::ASTStorage * storage = table_engine_definition->as<DB::ASTStorage>();
    DB::ASTs args = storage->engine->arguments->children;

    auto location = TableMetadata().withLocation();
    if (database_settings[DB::DatabaseDataLakeSetting::force_add_bucket])
        location.withForceAddBucket();
    location.setLocation(metadata_location);
    const String & storage_endpoint = database_settings[DB::DatabaseDataLakeSetting::storage_endpoint].value;
    String endpoint_arg = storage_endpoint.empty() ? metadata_location
        : location.getLocationWithEndpoint(storage_endpoint, database_settings[DB::DatabaseDataLakeSetting::storage_uri_style]);
    /// `TableMetadata` constructs a directory URL; here the location is a file.
    if (endpoint_arg.ends_with('/'))
        endpoint_arg.pop_back();
    if (args.empty())
        args.emplace_back(DB::make_intrusive<DB::ASTLiteral>(endpoint_arg));
    else
        args[0] = DB::make_intrusive<DB::ASTLiteral>(endpoint_arg);

    if (args.size() == 1)
    {
        if (auto static_credentials = DataLake::tryGetStaticStorageCredentials(
                DB::DatabaseDataLakeStorageType::S3, database_settings))
            static_credentials->addCredentialsToEngineArgs(args);
    }

    auto storage_settings = std::make_shared<DB::DataLakeStorageSettings>();
    storage_settings->loadFromSettingsChanges(database_settings.allChanged());
    auto configuration = std::make_shared<DB::StorageS3IcebergConfiguration>(storage_settings);
    DB::StorageObjectStorageConfiguration::initialize(*configuration, args, getContext(), false);

    return {configuration->createObjectStorage(getContext(), true, {}), configuration->getPathForRead().path};
}

}

#endif
