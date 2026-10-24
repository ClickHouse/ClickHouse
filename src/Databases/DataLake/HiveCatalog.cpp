#include <Databases/DataLake/HiveCatalog.h>
#include <algorithm>
#include <cctype>
#if USE_AVRO && USE_HIVE
#include <optional>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/UniqueLock.h>
#include <Core/Names.h>
#include <Databases/DataLake/ICatalog.h>

#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3Settings.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Common/RemoteHostFilter.h>
#include <Databases/DataLake/Common.h>
#include <Common/FailPoint.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int FAULT_INJECTED;
    extern const int NO_HIVEMETASTORE;
}

namespace DB::FailPoints
{
    extern const char check_database_datalake_negative[];
}

namespace DataLake
{

namespace
{

std::pair<String, Int32> parseHostPort(const String & url)
{
    auto protocol_sep = url.find("://");
    if (protocol_sep == String::npos)
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid URL format: missing protocol separator '://'");

    size_t start = protocol_sep + 3;

    auto colon_pos = url.find(':', start);
    if (colon_pos == String::npos)
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid URL format: missing port number");

    auto slash_pos = url.find('/', start);

    String host = url.substr(start, colon_pos - start);

    size_t port_end = (slash_pos != String::npos) ? slash_pos : url.size();
    String port_str = url.substr(colon_pos + 1, port_end - colon_pos - 1);

    if (port_str.empty() || !std::all_of(port_str.begin(), port_str.end(), ::isdigit))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid port number: '{}'", port_str);

    try
    {
        Int32 port = std::stoi(port_str);
        if (port <= 0 || port > 65535)
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Port number out of valid range (1-65535): {}", port);
        return {host, port};
    }
    catch (const std::out_of_range&)
    {
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid port number format: {}", port_str);
    }
    catch (const std::invalid_argument&)
    {
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid port number: '{}'", port_str);
    }
}

}

HiveCatalog::HiveCatalog(
    const std::string & warehouse_, const std::string & base_url_, size_t max_connections_, DB::ContextPtr context_)
    : ICatalog(warehouse_)
    , DB::WithContext(context_)
    , base_url(base_url_)
    , max_connections(max_connections_)
{
    if (max_connections == 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Setting `hive_metastore_max_connections` must be greater than 0");

    auto connection = createConnection();
    std::lock_guard lock(pool_mutex);
    idle_connections.push_back(std::move(connection));
    num_connections = 1;
}

HiveCatalog::Connection::~Connection()
{
    if (!transport || !transport->isOpen())
        return;
    try
    {
        transport->close();
    }
    catch (const std::exception & ex)
    {
        LOG_TRACE(&Poco::Logger::get("HiveCatalog"), "Error closing transport: {}", ex.what());
    }
}

HiveCatalog::ConnectionPtr HiveCatalog::createConnection() const
{
    auto [host, port] = parseHostPort(base_url);

    /// Also rejects control characters such as NUL, which `getaddrinfo` would truncate the host at.
    getContext()->getRemoteHostFilter().checkAndGetCanonicalHostAndPort(
        host + ":" + std::to_string(port), static_cast<UInt16>(port), "Hive metastore address");

    auto connection = std::make_unique<Connection>();
    auto socket = std::make_shared<apache::thrift::transport::TSocket>(host, port);
    connection->transport = std::make_shared<apache::thrift::transport::TBufferedTransport>(socket);
    auto protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(connection->transport);
    connection->client = std::make_unique<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>(protocol);
    connection->transport->open();
    return connection;
}

HiveCatalog::ConnectionPtr HiveCatalog::acquireConnection() const
{
    UInt64 connection_generation = 0;
    {
        DB::UniqueLock lock(pool_mutex);
        while (idle_connections.empty() && num_connections >= max_connections)
        {
            DB::CurrentThread::checkIfNotCancelled();
            connection_released.wait_for(lock.getUnderlyingLock(), std::chrono::seconds(1));
        }

        if (!idle_connections.empty())
        {
            auto connection = std::move(idle_connections.back());
            idle_connections.pop_back();
            return connection;
        }

        ++num_connections;
        connection_generation = generation;
    }

    try
    {
        auto connection = createConnection();
        connection->generation = connection_generation;
        return connection;
    }
    catch (...)
    {
        std::lock_guard lock(pool_mutex);
        --num_connections;
        connection_released.notify_one();
        throw;
    }
}

void HiveCatalog::releaseConnection(ConnectionPtr connection) const
{
    {
        std::lock_guard lock(pool_mutex);
        if (connection->generation == generation)
        {
            try
            {
                idle_connections.push_back(std::move(connection));
            }
            catch (...)
            {
                --num_connections;
                connection_released.notify_one();
                throw;
            }
            connection_released.notify_one();
            return;
        }
        --num_connections;
        connection_released.notify_one();
    }
    connection.reset();
}

void HiveCatalog::discardConnection(ConnectionPtr connection) const
{
    {
        std::lock_guard lock(pool_mutex);
        --num_connections;
        connection_released.notify_one();
    }
    connection.reset();
}

void HiveCatalog::invalidateConnections(ConnectionPtr failed_connection) const
{
    std::vector<ConnectionPtr> stale_connections;
    {
        std::lock_guard lock(pool_mutex);
        if (failed_connection->generation == generation)
        {
            ++generation;
            stale_connections.swap(idle_connections);
        }
        num_connections -= stale_connections.size() + 1;
        connection_released.notify_all();
    }
    failed_connection.reset();
    stale_connections.clear();
}

template <typename Func>
void HiveCatalog::executeWithRetry(Func && func) const
{
    constexpr int max_retries = 3;
    String last_err_msg;

    for (int i = 0; i < max_retries; ++i)
    {
        ConnectionPtr connection;
        try
        {
            connection = acquireConnection();
            func(*connection->client);
        }
        catch (apache::thrift::transport::TTransportException & e)
        {
            last_err_msg = e.what();
            LOG_TRACE(&Poco::Logger::get("HiveCatalog"), "Hive Metastore request failed in iteration {}/{} with error: {}", i, max_retries, e.what());
            if (connection)
                invalidateConnections(std::move(connection));
            continue;
        }
        catch (const apache::thrift::TException &)
        {
            if (connection)
                releaseConnection(std::move(connection));
            throw;
        }
        catch (...)
        {
            if (connection)
                discardConnection(std::move(connection));
            throw;
        }

        releaseConnection(std::move(connection));
        return;
    }

    throw DB::Exception(
        DB::ErrorCodes::NO_HIVEMETASTORE, "Hive Metastore connection failed after {} attempts. Last error: {}", max_retries, last_err_msg);
}

bool HiveCatalog::empty() const
{
    fiu_do_on(DB::FailPoints::check_database_datalake_negative,
    {
        throw DB::Exception(DB::ErrorCodes::FAULT_INJECTED, "Injecting fault when checking database");
    });

    std::vector<std::string> result;

    executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_all_databases(result); });
    return result.empty();
}

CatalogTables HiveCatalog::getTables() const
{
    CatalogTables result;
    DB::Names databases;

    executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_all_databases(databases); });

    for (const auto & db : databases)
    {
        DB::Names current_tables;
        executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_all_tables(current_tables, db); });
        /// A Hive catalog used with the Iceberg engine lists only Iceberg tables, so they are readable.
        for (const auto & table : current_tables)
            result.push_back(CatalogTable{.name = db + "." + table});
    }
    return result;
}

DataLake::ICatalog::Namespaces HiveCatalog::getNamespaces() const
{
    /// HMS databases are flat — they cannot contain nested namespaces.
    DB::Names databases;
    executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_all_databases(databases); });
    return databases;
}

CatalogTables HiveCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    DB::Names current_tables;
    /// Retry transient `TTransportException` (same wrapper as `getTables`) rather
    /// than letting it bubble up and become an empty table list.
    executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_all_tables(current_tables, namespace_name); });
    /// A Hive catalog used with the Iceberg engine lists only Iceberg tables, so they are readable.
    CatalogTables result;
    result.reserve(current_tables.size());
    for (const auto & table : current_tables)
        result.push_back(CatalogTable{.name = namespace_name + "." + table});
    return result;
}

bool HiveCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    Apache::Hadoop::Hive::Table table;

    executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_table(table, namespace_name, table_name); });

    if (table.tableName.empty())
        return false;

    return true;
}

void HiveCatalog::getTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from iceberg catalog");
}

bool HiveCatalog::tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    Apache::Hadoop::Hive::Table table;

    executeWithRetry([&](Apache::Hadoop::Hive::ThriftHiveMetastoreClient & client) { client.get_table(table, namespace_name, table_name); });

    if (table.tableName.empty())
        return false;

    if (result.requiresLocation())
    {
        if (table.sd.location.empty())
            result.setTableIsNotReadable(fmt::format("Cannot read table {}, because no 'location' in response", table_name));
        else
        {
            result.setLocation(table.sd.location);
        }
    }

    if (result.requiresSchema())
    {
        DB::NamesAndTypesList schema;
        auto columns = table.sd.cols;
        for (const auto & column : columns)
        {
            schema.push_back({column.name, getType(column.type, true)});
        }
        result.setSchema(schema);
    }

    if (result.requiresDataLakeSpecificProperties())
    {
        if (auto it = table.parameters.find("metadata_location"); it != table.parameters.end())
            result.setDataLakeSpecificProperties(DataLakeSpecificProperties{.iceberg_metadata_file_location = it->second});
    }

    return true;
}

std::optional<StorageType> HiveCatalog::getStorageType() const
{
    return std::nullopt;
}

}
#endif
