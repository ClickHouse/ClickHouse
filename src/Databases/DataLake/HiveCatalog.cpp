#include <Databases/DataLake/HiveCatalog.h>
#include <algorithm>
#include <cctype>
#if USE_AVRO && USE_HIVE
#include <optional>
#include <Common/Exception.h>
#include <Core/Names.h>
#include <Databases/DataLake/ICatalog.h>

#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3Settings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Databases/DataLake/Common.h>

namespace DB::ErrorCodes
{
extern const int DATALAKE_DATABASE_ERROR;
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

HiveCatalog::HiveCatalog(const std::string & warehouse_, const std::string & base_url_, DB::ContextPtr)
    : ICatalog(warehouse_)
    , socket(new apache::thrift::transport::TSocket(parseHostPort(base_url_).first, parseHostPort(base_url_).second))
    , transport(new apache::thrift::transport::TBufferedTransport(socket))
    , protocol(new apache::thrift::protocol::TBinaryProtocol(transport))
    , client(protocol)
{
    transport->open();
}

bool HiveCatalog::empty() const
{
    std::vector<std::string> result;

    std::lock_guard lock(client_mutex);
    client.get_all_databases(result);
    return result.empty();
}

DB::Names HiveCatalog::getTables() const
{
    DB::Names result;
    DB::Names databases;
    {
        std::lock_guard lock(client_mutex);
        client.get_all_databases(databases);
    }

    for (const auto & db : databases)
    {
        DB::Names current_tables;
        {
            std::lock_guard lock(client_mutex);
            client.get_all_tables(current_tables, db);
        }
        for (const auto & table : current_tables)
            result.push_back(db + "." + table);
    }
    return result;
}

bool HiveCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    Apache::Hadoop::Hive::Table table;
    {
        std::lock_guard lock(client_mutex);
        client.get_table(table, namespace_name, table_name);
    }

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

    {
        std::lock_guard lock(client_mutex);
        client.get_table(table, namespace_name, table_name);
    }

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
