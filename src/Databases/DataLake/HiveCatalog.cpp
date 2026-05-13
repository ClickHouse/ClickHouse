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
    size_t last_slash = 0;
    size_t last_points = 0;
    for (size_t i = 0; i < url.size(); ++i)
    {
        if (url[i] == ':')
            last_points = i;
        if (url[i] == '/')
            last_slash = i;
    }
    return {url.substr(last_slash + 1, last_points - (last_slash + 1)), std::stoi(url.substr(last_points + 1))};
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
