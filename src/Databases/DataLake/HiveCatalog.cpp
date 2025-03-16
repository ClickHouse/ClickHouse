#include "Databases/DataLake/HiveCatalog.h"
#if USE_AVRO
#    include <optional>
#    include "Common/Exception.h"
#    include "Core/Names.h"
#    include "Databases/DataLake/ICatalog.h"

#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypeUUID.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/IDataType.h>

#    include <IO/S3/Client.h>
#    include <IO/S3/Credentials.h>
#    include <IO/S3Settings.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#    include <Common/ProxyConfigurationResolverProvider.h>

namespace DB::ErrorCodes
{
extern const int DATALAKE_DATABASE_ERROR;
}

namespace DataLake
{

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

HiveCatalog::HiveCatalog(const std::string & warehouse_, const std::string & storage_type_, const std::string & base_url_, DB::ContextPtr)
    : ICatalog(warehouse_)
    , socket(new apache::thrift::transport::TSocket(parseHostPort(base_url_).first, parseHostPort(base_url_).second))
    , transport(new apache::thrift::transport::TBufferedTransport(socket))
    , protocol(new apache::thrift::protocol::TBinaryProtocol(transport))
    , client(protocol)
    , storage_type(storage_type_)
{
    transport->open();
}

bool HiveCatalog::empty() const
{
    std::vector<std::string> result;
    client.get_all_databases(result);
    return result.empty();
}

DB::Names HiveCatalog::getTables() const
{
    DB::Names result;
    DB::Names databases;
    client.get_all_databases(databases);
    for (const auto & db : databases)
    {
        DB::Names current_tables;
        client.get_all_tables(current_tables, db);
        for (const auto & table : current_tables)
            result.push_back(db + "." + table);
    }
    return result;
}

bool HiveCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    Apache::Hadoop::Hive::Table table;
    client.get_table(table, namespace_name, table_name);

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

    client.get_table(table, namespace_name, table_name);

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
    if (storage_type == "s3")
        return StorageType::S3;

    if (storage_type == "azure")
        return StorageType::Azure;

    if (storage_type == "hdfs")
        return StorageType::HDFS;

    if (storage_type == "local")
        return StorageType::Local;

    return StorageType::Other;
}

}
#endif
