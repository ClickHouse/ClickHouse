#pragma once

#include <Storages/IStorage.h>
#include <sparsehash/dense_hash_map>

#include <mysqlxx/Pool.h>

namespace DB
{

inline std::string SplitHostPort(const char * host_port, int & port)
{
    const char* colon = strchr(host_port, ':');
    std::string server;
    if (colon)
    {
        server = std::string(host_port, colon - host_port);
        port = atoi(colon + 1);
    }
    else
    {
        server = host_port;
        port = 3306;
    }
    return server;
}

std::string AnalyzeQuery(const SelectQueryInfo & query_info, const Context & context, std::string table_name, NamesAndTypesListPtr columns, google::dense_hash_map<std::string, DataTypePtr> & column_map, Block & sample_block);

class StorageMySQL : public IStorage
{
public:
    StorageMySQL(
        const std::string & table_name_,
        const std::string & server_,
        int port_,
        const std::string & database_name_,
        const std::string & mysql_table_name_,
        const std::string & user_name_,
        const std::string & password_,
        const NamesAndTypesListPtr & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context_);

    static StoragePtr create(
        const std::string & table_name,
        const std::string & host_port,
        const std::string & database_name,
        const std::string & mysql_table_name,
        const std::string & user_name,
        const std::string & password,
        const NamesAndTypesListPtr & columns,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context)
    {
        std::string server;
        int port;
        server = SplitHostPort(host_port.c_str(), port);
        return std::make_shared<StorageMySQL>(
            table_name, server, port, database_name, mysql_table_name, user_name, password, columns,
            materialized_columns_, alias_columns_, column_defaults_,
            context);
    }

    std::string getName() const override
    {
        return "MySQL";
    }

    std::string getTableName() const override
    {
        return table_name;
    }

    const NamesAndTypesList & getColumnsListImpl() const override
    {
        return *columns;
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    std::string table_name;
    std::string server;
    int port;
    std::string database_name;
    std::string mysql_table_name;
    std::string user_name;
    std::string password;
    Block sample_block;
    NamesAndTypesListPtr columns;
    const Context & context_global;
    google::dense_hash_map<std::string, DataTypePtr> column_map;
    mysqlxx::Pool pool;
};

}
