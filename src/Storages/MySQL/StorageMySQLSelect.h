#pragma once
#include "config.h"
#if USE_MYSQL

#include <Storages/IStorage.h>
#include <mysqlxx/PoolWithFailover.h>

namespace Poco
{
class Logger;
}

namespace DB
{

struct MySQLSettings;
class NamedCollection;

/**
 * StorageMySQLSelect is an implementation of IStorage similar to StorageMySQL.
 * However, it only provides interface to read result of SELECT query being directly passed to MySQL remote database.
 * Primarily developed to be used by table function `mysql` for alternative syntax with query passing.
 */
class StorageMySQLSelect final : public IStorage, WithContext
{
public:
    StorageMySQLSelect(
        const StorageID & table_id_,
        mysqlxx::PoolWithFailover && pool_,
        const std::string & remote_database_name_,
        /*const*/ std::string select_query_,
        const ColumnsDescription & columns_,
        const String & comment,
        ContextPtr context_,
        const MySQLSettings & mysql_settings_);

    std::string getName() const override { return "MySQLSelect"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr &,
        SelectQueryInfo &,
        ContextPtr,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    struct Configuration
    {
        using Addresses = std::vector<std::pair<String, UInt16>>;

        String host;
        UInt16 port = 0;
        String username = "default";
        String password;
        String database;
        String select_query;

        Addresses addresses; /// Failover replicas.
        String addresses_expr;
    };

    static Configuration getConfiguration(ASTs storage_args, ContextPtr context_, MySQLSettings & storage_settings);

    // Performs a query to MySQL and returns the structure of the result.
    static ColumnsDescription
    doQueryResultStructure(mysqlxx::PoolWithFailover & pool_, const String & select_query, const ContextPtr & context_);

private:
    std::string remote_database_name;
    std::string select_query;

    std::unique_ptr<MySQLSettings> mysql_settings;

    mysqlxx::PoolWithFailoverPtr pool;

    LoggerPtr log;
};

}

#endif
