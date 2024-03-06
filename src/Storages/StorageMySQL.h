#pragma once

#include "config.h"

#if USE_MYSQL

#include <Storages/IStorage.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <mysqlxx/PoolWithFailover.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class NamedCollection;

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only.
  */
class StorageMySQL final : public IStorage, WithContext
{
public:
    StorageMySQL(
        const StorageID & table_id_,
        mysqlxx::PoolWithFailover && pool_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        bool replace_query_,
        const std::string & on_duplicate_clause_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const MySQLSettings & mysql_settings_);

    std::string getName() const override { return "MySQL"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    struct Configuration
    {
        using Addresses = std::vector<std::pair<String, UInt16>>;

        String host;
        UInt16 port = 0;
        String username = "default";
        String password;
        String database;
        String table;

        bool replace_query = false;
        String on_duplicate_clause;

        Addresses addresses; /// Failover replicas.
        String addresses_expr;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context_, MySQLSettings & storage_settings);

    static Configuration processNamedCollectionResult(
        const NamedCollection & named_collection, MySQLSettings & storage_settings,
        ContextPtr context_, bool require_table = true);

private:
    friend class StorageMySQLSink;

    std::string remote_database_name;
    std::string remote_table_name;
    bool replace_query;
    std::string on_duplicate_clause;

    MySQLSettings mysql_settings;

    mysqlxx::PoolWithFailoverPtr pool;

    Poco::Logger * log;
};

}

#endif
