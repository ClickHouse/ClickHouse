#pragma once

#include "config_core.h"

#if USE_MYSQL

#include <Storages/IStorage.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <mysqlxx/PoolWithFailover.h>
#include <Storages/NamedCollections.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only.
  */
class StorageMySQL final : public IStorage, WithContext
{
public:
    struct Configuration : public StorageConfiguration
    {
        String database;
        String table;
        String schema;

        std::vector<std::pair<String, uint16_t>> addresses;

        String host;
        UInt16 port;

        String username;
        String password;

        String on_duplicate_clause;
        bool replace_query;
    };

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
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    static StorageMySQL::Configuration getConfiguration(ASTs engine_args, ContextPtr context_, MySQLBaseSettings & storage_settings);

    static Configuration parseConfigurationFromNamedCollection(ConfigurationFromNamedCollection & configuration_from_config, ContextPtr context);

    static NamedConfiguration getConfigKeys()
    {
        static const NamedConfiguration config_keys =
        {
            {"database", ConfigKeyInfo{ .type = Field::Types::String }},
            {"table", ConfigKeyInfo{ .type = Field::Types::String }},
            {"schema", ConfigKeyInfo{ .type = Field::Types::String }},
            {"addresses", ConfigKeyInfo{ .type = Field::Types::String }},
            {"host", ConfigKeyInfo{ .type = Field::Types::String }},
            {"port", ConfigKeyInfo{ .type = Field::Types::UInt64 }},
            {"username", ConfigKeyInfo{ .type = Field::Types::String }},
            {"password", ConfigKeyInfo{ .type = Field::Types::String }},
            {"replace_query", ConfigKeyInfo{ .type = Field::Types::Bool }},
            {"on_duplicate_clause", ConfigKeyInfo{ .type = Field::Types::String }},
            {"format", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
            {"compression_method", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
            {"structure", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
        };
        return config_keys;
    }

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
