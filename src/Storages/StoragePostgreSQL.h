#pragma once

#include "config_core.h"

#if USE_LIBPQXX
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/NamedCollections.h>

namespace Poco
{
class Logger;
}
namespace postgres
{
    class PoolWithFailover;
    using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;
}

namespace DB
{

class StoragePostgreSQL final : public IStorage
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

        String on_conflict;
        bool use_table_cache;
    };

    StoragePostgreSQL(
        const StorageID & table_id_,
        postgres::PoolWithFailoverPtr pool_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & remote_table_schema_ = "",
        const String & on_conflict = "");

    String getName() const override { return "PostgreSQL"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    static StoragePostgreSQL::Configuration getConfiguration(ASTs engine_args, ContextPtr context);

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
            {"on_conflict", ConfigKeyInfo{ .type = Field::Types::String }},
            {"use_table_cache", ConfigKeyInfo{ .type = Field::Types::Bool }},
            {"format", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
            {"compression_method", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
            {"structure", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
        };
        return config_keys;
    }

private:
    String remote_table_name;
    String remote_table_schema;
    String on_conflict;
    postgres::PoolWithFailoverPtr pool;

    Poco::Logger * log;
};

}

#endif
