#pragma once

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Core/Block.h>
#include <Dictionaries/RedisSource.h>
namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }

    namespace Redis
    {
        class Client;
        class Array;
        class Command;
    }
}

namespace DB
{

class StorageRedis final : public IStorage
{
public:
    using RedisCommand = Poco::Redis::Command;

    StorageRedis(
        const StorageID & table_id_,
        const std::string & host_,
        const UInt16 & port_,
        const UInt32 & db_index_,
        const std::string & password_,
        const RedisStorageType & storage_type_,
        const String & primary_key_column_name_,
        const String & secondary_key_column_name_,
        const String & value_column_name_,
        ContextPtr context_,
        const std::string & options_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    String getName() const override { return "Redis"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    static StorageRedisConfiguration getConfiguration(ASTs engine_args, ContextPtr context);
private:
    ConnectionPtr getConnection() const;
    Poco::Redis::Array getKeys(ConnectionPtr & connection);
    const std::string host;
    const UInt16 port;
    const UInt32 db_index;
    const std::string password;
    RedisStorageType storage_type;
    ContextPtr context;
    String options;
    PoolPtr pool;
    const std::string primary_key_column_name;
    const std::string secondary_key_column_name;
    const std::string value_column_name;
};

}
