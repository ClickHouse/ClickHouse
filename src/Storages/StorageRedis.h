#pragma once

#include <Poco/Redis/Redis.h>
#include <Storages/IStorage.h>
#include <Dictionaries/RedisSource.h>

namespace DB
{
/* Implements storage in the Redis.
 * Use ENGINE = Redis(host:port, db_id, password, storage_type);
 * Read only.
 */
class StorageRedis : public IStorage
{
public:
    enum class StorageType
    {
        SIMPLE,
        LIST,
        SET,
        HASH,
        ZSET
    };

    static String toString(StorageType storage_type);
    static StorageType toStorageType(const String & storage_type);

    struct Configuration
    {
        String host;
        uint32_t port;
        String db_id;
        String password;
        StorageType storage_type;
    };

    using RedisArray = Poco::Redis::Array;
    using RedisCommand = Poco::Redis::Command;

    using ClientPtr = std::unique_ptr<Poco::Redis::Client>;
    using Pool = BorrowedObjectPool<ClientPtr>;
    using PoolPtr = std::shared_ptr<Pool>;

    struct Connection
    {
        Connection(PoolPtr pool_, ClientPtr client_);
        ~Connection();

        PoolPtr pool;
        ClientPtr client;
    };

    using ConnectionPtr = std::unique_ptr<Connection>;

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

    StorageRedis(
        const StorageID & table_id_,
        const Configuration & configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_);

    std::string getName() const override { return "Redis"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context) override;

private:
    Configuration configuration;
    StorageID table_id;
    ColumnsDescription columns;
    ConstraintsDescription constraints;
    String comment;

    std::shared_ptr<Connection> connection;
    void connectIfNotConnected();
};

}
