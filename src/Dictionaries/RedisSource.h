#pragma once

#include <Core/Block.h>
#include <base/BorrowedObjectPool.h>
#include <Core/ExternalResultDescription.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Poco/Redis/Array.h>
#include <Poco/Redis/Type.h>
#include <Poco/Redis/Client.h>

namespace Poco
{
    namespace Redis
    {
        class Client;
        class Array;
        class Command;
    }
}


namespace DB
{
    enum class RedisStorageType
    {
            SIMPLE,
            HASH_MAP,
            UNKNOWN
    };

    using ClientPtr = std::unique_ptr<Poco::Redis::Client>;
    using Pool = BorrowedObjectPool<ClientPtr>;
    using PoolPtr = std::shared_ptr<Pool>;
    struct Connection
    {
        Connection(PoolPtr pool_, ClientPtr client_)
            : pool(std::move(pool_)), client(std::move(client_))
        {
        }

        ~Connection()
        {
            pool->returnObject(std::move(client));
        }

        PoolPtr pool;
        ClientPtr client;
    };

    using ConnectionPtr = std::unique_ptr<Connection>;

    class RedisSource final : public SourceWithProgress
    {
    public:
        using RedisArray = Poco::Redis::Array;
        using RedisBulkString = Poco::Redis::BulkString;
        RedisSource(
            ConnectionPtr connection_,
            const Poco::Redis::Array & keys_,
            const RedisStorageType & storage_type_,
            const Block & sample_block,
            size_t max_block_size,
            const std::vector<bool> & selected_columns_ = {true, true, true});

        ~RedisSource() override;

        String getName() const override { return "Redis"; }

    private:
        Chunk generate() override;

        ConnectionPtr connection;
        Poco::Redis::Array keys;
        RedisStorageType storage_type;
        const size_t max_block_size;
        ExternalResultDescription description;
        size_t cursor = 0;
        bool all_read = false;
        const std::vector<bool> selected_columns;
    };

}

