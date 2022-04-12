#pragma once

#include <Core/Block.h>
#include <base/BorrowedObjectPool.h>

#include "DictionaryStructure.h"
#include "IDictionarySource.h"

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
    namespace ErrorCodes
    {
        extern const int NOT_IMPLEMENTED;
    }

    enum class RedisStorageType
    {
            SIMPLE,
            HASH_MAP,
            UNKNOWN
    };

    class RedisDictionarySource final : public IDictionarySource
    {
    public:
        using RedisArray = Poco::Redis::Array;
        using RedisCommand = Poco::Redis::Command;

        using ClientPtr = std::unique_ptr<Poco::Redis::Client>;
        using Pool = BorrowedObjectPool<ClientPtr>;
        using PoolPtr = std::shared_ptr<Pool>;

        struct Configuration
        {
            const std::string host;
            const UInt16 port;
            const UInt32 db_index;
            const std::string password;
            const RedisStorageType storage_type;
            const size_t pool_size;
        };

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

        RedisDictionarySource(
            const DictionaryStructure & dict_struct_,
            const Configuration & configuration_,
            const Block & sample_block_);

        RedisDictionarySource(const RedisDictionarySource & other);

        ~RedisDictionarySource() override;

        Pipe loadAll() override;

        Pipe loadUpdatedAll() override
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for RedisDictionarySource");
        }

        bool supportsSelectiveLoad() const override { return true; }

        Pipe loadIds(const std::vector<UInt64> & ids) override;

        Pipe loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

        bool isModified() const override { return true; }

        bool hasUpdateField() const override { return false; }

        DictionarySourcePtr clone() const override { return std::make_shared<RedisDictionarySource>(*this); }

        std::string toString() const override;

    private:
        ConnectionPtr getConnection() const;

        const DictionaryStructure dict_struct;
        const Configuration configuration;

        PoolPtr pool;
        Block sample_block;
    };
}
