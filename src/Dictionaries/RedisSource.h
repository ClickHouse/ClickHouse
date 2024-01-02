#pragma once

#include <Core/Block.h>

#include <Core/ExternalResultDescription.h>
#include <Processors/ISource.h>
#include <Poco/Redis/Array.h>
#include <Poco/Redis/Type.h>
#include "RedisDictionarySource.h"

namespace Poco
{
    namespace Redis
    {
        class Client;
    }
}


namespace DB
{
    class RedisSource final : public ISource
    {
    public:
        using RedisArray = Poco::Redis::Array;
        using RedisBulkString = Poco::Redis::BulkString;
        using ConnectionPtr = RedisDictionarySource::ConnectionPtr;

        RedisSource(
            ConnectionPtr connection_,
            const Poco::Redis::Array & keys_,
            const RedisStorageType & storage_type_,
            const Block & sample_block,
            size_t max_block_size);

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
    };

}

