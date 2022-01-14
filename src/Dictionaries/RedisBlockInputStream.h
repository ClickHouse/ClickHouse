#pragma once

#include <Core/Block.h>

#include <Core/ExternalResultDescription.h>
#include <DataStreams/IBlockInputStream.h>
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
    class RedisBlockInputStream final : public IBlockInputStream
    {
    public:
        using RedisArray = Poco::Redis::Array;
        using RedisBulkString = Poco::Redis::BulkString;

        RedisBlockInputStream(
                const std::shared_ptr<Poco::Redis::Client> & client_,
                const Poco::Redis::Array & keys_,
                const RedisStorageType & storage_type_,
                const Block & sample_block,
                const size_t max_block_size);

        ~RedisBlockInputStream() override;

        String getName() const override { return "Redis"; }

        Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    private:
        Block readImpl() override;

        std::shared_ptr<Poco::Redis::Client> client;
        Poco::Redis::Array keys;
        RedisStorageType storage_type;
        const size_t max_block_size;
        ExternalResultDescription description;
        size_t cursor = 0;
        bool all_read = false;
    };

}
