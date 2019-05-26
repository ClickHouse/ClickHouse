#pragma once

#include <Core/Block.h>
#include <Core/ExternalResultDescription.h>
#include <DataStreams/IBlockInputStream.h>


namespace Poco
{
    namespace Redis
    {
        class Array;
        class Client;
    }
}


namespace DB
{
    class RedisBlockInputStream final : public IBlockInputStream
    {
    public:
        RedisBlockInputStream(
                const std::shared_ptr<Poco::Redis::Client> & client_,
                const Poco::Redis::Array & keys_,
                const Block & sample_block,
                const size_t max_block_size);

        ~RedisBlockInputStream() override;

        String getName() const override { return "Redis"; }

        Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    private:
        Block readImpl() override;

        std::shared_ptr<Poco::Redis::Client> client;
        Poco::Redis::Array keys;
        const size_t max_block_size;
        ExternalResultDescription description;
        size_t cursor = 0;
        bool all_read = false;
    };

}
