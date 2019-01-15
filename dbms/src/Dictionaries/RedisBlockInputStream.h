#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include "ExternalResultDescription.h"


namespace Poco
{
    namespace Redis
    {
        class Client;
    }
}


namespace DB
{
/// Converts Redis Cursor to a stream of Blocks
    class RedisBlockInputStream final : public IProfilingBlockInputStream
    {
    public:
        RedisBlockInputStream(
                std::shared_ptr<Poco::Redis::Client> client_,
                const Block & sample_block,
                const size_t max_block_size);

        ~RedisBlockInputStream() override;

        String getName() const override { return "Redis"; }

        Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    private:
        Block readImpl() override;

        std::shared_ptr<Poco::Redis::Client> client;
        const size_t max_block_size;
        ExternalResultDescription description;
        int64_t cursor = 0;
        bool all_read = false;
    };

}
