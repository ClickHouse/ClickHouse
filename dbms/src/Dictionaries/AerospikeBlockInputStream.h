#pragma once

#include <memory>
#include <aerospike/aerospike.h>
#include <aerospike/as_key.h>

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>

namespace DB
{
    class Aerospike;
    using AerospikeKey = std::unique_ptr<as_key>;

    class AerospikeBlockInputStream final : public IBlockInputStream
    {
    public:
        AerospikeBlockInputStream(
            Aerospike & client,
            std::vector<AerospikeKey> && keys,
            const Block & sample_block,
            const size_t max_block_size,
            const std::string & namespace_name,
            const std::string & set_name);

        ~AerospikeBlockInputStream() override;

        String getName() const override { return "Aerospike"; }
        Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    private:
        size_t cursor = 0;
        Aerospike & client;
        std::vector<std::unique_ptr<as_key>> keys;
        const size_t max_block_size;
        const std::string namespace_name;
        const std::string set_name;
        ExternalResultDescription description;
        bool all_read = false;

        Block readImpl() override;
    };

}
