#pragma once

#include <Core/QueryProcessingStage.h>
#include <DataStreams/IBlockInputStream.h>

#include <vector>

namespace DB
{

class CacheBlockInputStream : public IBlockInputStream
{
    public:
        CacheBlockInputStream(std::vector<Block> blocks_) : blocks(blocks_), pos(0) {}

        String getName() const override { return "CacheBlockInputStream"; }
        Block getHeader() const override
        {
            if (pos == blocks.size())
                return Block();
            return blocks[pos];
        }

    protected:
        Block readImpl() override
        {
            if (pos == blocks.size())
                return Block();
            return blocks[pos++];
        }

    private:
        std::vector<Block> blocks;
        size_t pos;
};

}

