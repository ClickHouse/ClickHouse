#pragma once

#include <queue>
#include <Processors/IAccumulatingTransform.h>


namespace DB
{

class QueueBuffer : public IAccumulatingTransform
{
private:
    std::queue<Block> blocks;
public:
    String getName() const override { return "QueueBuffer"; }

    QueueBuffer(Block header)
        : IAccumulatingTransform(header, header)
    {
    }

    void consume(Block block) override
    {
        blocks.push(std::move(block));
    }

    Block generate() override
    {
        if (blocks.empty())
            return {};

        Block res = std::move(blocks.front());
        blocks.pop();
        return res;
    }
};

}
