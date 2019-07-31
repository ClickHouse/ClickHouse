#pragma once

#include <queue>
#include <Processors/IAccumulatingTransform.h>


namespace DB
{

class QueueBuffer : public IAccumulatingTransform
{
private:
    std::queue<Chunk> chunks;
public:
    String getName() const override { return "QueueBuffer"; }

    QueueBuffer(Block header)
        : IAccumulatingTransform(header, header)
    {
    }

    void consume(Chunk block) override
    {
        chunks.push(std::move(block));
    }

    Chunk generate() override
    {
        if (chunks.empty())
            return {};

        auto res = std::move(chunks.front());
        chunks.pop();
        return res;
    }
};

}
