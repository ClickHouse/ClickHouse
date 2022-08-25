#pragma once

#include <queue>
#include <Processors/IAccumulatingTransform.h>


namespace DB
{

/** Reads all data into queue.
  * After all data has been read - output it in the same order.
  */
class QueueBuffer : public IAccumulatingTransform
{
private:
    std::queue<Chunk> chunks;
public:
    String getName() const override { return "QueueBuffer"; }

    explicit QueueBuffer(Block header)
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
