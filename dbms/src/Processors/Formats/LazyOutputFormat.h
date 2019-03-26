#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <DataStreams/BlockStreamProfileInfo.h>

namespace DB
{

class LazyOutputFormat : public IOutputFormat
{

public:
    LazyOutputFormat(Block header, WriteBuffer & out)
        : IOutputFormat(std::move(header), out), queue(1), finished(false) {}

    Block getBlock(UInt64 milliseconds = 0);
    Block getTotals();
    Block getExtremes();

    bool isFinished() { return finished; }

    BlockStreamProfileInfo & getProfileInfo() { return info; }

protected:
    void consume(Chunk chunk) override { queue.push(chunk); }
    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

    void finalize() override
    {
        finished = true;

        /// In case we are waiting for result.
        queue.push({});
    }

private:

    ConcurrentBoundedQueue<Chunk> queue;
    Chunk totals;
    Chunk extremes;

    BlockStreamProfileInfo info;

    std::atomic<bool> finished;
};

}
