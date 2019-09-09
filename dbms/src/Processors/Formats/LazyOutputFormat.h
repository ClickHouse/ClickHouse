#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <DataStreams/BlockStreamProfileInfo.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class LazyOutputFormat : public IOutputFormat
{

public:
    explicit LazyOutputFormat(const Block & header)
        : IOutputFormat(header, out), queue(2), finished_processing(false) {}

    String getName() const override { return "LazyOutputFormat"; }

    Block getBlock(UInt64 milliseconds = 0);
    Block getTotals();
    Block getExtremes();

    bool isFinished() { return finished_processing; }

    BlockStreamProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;

    void finish() { finished_processing = true; }
    void clearQueue() { queue.clear(); }

protected:
    void consume(Chunk chunk) override
    {
        if (!finished_processing)
            queue.emplace(std::move(chunk));
    }

    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

    void finalize() override
    {
        finished_processing = true;

        /// In case we are waiting for result.
        queue.emplace(Chunk());
    }

private:

    ConcurrentBoundedQueue<Chunk> queue;
    Chunk totals;
    Chunk extremes;

    /// Is not used.
    static WriteBuffer out;

    BlockStreamProfileInfo info;

    std::atomic<bool> finished_processing;
};

}
