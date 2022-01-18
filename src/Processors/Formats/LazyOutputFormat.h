#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <DataStreams/BlockStreamProfileInfo.h>
#include <IO/WriteBuffer.h>

namespace DB
{

/// LazyOutputFormat is used to retrieve ready data from executing pipeline.
/// You can periodically call `getChunk` from separate thread.
/// Used in PullingAsyncPipelineExecutor.
class LazyOutputFormat : public IOutputFormat
{

public:
    explicit LazyOutputFormat(const Block & header)
        : IOutputFormat(header, out), queue(2), finished_processing(false) {}

    String getName() const override { return "LazyOutputFormat"; }

    Chunk getChunk(UInt64 milliseconds = 0);
    Chunk getTotals();
    Chunk getExtremes();

    bool isFinished() { return finished_processing && queue.size() == 0; }

    BlockStreamProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;

    void finish()
    {
        finished_processing = true;
        /// Clear queue in case if somebody is waiting lazy_format to push.
        queue.clear();
    }

    void finalize() override
    {
        finished_processing = true;

        /// In case we are waiting for result.
        queue.emplace(Chunk());
    }

protected:
    void consume(Chunk chunk) override
    {
        if (!finished_processing)
            queue.emplace(std::move(chunk));
    }

    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

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
