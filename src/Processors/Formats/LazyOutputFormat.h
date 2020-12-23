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

protected:
    void consume(Port::Data data) override
    {
        if (!finished_processing)
            queue.emplace(std::move(data));
    }

    void consumeTotals(Port::Data data) override { totals = std::move(data); }
    void consumeExtremes(Port::Data data) override { extremes = std::move(data); }

    void finalize() override
    {
        finished_processing = true;

        /// In case we are waiting for result.
        queue.emplace(Port::Data{});
    }

private:

    ConcurrentBoundedQueue<Port::Data> queue;
    Port::Data totals;
    Port::Data extremes;

    /// Is not used.
    static WriteBuffer out;

    BlockStreamProfileInfo info;

    std::atomic<bool> finished_processing;
};

}
