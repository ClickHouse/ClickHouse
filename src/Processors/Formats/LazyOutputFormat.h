#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <QueryPipeline/ProfileInfo.h>
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
        : IOutputFormat(header, out), queue(2) {}

    String getName() const override { return "LazyOutputFormat"; }

    Chunk getChunk(UInt64 milliseconds = 0);
    Chunk getTotals();
    Chunk getExtremes();

    bool isFinished() { return queue.isFinishedAndEmpty(); }

    ProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;

    void onCancel() override
    {
        queue.clearAndFinish();
    }

    void finalize() override
    {
        queue.finish();
    }

    bool expectMaterializedColumns() const override { return false; }

protected:
    void consume(Chunk chunk) override
    {
        (void)(queue.emplace(std::move(chunk)));
    }

    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

private:

    ConcurrentBoundedQueue<Chunk> queue;
    Chunk totals;
    Chunk extremes;

    /// Is not used.
    static WriteBuffer out;

    ProfileInfo info;
};

}
