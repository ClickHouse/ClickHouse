#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <QueryPipeline/ProfileInfo.h>

namespace DB
{

class NullWriteBuffer;

/// LazyOutputFormat is used to retrieve ready data from executing pipeline.
/// You can periodically call `getChunk` from separate thread.
/// Used in PullingAsyncPipelineExecutor.
class LazyOutputFormat : public IOutputFormat
{

public:
    explicit LazyOutputFormat(SharedHeader header);

    String getName() const override { return "LazyOutputFormat"; }

    Chunk getChunk(UInt64 milliseconds = 0);
    Chunk getTotals();
    Chunk getExtremes();

    bool isFinished() { return queue.isFinishedAndEmpty(); }

    ProfileInfo & getProfileInfo()
    {
        /// Refresh rows-before-* from the shared counters: the connection draining that runs in
        /// PipelineExecutor::finalizeExecution (e.g. from parallel replicas after a LIMIT
        /// cancellation) can deliver late ProfileInfo packets that update the counters after
        /// work() snapshotted them. The caller reads this after execution has finished, so the
        /// values are final by now (see PullingAsyncPipelineExecutor::pull, which joins the
        /// executor thread before reporting the end of the stream).
        snapshotRowsBeforeCounters();
        return info;
    }

    void setRowsBeforeLimit(size_t rows_before_limit) override;
    void setRowsBeforeAggregation(size_t rows_before_aggregation) override;

    void onCancel() noexcept override
    {
        queue.clearAndFinish();
    }

    void finalizeImpl() override
    {
        queue.finish();
    }

    bool expectMaterializedColumns() const override { return false; }
    bool supportsSpecialSerializationKinds() const override { return true; }

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
    static NullWriteBuffer out;

    ProfileInfo info;
};

}
