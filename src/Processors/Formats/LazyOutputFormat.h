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

    ProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;
    void setRowsBeforeAggregation(size_t rows_before_aggregation) override;

    /// A query broken off by a time limit with `timeout_overflow_mode = 'break'` returns its partial result as a
    /// success, and the chunks that have reached the format belong to that result: the consumer keeps pulling them
    /// until the format is finalized by the executor, so the queue stays open - a chunk that is waiting for a free
    /// slot is delivered as well. On any other cancellation nothing is pulled any more, so the queue is cleared
    /// and closed, which also wakes up the pipeline if it is blocked on the full queue.
    void cancel(CancelReason reason) noexcept override
    {
        keep_queued_chunks_on_cancel = reason == CancelReason::CancelledByTimeout;
        IOutputFormat::cancel(reason);
    }

    void onCancel() noexcept override
    {
        if (!keep_queued_chunks_on_cancel)
            queue.clearAndFinish();
    }

    /// Called by the consumer when it abandons the result, whatever the reason the pipeline was cancelled for
    /// (`timeout_overflow_mode = 'throw'`, an error while sending the data, the destruction of the executor):
    /// the queue is cleared and closed, so that the pipeline is woken up if it is blocked on the full queue.
    void discardQueuedChunks() noexcept
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
    std::atomic<bool> keep_queued_chunks_on_cancel{false};
    Chunk totals;
    Chunk extremes;

    /// Is not used.
    static NullWriteBuffer out;

    ProfileInfo info;
};

}
