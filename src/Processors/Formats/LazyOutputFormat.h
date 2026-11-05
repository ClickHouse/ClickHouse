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
    /// `with_query_result_previews` must be set only by consumers that tell preview chunks apart
    /// from the result (by `Block::info.is_preview`), such as `TCPHandler` and `LocalConnection`.
    explicit LazyOutputFormat(SharedHeader header, bool with_query_result_previews_ = false);

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

    /// Preview chunks are queued like data (keeping their annotation) and delivered to the client
    /// by the native protocol as `PreviewData` packets (see `TCPHandler`). Only when the consumer
    /// asked for them: other consumers (scalar subqueries, gRPC, Prometheus) would take a preview
    /// for a part of the result.
    bool canWriteQueryResultPreviews() const override { return with_query_result_previews; }

protected:
    void consume(Chunk chunk) override
    {
        (void)(queue.emplace(std::move(chunk)));
    }

    void consumeQueryResultPreview(Chunk chunk) override
    {
        /// Previews precede the result: once a chunk of the result has been queued, a preview is
        /// stale, and the native protocol must not send `PreviewData` after `Data` (the same guard
        /// as in `IOutputFormat::consumeQueryResultPreview`). A preview can still arrive late, for
        /// example from another input of `IMergingTransformBase` after it has pushed its output.
        if (finalized || !need_write_prefix)
            return;
        (void)(queue.emplace(std::move(chunk)));
    }

    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

private:

    ConcurrentBoundedQueue<Chunk> queue;
    const bool with_query_result_previews;
    std::atomic<bool> keep_queued_chunks_on_cancel{false};
    Chunk totals;
    Chunk extremes;

    /// Is not used.
    static NullWriteBuffer out;

    ProfileInfo info;
};

}
