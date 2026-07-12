#pragma once

#include <atomic>
#include <deque>
#include <memory>

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/// Shards input rows to N output ports by hash(key) % N.
/// Hashes the key columns with `IColumn::computeHashInto` and physically splits every column with
/// `IColumn::scatter` so each output chunk holds only the rows belonging to its shard.
///
/// Output ports can only accept one chunk at a time (canPush/push). But one input chunk
/// produces N output chunks (one per shard), and downstream consume them at different rates.
/// Without queueing, we would have to wait until all N outputs are ready
/// before splitting an input chunk — one slow shard would stall all others.
///
/// So each output port has a FIFO queue. When a shard's port is busy, its chunk waits in
/// the queue and gets pushed on the next prepare()/work() cycle. This allows other shards
/// to continue processing without waiting for the slowest one.
///
/// TODO(nihalzp): A queue growing much faster than the others means the GROUP BY key
/// distribution is skewed onto one shard. That means one of the Aggregating hash tables would
/// be much bigger than the others, essentially serializing the pipeline. In that scenario, we could
/// potentially detect the skew from queue sizes and switch to a fallback where shard_i sends only to
/// AggregatingTransform_i % num_shards and then we merge at the end.
class BufferedShardByHashTransform : public IProcessor
{
public:
    /// `max_queue_length_` bounds each per-shard queue: once a queue hits it the transform stops pulling
    /// new input (back-pressure). Pass 0 for queues without per-queue back-pressure (never stall on a full
    /// queue) — required when a downstream *sorted* merge consumes the shards selectively, where
    /// back-pressure could deadlock. In that mode, `max_buffered_bytes_` caps the total bytes queued across
    /// all transforms sharing `total_buffered_bytes_` (0 = no cap); exceeding the cap throws
    /// TOO_MANY_ROWS_OR_BYTES instead of buffering without limit, because with a selective consumer the only
    /// alternatives to reading ahead are deadlock or spilling.
    BufferedShardByHashTransform(
        SharedHeader header,
        size_t num_shards_,
        ColumnNumbers key_columns_,
        size_t max_queue_length_ = MAX_QUEUE_LENGTH,
        size_t max_buffered_bytes_ = 0,
        std::shared_ptr<std::atomic<Int64>> total_buffered_bytes_ = nullptr);

    String getName() const override { return "BufferedShardByHashTransform"; }

    Status prepare() override;
    void work() override;

private:
    void generateOutputChunks();

    /// Default back-pressure threshold. Once any queue hits this length the transform stops pulling new
    /// input until the slow consumer drains it. Otherwise, we can have very high memory usage.
    static constexpr size_t MAX_QUEUE_LENGTH = 10;

    /// Queue bookkeeping that maintains the shared buffered-bytes counter.
    void enqueue(size_t shard, Chunk chunk);
    Chunk dequeue(size_t shard);
    void clearQueue(size_t shard);

    /// Charge/release the just-pulled input chunk against the shared budget. Charging happens the moment
    /// the chunk is pulled (before it is split and re-charged per shard), so the budget accounts for the
    /// in-flight read-ahead of every scatter and admission decisions cannot overshoot by a whole chunk.
    void chargePendingInput();
    void dischargePendingInput();

    size_t num_shards;
    ColumnNumbers key_columns;
    /// 0 means no per-queue back-pressure (never stall on a full queue).
    size_t max_queue_length;
    /// Total-bytes cap for max_queue_length == 0 mode; 0 means no cap.
    size_t max_buffered_bytes;
    /// Bytes currently queued across all transforms sharing this counter (never null).
    std::shared_ptr<std::atomic<Int64>> total_buffered_bytes;

    /// Set in prepare() when the shared budget is already exhausted, or when the just-pulled chunk pushes it
    /// past max_buffered_bytes; work() then throws before the chunk is split, so nothing over-budget buffers.
    bool budget_exceeded = false;

    /// Input chunk that was pulled in prepare() and will be split in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;
    /// Bytes charged against the shared budget for `pending_input_chunk` (released when it is split).
    Int64 pending_input_bytes = 0;

    /// Per-shard FIFO of chunks waiting to be pushed downstream. Bounded at MAX_QUEUE_LENGTH.
    std::vector<std::deque<Chunk>> output_queues;

    /// Reused across input chunks to skip per-chunk reallocation.
    PaddedPODArray<UInt32> hash_buffer;
    IColumn::Selector selector;
    std::vector<MutableColumns> shard_columns;
};

}
