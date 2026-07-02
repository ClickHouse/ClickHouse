#pragma once

#include <deque>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Common/PODArray.h>

namespace DB
{

/// Shards input rows to N output ports by hash(key) % N.
/// Hashes the key columns with `IColumn::computeHashInto` and physically splits
/// every column with `DB::ColumnsScatter::scatter`.
///
/// Output ports can only accept one chunk at a time (canPush/push). But one input chunk
/// produces N output chunks (one per shard), and downstream consumers process them at
/// different rates. Without queueing, we would have to wait until all N outputs are
/// ready before splitting an input chunk — one slow shard would stall all others.
///
/// So each output port has a FIFO queue. When a shard's port is busy, its chunk waits
/// in the queue and gets pushed on the next prepare()/work() cycle.
///
/// Input batching:
///   Input chunks are stashed in `pending_input` along with their per-row shard-ids.
///   All chunks share a single `PaddedPODArray<UInt32>` pids buffer. It grows to the
///   high-water mark, then stops reallocating. Once `accumulated_bytes >=
///   batch_threshold_bytes`, or when input is exhausted, a single
///   `ColumnsScatter::scatter` call per column-position fans all pending rows out to
///   the per-shard output queues. Chunks are atomic: the batch may overshoot the budget
///   by at most one input chunk's bytes.
///
///   `batch_threshold_bytes == 0` automatically degrades to per-chunk processing:
///   every chunk is flushed immediately through the same `ColumnsScatter::scatter`
///   path, so no batching across chunks occurs.
///
/// TODO(nihalzp): A queue growing much faster than the others means the GROUP BY key
/// distribution is skewed onto one shard. Detect the skew from queue sizes and switch
/// to a fallback where shard_i sends only to AggregatingTransform_i % num_shards and
/// then merge at the end.
class BufferedShardByHashTransform : public IProcessor
{
public:
    BufferedShardByHashTransform(SharedHeader header, size_t num_shards_, ColumnNumbers key_columns_, size_t batch_threshold_bytes_ = 0);

    String getName() const override { return "BufferedShardByHashTransform"; }

    Status prepare() override;
    void work() override;

private:
    void flushBatch();

    /// Once any queue hits this length the transform stops pulling new input until
    /// the slow consumer drains it.
    static constexpr size_t MAX_QUEUE_LENGTH = 10;

    size_t num_shards;
    ColumnNumbers key_columns;

    /// Input-batching budget for ColumnsScatter::scatter. A batch is flushed once the
    /// accumulated input bytes reach this budget. Counting bytes (rather than rows)
    /// keeps each flush short and its per-shard buffers small for wide rows, while
    /// narrow rows still accumulate until the budget is reached. When 0, each chunk is
    /// flushed immediately (no batching across chunks).
    size_t batch_threshold_bytes;

    // ── Common per-chunk state ──────────────────────────────────────────────────

    /// Input chunk pulled in prepare() and processed in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;

    /// 32-bit hash buffer, reused across chunks (always UInt32).
    PaddedPODArray<UInt32> hash_buffer;

    /// Shared pids accumulation buffer for the current batch. Each pending chunk
    /// records a pids_offset into this array. Grows to the high-water mark of the
    /// batch's row count, then stops reallocating. Cleared (capacity preserved) at the
    /// end of every flush.
    PaddedPODArray<UInt32> pids;

    // ── Batched path state ──────────────────────────────────────────────────────

    struct PendingChunk
    {
        Chunk chunk;
        size_t pids_offset; /// Index into the `pids` array where this chunk's pids begin.
    };

    std::deque<PendingChunk> pending_input;

    /// Accumulated input bytes since the last flush; compared against
    /// `batch_threshold_bytes` to decide when to flush the current batch.
    size_t accumulated_bytes = 0;

    /// Set when: (a) a stashed chunk crosses the threshold, or
    ///           (b) input is finished with pending_input non-empty.
    bool has_pending_flush = false;

    /// Pre-allocated buffers reused across every flush — avoids per-flush heap
    /// allocations that would otherwise dominate the batched scatter path.
    ///
    /// rows_per_shard: per-shard row counts, size == num_shards.
    ///   Zeroed at the start of each flush; passed to ColumnsScatter::scatter
    ///   for every column position so row counts are computed only ONCE per
    ///   flush (not K times — once per column).
    ///
    /// pids_spans_buf: view of each pending chunk's pids window inside pids.
    ///   Rebuilt once per flush (before the column loop) and reused across all
    ///   column positions.
    ///
    /// col_ptrs_buf: pointer-to-column for the current column position.
    ///   Resized to pending_input.size() once per flush; refilled per column
    ///   position inside the loop.
    PaddedPODArray<UInt32> rows_per_shard;
    std::vector<std::span<const UInt32>> pids_spans_buf;
    std::vector<const IColumn *> col_ptrs_buf;

    // ── Common state ────────────────────────────────────────────────────────────

    /// Per-shard FIFO of chunks waiting to be pushed downstream. Bounded at MAX_QUEUE_LENGTH.
    std::vector<std::deque<Chunk>> output_queues;
};

}
