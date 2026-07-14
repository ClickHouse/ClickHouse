#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <optional>
#include <unordered_map>

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

    /// A queued shard chunk, tagged with the id of the input block it was scattered from so the block's
    /// budget charge can be released once its last shard chunk leaves the pipeline (see `block_budgets`).
    struct QueuedChunk
    {
        Chunk chunk;
        size_t block_id;
    };

    /// Budget bookkeeping for one input block. Accounting is per input block, NOT per queued chunk, because
    /// `scatter` can share one physical buffer across all shard chunks of a block (the canonical cases are a
    /// `LowCardinality` dictionary - `ColumnLowCardinality::scatter` keeps a single dictionary shared across
    /// the shards, at any nesting depth - and a `ColumnConst` payload, wrapped unchanged for every shard).
    /// `generateOutputChunks` charges the exact bytes actually resident after the split: it sums each buffered
    /// shard chunk's `allocatedBytes()` and then discounts every buffer `scatter` shares across the shards so
    /// it is charged once per block (identified by pointer, see `subtractDuplicateSharedSubobjects`). This
    /// captures buffers `scatter` grows beyond the pre-split block (e.g. `ColumnString` does not reserve
    /// `chars`, so each shard regrows its own `chars` buffer) without the two errors of a naive per-shard
    /// measure: counting a shared buffer once per shard (inflating the counter up to `num_shards` times) or,
    /// with `Chunk::bytes()`, dropping a shared dictionary entirely (it reports zero owned bytes). The charge
    /// is held until `outstanding_chunks` reaches zero, i.e. until the block no longer keeps any buffer alive.
    struct BlockBudget
    {
        Int64 bytes = 0;             /// The block's exact resident bytes after the split (a shared buffer counted once).
        size_t outstanding_chunks = 0; /// Shard chunks from this block still buffered (in a queue or an output port).
    };

    /// Queue bookkeeping that maintains the shared buffered-bytes counter.
    void enqueue(size_t shard, Chunk chunk, size_t block_id);
    QueuedChunk dequeue(size_t shard);
    void clearQueue(size_t shard);
    /// Account for one shard chunk of `block_id` leaving the pipeline (consumed downstream or discarded on a
    /// finished output); release the block's charge once its last shard chunk is gone.
    void releaseQueuedChunk(size_t block_id);
    /// Release the charge for a chunk parked in an output port once the downstream merge has pulled it
    /// (`OutputPort::hasData()` is false again) or the downstream closed the port without pulling, making the
    /// chunk unreachable. A pushed chunk stays resident in the port state until the merge pulls it, so its
    /// bytes must remain counted until then; the transform never finishes a port that still holds a parked
    /// chunk and never returns Finished while one remains (see the EOF drain in prepare()).
    void reclaimPortResidentChunks();

    /// True once every output port is finished. When the whole stage's outputs are closed the buffered data is
    /// needed by nobody, so exceeding the budget must not fail the query (e.g. an outer `LIMIT 1` completing).
    bool allOutputsFinished() const;
    /// Raise TOO_MANY_ROWS_OR_BYTES for `max_buffered_bytes`. Used from both budget-enforcement paths: the
    /// pre-split admission check (via `budget_exceeded`) and the post-split reconciliation re-check in work().
    [[noreturn]] void throwBufferBudgetExceeded() const;

    /// Charge/release the just-pulled input chunk against the shared budget. Charging happens the moment
    /// the chunk is pulled (before it is split), so the budget accounts for the in-flight read-ahead of
    /// every scatter and admission decisions cannot overshoot by a whole chunk. When the chunk is split the
    /// same charge is carried over as the block's charge (no discharge/re-charge), so the counter is
    /// continuous across the split.
    ///
    /// `already_reserved` bytes were added to the counter before the pull as a conservative reservation (see
    /// the admission path in prepare()); chargePendingInput adds only the difference to the chunk's exact
    /// `allocatedBytes()`, so the reservation is reconciled rather than double-charged. It also records the
    /// chunk's size as `reservation_estimate` to reserve before the next pull.
    void chargePendingInput(Int64 already_reserved);
    void dischargePendingInput();

    size_t num_shards;
    ColumnNumbers key_columns;
    /// 0 means no per-queue back-pressure (never stall on a full queue).
    size_t max_queue_length;
    /// Total-bytes cap for max_queue_length == 0 mode; 0 means no cap.
    size_t max_buffered_bytes;
    /// Bytes currently queued across all transforms sharing this counter (never null).
    std::shared_ptr<std::atomic<Int64>> total_buffered_bytes;

    /// Set in prepare() when the shared budget is already exhausted, when the reservation for the next chunk
    /// would not fit (so the chunk is not even pulled), or when the just-pulled chunk pushes the counter past
    /// max_buffered_bytes; work() then throws before the chunk is split (or before it is pulled at all), so
    /// nothing over-budget buffers.
    bool budget_exceeded = false;

    /// Input chunk that was pulled in prepare() and will be split in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;
    /// Bytes charged against the shared budget for `pending_input_chunk`. Carried over as the block's charge
    /// when the chunk is split, or released if the chunk is dropped before it is split.
    Int64 pending_input_bytes = 0;

    /// Pre-split `allocatedBytes()` of the last chunk this scatter pulled. Reserved against the shared budget
    /// *before* the next pull so concurrent scatters (each prepare() runs under its own node mutex, not a
    /// stage-wide lock) serialize their admission through the counter and cannot each materialize a chunk while
    /// it still reads below the cap. 0 until the first pull, so the first chunk per scatter can still be pulled
    /// before the estimate warms up - inherent to measuring a chunk's size only after reading it; the post-pull
    /// re-check in prepare() and the post-split re-check in work() still catch that first chunk. Chunks from one
    /// input stream are ~uniform, so this converges after a single pull.
    Int64 reservation_estimate = 0;

    /// Per-shard FIFO of chunks waiting to be pushed downstream. Bounded at MAX_QUEUE_LENGTH.
    std::vector<std::deque<QueuedChunk>> output_queues;

    /// For each shard, the block id of the chunk currently parked in its output port (empty if the port holds
    /// no chunk we pushed). The chunk's budget charge stays held until the downstream merge pulls it out of the
    /// port; only then is it truly gone from the pipeline (a port can hold at most one chunk at a time).
    std::vector<std::optional<size_t>> port_resident_block;

    /// Per-block budget charges, keyed by the block id carried in each `QueuedChunk`. An entry lives from the
    /// moment a block is split until its last shard chunk leaves the pipeline (a queue or an output port).
    std::unordered_map<size_t, BlockBudget> block_budgets;
    /// Monotonic id assigned to each input block when it is split.
    size_t next_block_id = 0;

    /// Reused across input chunks to skip per-chunk reallocation.
    PaddedPODArray<UInt32> hash_buffer;
    IColumn::Selector selector;
    std::vector<MutableColumns> shard_columns;
};

}
