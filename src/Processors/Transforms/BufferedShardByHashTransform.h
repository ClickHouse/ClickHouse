#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
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

/// Bookkeeping for one physical buffer (by pointer) referenced by at least one currently-buffered charge
/// somewhere in a shuffle stage. `refcount` counts every live *visit* that currently holds a reference to it
/// via `BufferedShardByHashTransform::chargeColumnAndDescendants` (a block visits a buffer once per shard chunk
/// that reaches it, so a dictionary shared across `num_shards` shard chunks of one block has
/// `refcount == num_shards`, released by exactly that many calls to `releaseTouchedObjects`); `bytes` is the
/// buffer's own size (excluding whatever is reachable from it, which is tracked as separate entries), cached at
/// the moment it was first ever registered - it must not be re-measured at release time, since the underlying
/// chunk that kept the column alive may already be gone by then.
struct SharedObjectAccounting
{
    size_t refcount = 0;
    Int64 bytes = 0;
};

/// Budget state shared by all `BufferedShardByHashTransform` of one shuffle stage. The scatters run
/// concurrently - each `prepare()`/`work()` runs under its own node mutex, not a stage-wide lock - so both the
/// buffered-bytes counter and the shared-object de-duplication table are shared across them and guarded by
/// `mutex`. Sharing the table (not only the counter) is what charges a physical buffer referenced by more than
/// one scatter at once - e.g. a constant aggregate argument that `ExpressionActions` materializes once and every
/// input stream then buffers - exactly once for the whole stage, rather than once per scatter.
struct BufferedShardByHashBudget
{
    /// Guards `shared_object_refcounts` and the accounting updates to `total_buffered_bytes`.
    std::mutex mutex;
    /// Total resident buffered bytes across all scatters of the stage. Updated under `mutex` together with the
    /// table; the admission checks read it locklessly (a best-effort guardrail read is enough).
    std::atomic<Int64> total_buffered_bytes{0};
    /// One entry per physical buffer (by pointer) currently referenced by at least one live charge across the
    /// whole stage - any scatter's block shard chunks or transient pre-split `pending_input_chunk`. This is what
    /// lets a buffer `scatter` shares across the shard chunks of one block, across more than one block still
    /// buffered, or across sibling scatters (e.g. the same `ColumnConst`/`LowCardinality` payload the query
    /// evaluates once and every stream references) be charged exactly once for as long as any reference holds it.
    std::unordered_map<const void *, SharedObjectAccounting> shared_object_refcounts;
};

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
    /// all transforms sharing `budget_` (0 = no cap); exceeding the cap throws TOO_MANY_ROWS_OR_BYTES instead
    /// of buffering without limit, because with a selective consumer the only alternatives to reading ahead
    /// are deadlock or spilling.
    ///
    /// The cap is enforced at input-block granularity, on measured sizes only: admission is checked against
    /// each pulled block's measured size and re-checked after the block is split (`scatter` can grow buffers
    /// beyond the pre-split size). Rejection is never taken on an estimate, so the check runs only after a
    /// chunk has been read; each scatter (they run concurrently, each under its own node mutex) can therefore
    /// admit one chunk that it has already pulled before the shared counter reveals the cap is crossed, so the
    /// bytes read ahead can transiently exceed the cap by up to one block's post-split footprint per concurrent
    /// scatter before the exception is raised. Every measurement charges every distinct physical buffer exactly
    /// once, however many references hold it live - within one block (a `ColumnConst` payload projected into
    /// several columns, a shared `LowCardinality` dictionary), across every block currently buffered, and across
    /// the sibling scatters of the stage (the same payload/dictionary object referenced, by pointer, by more
    /// than one scatter - see `BufferedShardByHashBudget::shared_object_refcounts`). A chunk's footprint cannot
    /// be known before reading it, so any earlier enforcement would reject chunks whose actual bytes fit. The
    /// cap is a guardrail against unbounded read-ahead with an actionable error, not a byte-exact memory limit -
    /// the query memory tracker (`max_memory_usage`) remains the hard limit.
    BufferedShardByHashTransform(
        SharedHeader header,
        size_t num_shards_,
        ColumnNumbers key_columns_,
        size_t max_queue_length_ = MAX_QUEUE_LENGTH,
        size_t max_buffered_bytes_ = 0,
        std::shared_ptr<BufferedShardByHashBudget> budget_ = nullptr);

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
    /// `generateOutputChunks` charges the exact bytes actually resident after the split via
    /// `chargeColumnAndDescendants`, which also de-duplicates a buffer shared with any OTHER block still
    /// buffered or with a sibling scatter (see `BufferedShardByHashBudget::shared_object_refcounts`), not only
    /// within this one block. `touched_objects` records every physical buffer (pointer) this block's charge
    /// registered, so its exact contribution can be reversed - one buffer at a time, correctly leaving alone
    /// whatever another still-live reference shares with it - once the block no longer keeps any of its shard
    /// chunks alive (see `releaseTouchedObjects`).
    struct BlockBudget
    {
        std::vector<const void *> touched_objects;
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

    /// Registers `column` - and everything reachable from it (a `LowCardinality` dictionary, or any nested
    /// subcolumn `scatter` may share across the shards, across a different buffered block, or across a sibling
    /// scatter) - as referenced by the charge currently being computed (a block's shard chunks, or the transient
    /// pre-split `pending_input_chunk`). Every visit, including a repeat one, is appended to `touched` and bumps
    /// the object's `BufferedShardByHashBudget::shared_object_refcounts` entry, so `releaseTouchedObjects` can
    /// later reverse this call exactly, however many times the same object was visited. The caller must hold
    /// `budget->mutex` (the table and the counter are shared across all scatters of the stage).
    ///
    /// `total_bytes` is increased by the object's own bytes - excluding whatever is reachable from it, which is
    /// registered, and billed, separately - the moment it is registered for the first time ever (i.e. no other
    /// currently-buffered charge, from this or an earlier block or a sibling scatter, already references it); a
    /// repeat visit (this object is already referenced by some still-buffered charge) adds nothing and does not
    /// descend further, since whichever visit registered it first already registered its whole subtree. This is
    /// what makes a buffer shared across many shard chunks of one block, across more than one buffered block
    /// (e.g. `ColumnConst::cloneResized` keeps the same backing payload), or across sibling scatters - charged
    /// exactly once for as long as any of them still holds it.
    void chargeColumnAndDescendants(const IColumn & column, std::vector<const void *> & touched, Int64 & total_bytes);
    /// Reverses `chargeColumnAndDescendants` for every object in `touched`: releases this charge's reference to
    /// each, and once an object's refcount reaches zero (no buffered charge references it any longer), subtracts
    /// its cached bytes from the shared counter and forgets it. Takes `budget->mutex` itself.
    void releaseTouchedObjects(const std::vector<const void *> & touched);

    /// Charge the just-pulled input chunk's measured size against the shared budget, before it is split. So the
    /// budget accounts for the in-flight read-ahead of every scatter and the admission decision in prepare()
    /// runs on the chunk's measured size. `chargeColumnAndDescendants` charges every distinct physical buffer
    /// exactly once for as long as anything still buffered references it, so a buffer this chunk shares with an
    /// already-buffered block or with a sibling scatter (e.g. a `LowCardinality` dictionary, or a `ColumnConst`
    /// payload `cloneResized` keeps by pointer) is not billed again. When the chunk is split the post-split
    /// shard chunks are registered first and this charge is released after (see `generateOutputChunks`), so any
    /// buffer the split shares with it (unchanged by the split) never drops to a zero refcount in between. The
    /// pre-split size is only an estimate of the post-split resident bytes - `scatter` can grow per-shard buffers
    /// beyond the source (e.g. `ColumnString` regrows each shard's `chars`) - so once the split is done
    /// `generateOutputChunks` reconciles this charge to the exact bytes actually buffered.
    void chargePendingInput();
    void dischargePendingInput();

    size_t num_shards;
    ColumnNumbers key_columns;
    /// 0 means no per-queue back-pressure (never stall on a full queue).
    size_t max_queue_length;
    /// Total-bytes cap for max_queue_length == 0 mode; 0 means no cap.
    size_t max_buffered_bytes;
    /// Budget shared by all scatters of the stage: the buffered-bytes counter, the shared-object de-duplication
    /// table, and the mutex guarding them (never null - the constructor makes a private one if none is passed).
    std::shared_ptr<BufferedShardByHashBudget> budget;

    /// Set in prepare() when the shared budget is already exhausted (so no further chunk is pulled) or when
    /// the just-pulled chunk's measured size pushes the counter past max_buffered_bytes; work() then throws
    /// before the chunk is split, so nothing over-budget buffers.
    bool budget_exceeded = false;

    /// Input chunk that was pulled in prepare() and will be split in work().
    bool has_pending_input_chunk = false;
    Chunk pending_input_chunk;
    /// Objects `chargePendingInput` registered for `pending_input_chunk` (see `chargeColumnAndDescendants`),
    /// released via `releaseTouchedObjects` once the chunk is split (`generateOutputChunks`) or dropped before
    /// it is split.
    std::vector<const void *> pending_input_touched;

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
