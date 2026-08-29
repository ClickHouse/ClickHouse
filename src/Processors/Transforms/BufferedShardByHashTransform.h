#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
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

class BufferedShardByHashTransform;
struct BufferedShardByHashChunkCharge;

/// Bookkeeping for one physical buffer (by pointer) referenced by at least one currently-buffered charge
/// somewhere in a shuffle stage. `refcount` counts every live *visit* that currently holds a reference to it
/// via `BufferedShardByHashTransform::chargeColumnAndDescendants` (a buffer is visited once per buffered chunk
/// that reaches it, so a dictionary shared across the `num_shards` shard chunks of one block has
/// `refcount == num_shards`, released by exactly that many calls to `releaseTouchedObjects`); `bytes` is the
/// buffer's own size (excluding whatever is reachable from it, which is tracked as separate entries), cached at
/// the moment it was first ever registered - it must not be re-measured at release time, since the underlying
/// chunk that kept the column alive may already be gone by then.
///
/// The key is a raw address, so an entry must never outlive the object it accounts for: a buffer allocated later
/// could land on the freed address and be mistaken for this one (charged nothing, then billed this entry's stale
/// `bytes` when it is released). Two properties keep that from happening. Every charge is released as soon as the
/// chunk holding it stops being reachable - per buffered chunk, not per input block, so the exclusive buffers of
/// a shard chunk the downstream has consumed are forgotten even while its siblings from the same block stay
/// buffered. And the one charge whose release is inherently deferred - a chunk parked in an output port, released
/// only once the port shows the downstream pulled it, which the owning scatter can notice no earlier than its
/// next `prepare()` - is reclaimed across the whole stage before any new registration
/// (`BufferedShardByHashTransform::reclaimAllPortResidentChunks`), so no registration can ever consult the table
/// while it still holds an entry for a chunk that has left the pipeline.
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
    /// Guards `shared_object_refcounts`, `scatters`, the accounting updates to `total_buffered_bytes`, and the
    /// per-scatter bookkeeping of everything currently charged (each scatter's `output_queues` charges and
    /// `port_resident_charges`) - a scatter reclaims the stale charges of its siblings through `scatters`, so that
    /// bookkeeping is not private to one scatter any more.
    std::mutex mutex;
    /// Total resident buffered bytes across all scatters of the stage. Updated under `mutex` together with the
    /// table; the admission checks read it locklessly, so every update must land as ONE atomic operation on the
    /// net difference - in particular the post-split reconciliation (charge the exact post-split objects,
    /// release the provisional pre-split charge) must never expose an intermediate value where both charges of
    /// one block are counted at once, or a concurrent scatter would fail the query on a transient artifact.
    std::atomic<Int64> total_buffered_bytes{0};
    /// One entry per physical buffer (by pointer) currently referenced by at least one live charge across the
    /// whole stage - any scatter's block shard chunks or transient pre-split `pending_input_chunk`. A buffer here
    /// is not necessarily a column: an `Arena` holding aggregate-function states gets an entry of its own, since a
    /// `ColumnAggregateFunction` shares it rather than owning it. This is what
    /// lets a buffer `scatter` shares across the shard chunks of one block, across more than one block still
    /// buffered, or across sibling scatters (e.g. the same `ColumnConst`/`LowCardinality` payload the query
    /// evaluates once and every stream references) be charged exactly once for as long as any reference holds it.
    std::unordered_map<const void *, SharedObjectAccounting> shared_object_refcounts;
    /// Every scatter of the stage, so that any of them can reclaim the charges of chunks the downstream merges
    /// have already pulled out of ANY scatter's output ports before consulting - or adding to - the table and the
    /// counter. A scatter can only notice a pull of its own by running `prepare()` again (nothing calls back into
    /// it at the pull event), so without this a scatter that is not scheduled for a while would keep bytes charged
    /// that are no longer resident: a sibling would raise TOO_MANY_ROWS_OR_BYTES against them, and a buffer
    /// allocated at a freed address would be taken for one of them (see
    /// `BufferedShardByHashTransform::reclaimAllPortResidentChunks`). Entries are added by the constructor and
    /// removed by the destructor, which also releases whatever that scatter still had charged.
    std::vector<BufferedShardByHashTransform *> scatters;
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
    /// The per-shard queues have no per-queue back-pressure: the transform never stalls on a full queue,
    /// because the downstream *sorted* merge consumes the shards selectively (it waits for the smallest key),
    /// so back-pressure could deadlock. `max_buffered_bytes_` caps the total bytes queued across all
    /// transforms sharing `budget_` (0 = no cap); exceeding the cap throws TOO_MANY_ROWS_OR_BYTES instead
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
        size_t max_buffered_bytes_ = 0,
        std::shared_ptr<BufferedShardByHashBudget> budget_ = nullptr);

    ~BufferedShardByHashTransform() override;

    String getName() const override { return "BufferedShardByHashTransform"; }

    Status prepare() override;
    void work() override;

private:
    void generateOutputChunks();

    /// A buffered shard chunk, carrying the physical buffers (by pointer) its own budget charge registered, so
    /// that charge is reversed exactly when this chunk - and no other - stops being buffered. Accounting is per
    /// buffered chunk, not per input block: `scatter` shares one physical buffer across all shard chunks of a
    /// block (the canonical cases are a `LowCardinality` dictionary - `ColumnLowCardinality::scatter` keeps a
    /// single dictionary shared across the shards, at any nesting depth - and a `ColumnConst` payload, wrapped
    /// unchanged for every shard), but such a buffer is charged once for the whole stage anyway, by refcounting
    /// it in `BufferedShardByHashBudget::shared_object_refcounts`: every shard chunk that reaches it registers a
    /// reference, and it stays billed until the last of them releases it. Holding the charge of a whole input
    /// block together until its last shard chunk drained would bill the same bytes for exactly as long, but it
    /// would also keep an entry for the exclusive buffers of a shard chunk the downstream has long consumed,
    /// which the table (keyed by address) must not do - see `SharedObjectAccounting`.
    struct QueuedChunk
    {
        Chunk chunk;
        std::vector<const void *> touched_objects;
    };

    /// Queue bookkeeping that maintains the shared buffered-bytes counter. When `budget_enabled`, all of it -
    /// like everything else that touches the charges recorded in `output_queues` or `port_resident_charges` -
    /// runs with `budget->mutex` held: a sibling scatter reclaims this scatter's stale port-resident charges
    /// through `BufferedShardByHashBudget::scatters`. Without a budget the chunks carry no charge, so there is
    /// nothing shared to guard and the mutex is not taken (see `lockBudget`).
    /// `touched_objects` is taken by reference and moved from only once the queue slot is committed, so if
    /// growing the queue throws, the caller still owns the charge and can roll it back.
    void enqueue(size_t shard, Chunk chunk, std::vector<const void *> & touched_objects);
    QueuedChunk dequeue(size_t shard);
    void clearQueue(size_t shard);
    /// Account for one shard chunk leaving the pipeline (consumed downstream or discarded on a finished output):
    /// release its charge, which frees every buffer no other buffered chunk references any more.
    void releaseQueuedChunk(const std::vector<const void *> & touched_objects);
    /// Release the charge for a chunk parked in an output port once the downstream merge has pulled it
    /// (`OutputPort::hasData()` is false again) or the downstream closed the port without pulling, making the
    /// chunk unreachable. A pushed chunk stays resident in the port state until the merge pulls it, so its
    /// bytes must remain counted until then; the transform never finishes a port that still holds a parked
    /// chunk and never returns Finished while one remains (see the EOF drain in prepare()).
    void reclaimPortResidentChunks();
    /// The same, for every scatter of the stage (`BufferedShardByHashBudget::scatters`), so that neither the
    /// shared counter nor the de-duplication table holds anything for a chunk some sibling's downstream has
    /// already pulled. A scatter learns of a pull only when it is scheduled again, so its own charges can be
    /// stale for as long as the executor leaves it alone, while every scatter reads the shared counter on every
    /// admission decision and registers new objects in the shared table on every chunk it buffers. Hence this
    /// runs both before enforcement (`isOverBudget`) and before any registration (`chargePendingInput`,
    /// `generateOutputChunks`): a stale entry consulted by a registration would make a buffer allocated at the
    /// freed address of a consumed chunk's column look like an already-charged one (charged nothing, then billed
    /// the stale entry's bytes when the stale reference is finally released), permanently corrupting the budget.
    void reclaimAllPortResidentChunks();
    /// True when the bytes actually resident across the stage exceed `max_buffered_bytes`. Reads the shared
    /// counter locklessly first, and only when that (possibly stale) reading is over the cap takes
    /// `budget->mutex`, reclaims the already-pulled port-resident charges of every scatter, and re-reads: the
    /// budget must fail a query on bytes that are really held, never on a charge whose chunk is already gone.
    bool isOverBudget();

    /// True once every output port is finished. When the whole stage's outputs are closed the buffered data is
    /// needed by nobody, so exceeding the budget must not fail the query (e.g. an outer `LIMIT 1` completing).
    bool allOutputsFinished() const;
    /// Raise TOO_MANY_ROWS_OR_BYTES for `max_buffered_bytes`. Used from both budget-enforcement paths: the
    /// pre-split admission check (via `budget_exceeded`) and the post-split reconciliation re-check in work().
    [[noreturn]] void throwBufferBudgetExceeded() const;

    /// Registers `column` - and everything reachable from it (a `LowCardinality` dictionary, the arena holding a
    /// `ColumnAggregateFunction`'s states, or any nested subcolumn `scatter` may share across the shards, across a
    /// different buffered block, or across a sibling scatter) - as referenced by the charge currently being computed (a block's shard chunks, or the transient
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
    /// Same, for a shared object that is not a column and therefore has no subobjects of its own to walk: an
    /// `Arena` holding aggregate-function states, which several `ColumnAggregateFunction` reach at once (see
    /// `chargeColumnAndDescendants`). `bytes` is measured by the caller, since only it knows how to size the
    /// object; like a column, it is billed the first time it is registered and forgotten once the last charge
    /// referencing it is released. The caller must hold `budget->mutex`.
    void chargeSharedObject(const void * object, Int64 bytes, std::vector<const void *> & touched, Int64 & total_bytes);
    /// Reverses `chargeColumnAndDescendants` for every object in `touched`: releases this charge's reference to
    /// each, and once an object's refcount reaches zero (no buffered charge references it any longer), forgets
    /// it. Returns the total cached bytes of the objects released, which the caller must subtract from the
    /// shared counter (in one atomic update, possibly combined with a charge it registers in the same critical
    /// section - see `generateOutputChunks`). The caller must hold `budget->mutex`.
    Int64 releaseTouchedObjectsUnlocked(const std::vector<const void *> & touched);
    /// Same, but takes `budget->mutex` itself and subtracts the released bytes from the shared counter.
    void releaseTouchedObjects(const std::vector<const void *> & touched);

    /// A lock on `budget->mutex` when there is a budget to maintain, and an empty (unlocked, mutex-less) lock
    /// otherwise - with `budget_enabled` false nothing shared is touched, so the stage-wide mutex is not taken
    /// at all. Held around the queue/port bookkeeping, which a sibling scatter may reclaim through
    /// `BufferedShardByHashBudget::scatters`.
    std::unique_lock<std::mutex> lockBudget();

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
    /// Release the pending input chunk's provisional pre-split budget charge and drop it without splitting.
    /// Used on every path where the pulled block will never be scattered: the outputs all finished before
    /// work() could split it (a downstream LIMIT or cancellation), or the budget was exceeded. Leaving the
    /// charge behind would make sibling scatters (sharing the counter) trip the budget spuriously.
    void dropPendingInput();

    size_t num_shards;
    ColumnNumbers key_columns;
    /// Total-bytes cap on what the whole stage buffers; 0 means no cap.
    size_t max_buffered_bytes;
    /// True only when there is a byte cap to enforce (a non-zero `max_buffered_bytes`). When false -
    /// a stage with the cap explicitly disabled -
    /// nothing ever consults `total_buffered_bytes`, so the transform does no ownership accounting whatsoever:
    /// it neither walks the pulled block nor the scattered columns, keeps no charges in `QueuedChunk` /
    /// `port_resident_charges`, does not register in `BufferedShardByHashBudget::scatters`, and never takes
    /// `budget->mutex`. That accounting is two full recursive walks per input block plus hash-table churn, and
    /// the bounded-queue path is itself a hot-path optimization, so it must not pay for a budget it does not have.
    bool budget_enabled;
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

    /// Per-shard FIFO of chunks waiting to be pushed downstream. Unbounded chunk-wise; `max_buffered_bytes`
    /// bounds the resident bytes instead.
    std::vector<std::deque<QueuedChunk>> output_queues;

    /// For each shard, the budget charge of the chunk currently parked in its output port (empty if the port
    /// holds no chunk we pushed). The charge stays held until the downstream merge pulls the chunk out of the
    /// port; only then is it truly gone from the pipeline (a port can hold at most one chunk at a time).
    std::vector<std::shared_ptr<BufferedShardByHashChunkCharge>> port_resident_charges;

    /// Reused across input chunks to skip per-chunk reallocation.
    PaddedPODArray<UInt32> hash_buffer;
    IColumn::Selector selector;
    std::vector<MutableColumns> shard_columns;
};

}
