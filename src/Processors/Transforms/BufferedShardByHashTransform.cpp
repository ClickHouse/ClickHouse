#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/MapToRange.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
}

static Int64 releaseChunkChargeUnlocked(BufferedShardByHashBudget & budget, const std::vector<const void *> & touched)
{
    Int64 released_bytes = 0;
    for (const void * ptr : touched)
    {
        auto it = budget.shared_object_refcounts.find(ptr);
        chassert(it != budget.shared_object_refcounts.end());
        if (--it->second.refcount == 0)
        {
            released_bytes += it->second.bytes;
            budget.shared_object_refcounts.erase(it);
        }
    }
    return released_bytes;
}

struct BufferedShardByHashChunkCharge
{
    BufferedShardByHashChunkCharge(std::shared_ptr<BufferedShardByHashBudget> budget_, const std::vector<const void *> & touched_)
        : budget(std::move(budget_))
        , touched(touched_)
    {
    }

    void activate()
    {
        active.store(true, std::memory_order_relaxed);
    }

    void releaseUnlocked()
    {
        if (!active.exchange(false, std::memory_order_relaxed))
            return;

        budget->total_buffered_bytes.fetch_sub(releaseChunkChargeUnlocked(*budget, touched), std::memory_order_relaxed);
    }

    void release()
    {
        /// The charge is attached to a queued chunk before it is transferred from the queue. If insertion into
        /// `ChunkInfoCollection` fails, that inactive ChunkInfo is destroyed while its caller holds this mutex.
        /// It owns no accounting yet, so it must not try to acquire the same mutex during that cleanup.
        if (!active.load(std::memory_order_relaxed))
            return;

        std::lock_guard lock(budget->mutex);
        releaseUnlocked();
    }

    std::shared_ptr<BufferedShardByHashBudget> budget;
    std::vector<const void *> touched;
    std::atomic_bool active = false;
};

namespace
{

/// Keeps a shuffle budget charge alive while a downstream processor retains the chunk. In particular,
/// `MergingSortedTransform` retains a pulled input until it advances that input.
class BufferedShardByHashChunkInfo final : public ChunkInfo
{
private:
    struct State
    {
        explicit State(std::shared_ptr<BufferedShardByHashChunkCharge> charge_)
            : charge(std::move(charge_))
        {
        }

        ~State()
        {
            charge->release();
        }

        std::shared_ptr<BufferedShardByHashChunkCharge> charge;
    };

public:
    explicit BufferedShardByHashChunkInfo(std::shared_ptr<BufferedShardByHashChunkCharge> charge_)
        : state(std::make_shared<State>(std::move(charge_)))
    {
    }

    Ptr clone() const override { return std::make_shared<BufferedShardByHashChunkInfo>(*this); }

private:
    std::shared_ptr<State> state;
};

}


BufferedShardByHashTransform::BufferedShardByHashTransform(
    SharedHeader header,
    size_t num_shards_,
    ColumnNumbers key_columns_,
    size_t max_buffered_bytes_,
    std::shared_ptr<BufferedShardByHashBudget> budget_)
    : IProcessor(InputPorts{header}, OutputPorts{num_shards_, header})
    , num_shards(num_shards_)
    , key_columns(std::move(key_columns_))
    , max_buffered_bytes(max_buffered_bytes_)
    , budget_enabled(max_buffered_bytes_ != 0)
    , budget(budget_ ? std::move(budget_) : std::make_shared<BufferedShardByHashBudget>())
    , output_queues(num_shards)
    , port_resident_charges(num_shards)
    , shard_columns(num_shards)
{
    chassert(num_shards > 0);

    /// Without a cap to enforce nobody ever reads the shared counter, so this transform charges nothing and
    /// has nothing for a sibling to reclaim - it stays out of the stage's scatter list entirely.
    if (!budget_enabled)
        return;

    std::lock_guard lock(budget->mutex);
    budget->scatters.push_back(this);
}

BufferedShardByHashTransform::~BufferedShardByHashTransform()
{
    if (!budget_enabled)
        return;

    std::lock_guard lock(budget->mutex);

    /// Nothing this scatter holds is resident any more, and once it is unregistered no sibling could reclaim it
    /// either, so release every charge it still has: the chunks in its queues and parked in its ports, and the
    /// provisional charge of a pending input chunk it never got to split. A shuffle stage normally tears down
    /// together with its budget, but a scatter destroyed while siblings keep running (an exception during
    /// pipeline construction, say) must not leave bytes charged that would make them throw.
    Int64 released_bytes = 0;
    for (const auto & queue : output_queues)
        for (const auto & queued : queue)
            released_bytes += releaseTouchedObjectsUnlocked(queued.touched_objects);
    for (const auto & parked : port_resident_charges)
        if (parked)
            parked->releaseUnlocked();
    released_bytes += releaseTouchedObjectsUnlocked(pending_input_touched);
    budget->total_buffered_bytes.fetch_sub(released_bytes, std::memory_order_relaxed);

    std::erase(budget->scatters, this);
}

void BufferedShardByHashTransform::enqueue(size_t shard, Chunk chunk, std::vector<const void *> & touched_objects)
{
    auto & queue = output_queues[shard];
    /// Committed in two steps so a failed allocation cannot strand the charge: growing the deque can throw, but
    /// at that point `touched_objects` is still owned by the caller, which rolls the charge back
    /// (`generateOutputChunks`); the moves into the committed slot below cannot fail.
    queue.emplace_back();
    queue.back() = QueuedChunk{std::move(chunk), std::move(touched_objects)};
}

BufferedShardByHashTransform::QueuedChunk BufferedShardByHashTransform::dequeue(size_t shard)
{
    QueuedChunk queued = std::move(output_queues[shard].front());
    output_queues[shard].pop_front();
    /// The chunk is not released here: it moves from the queue into the output port and stays resident in the
    /// port state (still consuming memory) until the downstream merge pulls it. `reclaimPortResidentChunks`
    /// releases its charge once that happens; until then it is tracked via `port_resident_touched`.
    return queued;
}

void BufferedShardByHashTransform::clearQueue(size_t shard)
{
    for (const auto & queued : output_queues[shard])
        releaseQueuedChunk(queued.touched_objects);
    output_queues[shard].clear();
}

void BufferedShardByHashTransform::releaseQueuedChunk(const std::vector<const void *> & touched_objects)
{
    if (touched_objects.empty())
        return; /// Nothing was charged for this chunk (no budget to enforce).

    /// Releases this chunk's reference to every buffer it registered; a buffer shared with another chunk still
    /// buffered anywhere in the stage (e.g. a `LowCardinality` dictionary `scatter` shares across the shards of
    /// one block) keeps a non-zero refcount and stays charged for exactly as long as that chunk holds it.
    budget->total_buffered_bytes.fetch_sub(releaseTouchedObjectsUnlocked(touched_objects), std::memory_order_relaxed);
}

void BufferedShardByHashTransform::chargeSharedObject(
    const void * object, Int64 bytes, std::vector<const void *> & touched, Int64 & total_bytes)
{
    /// Registration is transactional: the reference recorded in `touched` and the table entry it refers to must
    /// appear together, or not at all. `touched` is the only handle a release path has, so an entry in it with
    /// no matching refcount would make `releaseTouchedObjectsUnlocked` decrement through a missing element -
    /// undefined behavior once `chassert` is compiled out. `try_emplace` goes first (it has the strong exception
    /// guarantee - hashing a pointer does not throw - so a failed insertion changes nothing); if recording the
    /// reference then fails, a just-created entry is removed again and the charge never happened.
    auto [it, is_new] = budget->shared_object_refcounts.try_emplace(object);
    try
    {
        touched.push_back(object);
    }
    catch (...)
    {
        if (is_new)
            budget->shared_object_refcounts.erase(it);
        throw;
    }
    ++it->second.refcount;
    if (!is_new)
        return; /// Already billed by whichever charge registered it first, and still held by it.

    it->second.bytes = bytes;
    total_bytes += bytes;
}

void BufferedShardByHashTransform::chargeColumnAndDescendants(
    const IColumn & column, std::vector<const void *> & touched, Int64 & total_bytes)
{
    /// Same transactional registration as `chargeSharedObject`: the table entry first (strong guarantee), the
    /// `touched` reference only once that succeeded, rolling a just-created entry back if it does not - so every
    /// pointer in `touched` always has a refcount to release. A node the descent below abandons mid-walk (a
    /// descendant's registration threw) is left at its default `bytes == 0`, so releasing it forgets nothing.
    auto [it, is_new] = budget->shared_object_refcounts.try_emplace(&column);
    try
    {
        touched.push_back(&column);
    }
    catch (...)
    {
        if (is_new)
            budget->shared_object_refcounts.erase(it);
        throw;
    }
    /// The recursion below inserts into the same table, which can rehash it - that invalidates iterators, but
    /// never references to the elements themselves, so this entry is held by reference across the descent.
    auto & accounting = it->second;
    ++accounting.refcount;

    /// The whole subtree is walked on every visit, whether or not this node is new. Bumping only this node's
    /// refcount and stopping would leave its descendants referenced by just the *first* charge that reached
    /// them, so releasing that charge would drop them to zero - forgetting their bytes, and their table entries,
    /// while a later charge still keeps the very same buffers alive through this shared node. That is exactly
    /// how a composite shared payload behaves: `ColumnConst::scatter` hands every shard the same `data` object,
    /// and for a `ColumnConst(Array(String))` the bulk of the bytes lives in that payload's children, not in the
    /// node the shards share. Bytes are still billed only on the first visit, so re-walking costs nothing but
    /// the traversal.
    ///
    /// `allocatedBytes()` already recursively sums every reachable subobject, so a new node starts from it and
    /// subtracts whatever is registered - and billed - separately below, leaving this node's own exclusive bytes
    /// (typically negligible wrapper/offset overhead for a composite column). The subtraction applies to every
    /// subobject regardless of whether it turns out to be new (billed via its own entry) or a duplicate (already
    /// billed elsewhere): either way its bytes must not also be attributed to this node.
    Int64 self_bytes = is_new ? static_cast<Int64>(column.allocatedBytes()) : 0;

    auto charge_subobject = [&](const IColumn & subobject, bool counted_in_allocated_bytes)
    {
        if (is_new && counted_in_allocated_bytes)
            self_bytes -= static_cast<Int64>(subobject.allocatedBytes());
        chargeColumnAndDescendants(subobject, touched, total_bytes);
    };

    column.forEachSubcolumn([&](const auto & subcolumn) { charge_subobject(*subcolumn, true); });

    if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column))
    {
        /// `forEachSubcolumn` visits an owned dictionary but skips a shared one (the column does not own it),
        /// so only a shared dictionary needs this explicit case. Running it for an owned dictionary too would
        /// subtract the dictionary from `self_bytes` a second time - `allocatedBytes` contains it exactly once
        /// either way - leaving the column billed a full dictionary short, so an over-budget chunk would pass
        /// the pre-split admission check and only fail after `scatter` had materialized it. The index column is
        /// owned per shard and contributes nothing beyond what's already in `self_bytes`.
        if (lc->isSharedDictionary())
            charge_subobject(lc->getDictionary(), true);
    }
    else if (const auto * aggregate = typeid_cast<const ColumnAggregateFunction *>(&column))
    {
        /// The states themselves live in arenas, and an arena is shared rather than owned: `scatter` hands every
        /// shard a *view* of this column - the view's `getData` holds the very same state pointers, the source's
        /// arena becomes one of the view's foreign arenas, and the view keeps the source column alive.
        /// `allocatedBytes` counts an owned arena in full, so two columns that reach the same arena would each
        /// charge it whole. Registering each arena as a shared object in its own right, keyed by the `Arena`
        /// address, bills it exactly once, for exactly as long as some buffered chunk still reaches it.
        ///
        /// The foreign arenas must be measured too: a column produced by aggregate-state arithmetic
        /// (`FunctionBinaryArithmetic`) or emitted by an upstream aggregation (`AggregationUtils`, `Aggregator`)
        /// carries its states in arenas attached via `addArena` only - it owns none - so skipping foreign arenas
        /// would charge nothing for its states at all. Sizing a foreign arena is safe: `Arena::allocatedBytes` is
        /// an atomic snapshot, readable while the arena's creator still grows it (nothing else of a foreign arena
        /// is touched here). A shared arena that keeps growing after the charge is under-counted by the growth -
        /// the budget bills what the block references when it is buffered, which is the bound it enforces.
        aggregate->forEachArena([&](const Arena & arena, bool is_owned)
        {
            const Int64 arena_bytes = static_cast<Int64>(arena.allocatedBytes());
            /// An owned arena is inside this column's `allocatedBytes` and has to come out of its own bytes;
            /// a foreign one never is.
            if (is_new && is_owned)
                self_bytes -= arena_bytes;
            chargeSharedObject(&arena, arena_bytes, touched, total_bytes);
        });

        if (const ColumnPtr & source = aggregate->getSourceColumn())
            charge_subobject(*source, false);
    }

    if (!is_new)
        return; /// Already billed when it was first seen; only the references had to be added.

    accounting.bytes = self_bytes;
    total_bytes += self_bytes;
}

Int64 BufferedShardByHashTransform::releaseTouchedObjectsUnlocked(const std::vector<const void *> & touched)
{
    Int64 released_bytes = 0;
    for (const void * ptr : touched)
    {
        auto it = budget->shared_object_refcounts.find(ptr);
        chassert(it != budget->shared_object_refcounts.end());
        if (--it->second.refcount == 0)
        {
            released_bytes += it->second.bytes;
            budget->shared_object_refcounts.erase(it);
        }
    }
    return released_bytes;
}

void BufferedShardByHashTransform::releaseTouchedObjects(const std::vector<const void *> & touched)
{
    if (touched.empty())
        return;

    std::lock_guard lock(budget->mutex);
    budget->total_buffered_bytes.fetch_sub(releaseTouchedObjectsUnlocked(touched), std::memory_order_relaxed);
}

std::unique_lock<std::mutex> BufferedShardByHashTransform::lockBudget()
{
    if (!budget_enabled)
        return {};
    return std::unique_lock(budget->mutex);
}

void BufferedShardByHashTransform::reclaimPortResidentChunks()
{
    /// A chunk pushed to an output port stays buffered in the port state until the downstream merge pulls it
    /// (`hasData()` flips back to false). Releasing its charge any earlier (e.g. when it left the local
    /// queue) would let bytes still resident between the scatter and the merge escape the shared budget: a
    /// block that hashes entirely to one shard could park a full block in each of the `num_shards` ports
    /// while the counter reads zero, defeating `aggregation_in_order_shuffle_max_buffered_bytes`.
    ///
    /// A *parked* chunk (the port still reports data) on a *finished* port can only mean the downstream closed
    /// the port without pulling (cancellation, LIMIT): OutputPort::finish() and InputPort::close() both set the
    /// same IS_FINISHED flag, but this transform never finishes a port that still holds a parked chunk (see the
    /// EOF drain in prepare()) and never pushes to a finished port, so IS_FINISHED here is always the
    /// downstream's doing. A finished port with no data is the opposite case - the chunk was pulled first and
    /// is still owned downstream - and must not be released here.
    /// Such a chunk is unreachable - nothing can pull it, its memory is freed only at pipeline teardown - so
    /// its charge is released rather than kept: keeping it could only make sibling scatters throw for bytes
    /// nobody can reclaim, while that subtree of the pipeline is shutting down anyway.
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (!port_resident_charges[shard])
            continue;

        /// Whether the chunk is still parked decides who owns its charge, so it is checked before
        /// `isFinished()`: a finished port with no data means the downstream pulled the chunk and only then
        /// closed the input, not that the chunk was discarded. That order really happens - a
        /// `MergingSortedTransform` keeps a pulled chunk in `IMergingTransformBase`'s retained input state (and
        /// afterwards in the algorithm's current inputs), and closes all of its inputs as soon as its own
        /// output finishes - so releasing on `isFinished()` alone would stop charging bytes that are still
        /// resident downstream.
        if (!output_it->hasData())
        {
            /// The downstream pulled the chunk. Its ChunkInfo now keeps the charge until the downstream
            /// processor releases its retained input.
            port_resident_charges[shard].reset();
        }
        else if (output_it->isFinished())
        {
            /// A finished output still reporting data has discarded that parked chunk. The ChunkInfo remains
            /// attached until the port is torn down, so release its shared charge explicitly now.
            port_resident_charges[shard]->releaseUnlocked();
            port_resident_charges[shard].reset();
        }
    }
}

void BufferedShardByHashTransform::reclaimAllPortResidentChunks()
{
    /// A scatter only notices that its parked chunk was pulled when the executor schedules it again - the pull
    /// itself just queues an edge update, it does not call back into the producer - while every scatter reads the
    /// shared counter on every admission decision. So reclaiming only our own ports would leave a sibling's
    /// already-pulled bytes charged for as long as the executor leaves that sibling alone, and this scatter would
    /// raise TOO_MANY_ROWS_OR_BYTES against memory nobody holds. The de-duplication table is worse off still: it
    /// is keyed by column address, so an entry left behind for a chunk the downstream has consumed - and freed -
    /// can be matched by a buffer a sibling allocates at the recycled address, which is then charged nothing and,
    /// once the stale reference is released, leaves the counter permanently wrong. So the whole stage is reclaimed
    /// not only before enforcement but before any registration, which is what keeps the table free of entries for
    /// objects that no longer exist. This is safe from any scatter's thread because all of it runs under
    /// `budget->mutex` (held by the caller), which every scatter also holds while it moves a chunk from a queue
    /// into a port, and the port flags a downstream pull updates are atomic (a pull that lands right after the
    /// check simply gets reclaimed by the next one - the chunk is alive until it is pulled, so no address it owns
    /// can be recycled while its entry is still in the table).
    for (auto * scatter : budget->scatters)
        scatter->reclaimPortResidentChunks();
}

bool BufferedShardByHashTransform::isOverBudget()
{
    auto total_buffered_bytes = budget->total_buffered_bytes.load(std::memory_order_relaxed);
    if (total_buffered_bytes <= 0 || static_cast<UInt64>(total_buffered_bytes) <= max_buffered_bytes)
        return false;

    /// Over the cap according to a reading that can include chunks the downstream merges have already pulled out
    /// of this or a sibling scatter's output ports (those charges are released lazily, by the owning scatter's
    /// next prepare()). Reclaim them across the stage and re-read, so the query is only failed for bytes that are
    /// really resident.
    std::lock_guard lock(budget->mutex);
    reclaimAllPortResidentChunks();
    total_buffered_bytes = budget->total_buffered_bytes.load(std::memory_order_relaxed);
    return total_buffered_bytes > 0 && static_cast<UInt64>(total_buffered_bytes) > max_buffered_bytes;
}

void BufferedShardByHashTransform::chargePendingInput()
{
    /// Charge the chunk's measured size the moment it is pulled, before it is split, so the shared counter
    /// accounts for the in-flight read-ahead of every scatter and the admission decision in prepare() runs on
    /// measured bytes. `chargeColumnAndDescendants` is ownership-aware across the whole stage, not only within
    /// this one chunk: a source chunk can reference the same physical buffer more than once (e.g. one
    /// `ColumnConst` literal projected into two columns of the block), and the very same buffer can also already
    /// be registered by an earlier still-buffered block, or by a sibling scatter (e.g. a `LowCardinality`
    /// dictionary, or a `ColumnConst` payload `cloneResized` keeps by pointer, that every stream evaluates and
    /// buffers); either way it is charged exactly once for as long as anything still buffered references it,
    /// rather than once per reference. The pre-split size is only an estimate of the post-split resident bytes -
    /// `scatter` can grow per-shard buffers beyond the source (e.g. `ColumnString` regrows each shard's `chars`)
    /// - so once the split is done `generateOutputChunks` reconciles this charge to the exact bytes buffered.
    if (!budget_enabled)
        return; /// No cap to enforce: nothing consults the counter, so do not walk the block at all.

    Int64 measured_bytes = 0;
    pending_input_touched.clear();

    std::lock_guard lock(budget->mutex);
    /// Registering into the shared table requires it to hold nothing for chunks that have already left the
    /// pipeline: this chunk's columns could have been allocated at the freed addresses of a consumed one.
    reclaimAllPortResidentChunks();
    try
    {
        for (const auto & column : pending_input_chunk.getColumns())
            chargeColumnAndDescendants(*column, pending_input_touched, measured_bytes);
    }
    catch (...)
    {
        /// A failed walk (an allocation threw) rolls its partial charge back by dropping the references alone.
        /// The bytes it billed live only in `measured_bytes`, never published to the shared counter, so the
        /// released bytes are discarded here - the ordinary release path (which teardown would take) subtracts
        /// them from the counter and would corrupt the stage-wide budget with bytes that were never added.
        releaseTouchedObjectsUnlocked(pending_input_touched);
        pending_input_touched.clear();
        throw;
    }
    budget->total_buffered_bytes.fetch_add(measured_bytes, std::memory_order_relaxed);
}

void BufferedShardByHashTransform::dischargePendingInput()
{
    releaseTouchedObjects(pending_input_touched);
    pending_input_touched.clear();
}

void BufferedShardByHashTransform::dropPendingInput()
{
    if (!has_pending_input_chunk)
        return;
    dischargePendingInput();
    pending_input_chunk = {};
    has_pending_input_chunk = false;
}

bool BufferedShardByHashTransform::allOutputsFinished() const
{
    for (const auto & output : outputs)
        if (!output.isFinished())
            return false;
    return true;
}

void BufferedShardByHashTransform::throwBufferBudgetExceeded() const
{
    throw Exception(ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
        "Shuffled aggregation-in-order buffered more than {} bytes while repartitioning the input: "
        "the data distribution requires reading too far ahead (e.g. long runs of a single set of GROUP BY keys). "
        "Increase the setting `aggregation_in_order_shuffle_max_buffered_bytes` or disable the setting "
        "`aggregation_in_order_shuffle`",
        max_buffered_bytes);
}

IProcessor::Status BufferedShardByHashTransform::prepare()
{
    auto & input = getInputs().front();

    bool all_finished = true;
    {
        auto lock = lockBudget();

        /// Release the charge for any chunk the downstream merge has already pulled out of an output port (or
        /// that a finished output discarded), so port-resident bytes are counted for exactly as long as they are
        /// held.
        if (budget_enabled)
            reclaimPortResidentChunks();

        /// Free queues for outputs closed by downstream
        auto output_it = outputs.begin();
        for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
        {
            if (output_it->isFinished())
                clearQueue(shard);
            else
                all_finished = false;
        }
    }

    if (all_finished)
    {
        /// Drop the pending input chunk if we reach EOF before splitting it.
        dropPendingInput();
        input.close();
        return Status::Finished;
    }

    /// Pending input chunk takes priority - split it before doing anything else.
    if (has_pending_input_chunk)
        return Status::Ready;

    /// Scan queues to decide what to do next.
    bool has_queued_chunks = false;         /// any shard has chunks waiting in its queue
    bool has_pushable_queued_chunks = false; /// at least one queued chunk can be pushed right now (port is ready)
    bool has_starving_ready_output = false;  /// at least one output wants data (canPush) but has nothing queued

    auto queued_output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++queued_output_it)
    {
        if (queued_output_it->isFinished())
            continue;

        const auto & queue = output_queues[shard];
        const bool can_push = queued_output_it->canPush();
        if (!queue.empty())
        {
            has_queued_chunks = true;
            if (can_push)
                has_pushable_queued_chunks = true;
        }
        else if (can_push)
            has_starving_ready_output = true;
    }

    /// Input exhausted - drain remaining queues, then finish.
    if (input.isFinished())
    {
        /// No more input will arrive, so finish every output that has nothing left buffered. This is
        /// essential when a downstream *sorted* merge consumes the shards: a merge waits for EOF (or data)
        /// on every open input, and different shards drain at different times, so an already-empty output
        /// left open would make the merge wait forever while this scatter still holds data for other shards.
        ///
        /// An output whose queue is empty but whose port still holds a parked chunk must NOT be finished
        /// yet: OutputPort::finish() only marks the port finished, it does not discard the parked chunk
        /// (the merge still sees the data and will pull it before observing EOF). Finishing now would make
        /// reclaimPortResidentChunks - which must treat IS_FINISHED as "downstream closed the port, the
        /// chunk is unreachable" - release the chunk's budget charge while its bytes are still resident
        /// between the scatter and the merge, under-counting the shared budget. The merge is not stalled by
        /// the delay: the parked chunk is data on that lane, and pulling it re-runs this prepare(), which
        /// then finishes the emptied port.
        bool fully_drained = true;
        {
            auto lock = lockBudget();
            auto drain_it = outputs.begin();
            for (size_t shard = 0; shard < num_shards; ++shard, ++drain_it)
            {
                if (output_queues[shard].empty() && !port_resident_charges[shard])
                    drain_it->finish();
                else
                    fully_drained = false;
            }
        }
        /// Likewise, do not return Finished while a chunk is still parked in a port: this processor would
        /// never run again, so the chunk's budget charge would stay in the shared counter forever and
        /// sibling scatters would trip the budget spuriously. Wait for the merge to pull the parked chunks
        /// (each pull re-runs prepare()); the last pass releases every charge and finishes every port.
        if (fully_drained)
            return Status::Finished;
        return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;
    }

    /// Push-priority: drain everything we can before reading more input. This keeps read-ahead (and hence
    /// memory) bounded by how far the fastest consumer runs ahead of the slowest, instead of pulling the
    /// whole input into the queues.
    if (has_pushable_queued_chunks)
        return Status::Ready;

    /// Nothing can be pushed right now. Decide whether to pull a new input chunk: pull only to feed an
    /// output that is ready AND starving (empty queue). This never stalls the shared scatter on a slow lane
    /// — a slow lane just keeps its data buffered — so there is no cross-lane deadlock, while a lane that is
    /// genuinely waiting for its next rows always gets fed. The read-ahead this buffers is capped by
    /// max_buffered_bytes (shared across all scatters of the stage): once the cap is hit, the next pull
    /// throws. Refusing to pull instead could deadlock (a merge may need this scatter's EOF to make progress,
    /// and reaching EOF requires buffering everything in between), so with a selective consumer the only
    /// bounded-memory behavior that cannot hang is to fail the query.
    const bool may_pull = has_starving_ready_output;
    if (may_pull)
    {
        /// Short-circuit: if bytes actually buffered elsewhere in the stage already exceed the cap (a sibling
        /// scatter has charged more than `max_buffered_bytes`), do not pull anything more - that sibling will
        /// throw, so fail here too instead of reading further ahead. This reads the shared counter, which holds
        /// only measured resident bytes (there are no provisional reservations), so it never rejects on an
        /// estimate: the counter can only exceed the cap once some scatter has actually buffered over it.
        if (budget_enabled && isOverBudget())
        {
            budget_exceeded = true;
            return Status::Ready;
        }

        input.setNeeded();
        if (input.hasData())
        {
            pending_input_chunk = input.pull();
            has_pending_input_chunk = true;
            /// Charge the chunk's measured size before it is split in work().
            chargePendingInput();

            /// Admission decision, taken on measured bytes only: reject the chunk once the shared counter -
            /// this chunk's exact measured size plus any concurrent siblings' measured chunks - crosses the
            /// cap. Rejecting only after the chunk is read (never on an estimate) means each concurrent scatter
            /// can admit one already-pulled chunk before the counter reveals the cap is crossed; that bounded
            /// per-scatter overshoot is the documented enforcement granularity (see the constructor comment).
            /// Without the check the very chunk that overshoots would still be split, and if it were the last
            /// chunk (input then reaches EOF, budget never re-checked) the stage could exceed `max_buffered_bytes`
            /// and finish without ever throwing. work() throws before splitting the chunk, so no over-budget
            /// data buffers.
            if (budget_enabled && isOverBudget())
                budget_exceeded = true;

            return Status::Ready;
        }
        return Status::NeedData;
    }

    /// Otherwise wait for a slow consumer to drain (it is not blocked on us: whenever a merge needs an input
    /// it marks that output "needed", which makes the corresponding lane pushable above).
    return has_queued_chunks ? Status::PortFull : Status::NeedData;
}

/// Split pending input chunk into per-shard queues, then drain queues to output ports.
void BufferedShardByHashTransform::work()
{
    if (budget_exceeded)
    {
        /// Between the prepare() that set `budget_exceeded` and this work(), a downstream can finish every
        /// output of this processor - a `LimitTransform` closes all of its upstream inputs the moment it
        /// reaches its limit, and so does a cancellation. Once every output is finished the buffered data is
        /// needed by nobody, so raising TOO_MANY_ROWS_OR_BYTES here would fail a query (e.g. an outer
        /// `LIMIT 1`) that is actually completing normally. In that case drop the pending chunk, releasing its
        /// budget charge, and return; the next prepare() observes the finished outputs and finishes this
        /// processor cleanly. When at least one output is still open the buffered data is genuinely needed, so
        /// the over-budget error stands.
        if (!allOutputsFinished())
            throwBufferBudgetExceeded();

        dropPendingInput();
        budget_exceeded = false;
        return;
    }

    if (has_pending_input_chunk)
    {
        /// Between the prepare() that pulled this chunk and this work(), a downstream can finish every output -
        /// a `LimitTransform` closes all of its upstream inputs the moment it reaches its limit, and so does a
        /// cancellation. The buffered block is then needed by nobody, so skip the repartitioning entirely
        /// instead of letting `generateOutputChunks` hash and `scatter` the whole block - materializing every
        /// per-shard column up front, and possibly hitting `max_memory_usage` - only to enqueue nothing because
        /// every output is finished. This mirrors the all-outputs-finished carve-out on the budget_exceeded path
        /// above; the next prepare() observes the finished outputs and finishes this processor cleanly.
        if (allOutputsFinished())
        {
            dropPendingInput();
            return;
        }

        generateOutputChunks();
        has_pending_input_chunk = false;

        /// `generateOutputChunks` replaced the provisional pre-split charge with the exact post-split resident
        /// bytes, which `scatter` can grow beyond that pre-split estimate (e.g. `ColumnString` does not reserve
        /// `chars`, so each shard regrows its own `chars` buffer). If that pushed the shared counter past the
        /// cap, fail here: prepare()'s budget checks only run on the `may_pull` path before a pull, and the
        /// last scatter can reach EOF and drain straight to Finished without another pull, so the query could
        /// otherwise complete having buffered more than `max_buffered_bytes`. The carve-out matches the
        /// pre-split path: once every output is finished the buffered data is needed by nobody.
        ///
        /// This check running only after the split is part of the enforcement granularity of the budget (see
        /// the constructor comment): the split of one already-admitted block can transiently exceed the cap
        /// before the throw. A pre-split bound on `scatter` amplification would have to be an estimate, and
        /// rejecting on an estimate fails queries whose actual footprint fits. The budget is a guardrail against
        /// unbounded read-ahead with an actionable error, not a byte-exact cap; the query memory tracker
        /// (`max_memory_usage`) accounts these allocations as they are made and remains the hard memory limit.
        if (budget_enabled && isOverBudget() && !allOutputsFinished())
            throwBufferBudgetExceeded();
    }

    /// Push one queued chunk per shard (if the port can accept it). Under `budget->mutex`, so that a sibling
    /// scatter reclaiming this scatter's port-resident charges never observes a chunk half-way between the queue
    /// and the port - it would see an empty port for a chunk already recorded as parked and release a charge that
    /// is about to become resident.
    auto lock = lockBudget();
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        auto & queue = output_queues[shard];

        if (output_it->isFinished())
        {
            /// The output closed since prepare(): drop the chunk parked in its port (if any) and its queue.
            if (port_resident_charges[shard])
            {
                /// Only a chunk the downstream never pulled is discarded by the close. If the port no longer
                /// reports data, the chunk is live downstream (a merge can pull a chunk and close the input
                /// right after), and its ChunkInfo keeps the charge until that owner releases it.
                if (output_it->hasData())
                    port_resident_charges[shard]->releaseUnlocked();
                port_resident_charges[shard].reset();
            }
            clearQueue(shard);
            continue;
        }

        if (queue.empty())
            continue;

        if (!output_it->canPush())
            continue;

        /// The charge starts inactive, so if `ChunkInfoCollection::add` throws while this mutex is held its
        /// cleanup does not attempt to re-lock it. Once the collection owns the ChunkInfo, dequeue transfers
        /// the queue's charge to it and the explicit port handle releases it if downstream closes this output.
        if (budget_enabled)
        {
            auto charge = std::make_shared<BufferedShardByHashChunkCharge>(budget, queue.front().touched_objects);
            auto info = std::make_shared<BufferedShardByHashChunkInfo>(charge);
            queue.front().chunk.getChunkInfos().add(std::move(info));
            QueuedChunk queued = dequeue(shard);
            charge->activate();
            port_resident_charges[shard] = std::move(charge);
            output_it->push(std::move(queued.chunk));
        }
        else
        {
            output_it->push(std::move(dequeue(shard).chunk));
        }
    }
}

void BufferedShardByHashTransform::generateOutputChunks()
{
    const auto num_rows = pending_input_chunk.getNumRows();
    auto columns = pending_input_chunk.detachColumns();

    chassert(!columns.empty());

    /// Compute a composite 32-bit hash over all key columns into a reusable buffer.
    /// No allocations: each `computeHashInto` call writes directly into hash_buffer.
    hash_buffer.assign(static_cast<size_t>(num_rows), WEAK_HASH32_INITIAL_VALUE);
    for (auto column_number : key_columns)
        columns[column_number]->computeHashInto(0, num_rows, hash_buffer.data(), false);

    selector.resize(num_rows);
    mapToRange(hash_buffer.data(), num_rows, static_cast<UInt32>(num_shards), selector.data());

    /// Physically split every column into N per-shard mutable columns.
    for (auto & cols : shard_columns)
        cols.clear();

    for (const auto & column : columns)
    {
        /// A downstream `LimitTransform` closes every input of this processor the moment it reaches its limit,
        /// and so does a cancellation - that can happen after the `allOutputsFinished()` carve-out in work()
        /// let this split start, while this loop is still materializing the per-shard copies. Re-check between
        /// columns: once every output is finished nobody will ever consume them, so stop scattering, drop what
        /// was built so far and release the pre-split charge. The next prepare() observes the finished outputs
        /// and finishes this processor cleanly, exactly as on the pre-split carve-out in work().
        if (allOutputsFinished())
        {
            for (auto & cols : shard_columns)
                cols.clear();
            dischargePendingInput();
            return;
        }

        auto split = column->scatter(num_shards, selector);
        for (size_t s = 0; s < num_shards; ++s)
            shard_columns[s].push_back(std::move(split[s]));
    }

    /// Charge the exact bytes resident after the split. `scatter` copies each column into independent per-shard
    /// buffers, so the post-split total can exceed the pre-split block (e.g. `ColumnString` does not reserve
    /// `chars`, so each shard regrows its own `chars` buffer by doubling); the pre-split provisional estimate
    /// would under-count that. `chargeColumnAndDescendants` charges every distinct buffer exactly once for as
    /// long as anything buffered still references it - a buffer `scatter` shares across the shards of this
    /// block (a `LowCardinality` dictionary, a `ColumnConst` payload), one shared with another still-live block
    /// (e.g. the same dictionary read again from the same part), and one shared with a sibling scatter (the same
    /// stage-wide `budget` de-duplicates all three). Registering these post-split objects before releasing the
    /// pre-split charge below (rather than reconciling by a byte delta) means such a buffer's refcount never
    /// drops to zero in between the two.
    ///
    /// Each shard chunk carries the charge it registered, so it is released exactly when that chunk stops being
    /// buffered; a buffer shared with another shard chunk of this block, with another buffered block, or with a
    /// sibling scatter stays billed until the last chunk referencing it is gone (that is what the refcounts in
    /// `BufferedShardByHashBudget::shared_object_refcounts` are for).
    ///
    /// The charging loop and the counter update run under `budget->mutex` (the de-dup table and counter are
    /// shared across all scatters); `enqueue` touches only this transform's own queues/bookkeeping.
    Int64 block_bytes = 0;
    {
        auto lock = lockBudget();

        /// Registering into the shared table requires it to hold nothing for chunks that have already left the
        /// pipeline: `scatter` just allocated these per-shard buffers, possibly at the freed addresses of a
        /// consumed chunk's columns, and a stale entry would make them look already charged.
        if (budget_enabled)
            reclaimAllPortResidentChunks();

        /// Pre-charging queue sizes, so a failure below can tell the chunks this call committed (whose charges
        /// live only in the unpublished `block_bytes`) from chunks that were already buffered.
        std::vector<size_t> queue_sizes_before;
        if (budget_enabled)
            for (const auto & queue : output_queues)
                queue_sizes_before.push_back(queue.size());

        std::vector<const void *> shard_touched;
        try
        {
            auto output_it = outputs.begin();
            for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
            {
                if (output_it->isFinished())
                    continue;

                const size_t shard_rows = shard_columns[shard][0]->size();
                if (shard_rows == 0)
                    continue;

                /// With no cap to enforce, skip the ownership walk of every scattered column: the queued chunk
                /// carries no charge and there is nothing to release when it leaves.
                shard_touched.clear();
                if (budget_enabled)
                    for (const auto & column : shard_columns[shard])
                        chargeColumnAndDescendants(*column, shard_touched, block_bytes);

                enqueue(shard, Chunk(std::move(shard_columns[shard]), shard_rows), shard_touched);
            }
        }
        catch (...)
        {
            /// An allocation threw mid-block (a charge walk, or growing a queue). The bytes charged for this
            /// block live only in `block_bytes`, which is never published now, so the partial charge is rolled
            /// back by dropping the references alone - the returned bytes are discarded, exactly as in
            /// `chargePendingInput`. That covers both the walk that threw halfway (`shard_touched`) and the
            /// shard chunks this call had already committed. The provisional pre-split charge stays: it was
            /// published, and whichever release path runs next (teardown, `dropPendingInput`) subtracts it
            /// correctly.
            if (budget_enabled)
            {
                releaseTouchedObjectsUnlocked(shard_touched);
                for (size_t shard = 0; shard < num_shards; ++shard)
                {
                    auto & queue = output_queues[shard];
                    while (queue.size() > queue_sizes_before[shard])
                    {
                        releaseTouchedObjectsUnlocked(queue.back().touched_objects);
                        queue.pop_back();
                    }
                }
            }
            /// The per-shard copies are consumed by nobody now - free them instead of leaving a whole
            /// scattered block resident until the next split or the destructor.
            for (auto & cols : shard_columns)
                cols.clear();
            throw;
        }

        /// Every shard skipped above - a finished output, or no rows for it - still holds its scattered copy,
        /// and an enqueued shard's vector is only left empty by the move into the chunk. Free them all here,
        /// BEFORE the pre-split charge is released below: nothing will ever consume the skipped copies, and
        /// leaving them in `shard_columns` keeps a whole scattered block resident - uncounted by
        /// `total_buffered_bytes` once the pre-split charge is gone - until the next split or the destructor.
        /// A query that already reached its outer `LIMIT` could otherwise hit `max_memory_usage` on data that
        /// nobody will read.
        for (auto & cols : shard_columns)
            cols.clear();

        /// Release the provisional pre-split charge now that the exact post-split objects are registered above -
        /// any buffer this block shares with the pre-split chunk (unchanged by the split, e.g. a `LowCardinality`
        /// dictionary or a `ColumnConst` payload) was just re-registered, so its refcount is already at least 2
        /// and releasing the pre-split reference here only drops it back to what this block itself holds.
        ///
        /// The whole reconciliation is published as ONE atomic update of the net difference: the admission
        /// checks in prepare() read the counter without taking `budget->mutex`, so a sibling scatter must never
        /// observe an intermediate state where the pre-split charge and the post-split charge of this block are
        /// both counted at once - it would arm `budget_exceeded` and fail the query on a transient
        /// reconciliation artifact rather than on bytes actually buffered. Each shard chunk keeps its buffers
        /// (including any shared dictionaries) alive as long as it is buffered, so its part of `block_bytes` is
        /// released with it, when it leaves the pipeline. When nothing was enqueued (every row went to an
        /// already-finished output) `block_bytes` is zero and this just releases the pre-split charge.
        if (budget_enabled)
        {
            const Int64 released_bytes = releaseTouchedObjectsUnlocked(pending_input_touched);
            budget->total_buffered_bytes.fetch_add(block_bytes - released_bytes, std::memory_order_relaxed);
        }
    }
    pending_input_touched.clear();
}

}
