#include <unordered_set>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
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

namespace
{

/// `scatter` can share one physical buffer across a block's shard chunks while `allocatedBytes()` reports it
/// on every shard: `ColumnLowCardinality::scatter` builds a single dictionary and `setShared`s it into every
/// shard, and `ColumnConst::scatter` wraps the same backing `data` column for every shard. The outer
/// `ColumnTuple`/`ColumnArray`/`ColumnNullable`/`ColumnMap` delegate `scatter` to their nested columns, so the
/// sharing appears at any nesting depth. Summing `allocatedBytes()` over the shards therefore counts such a
/// shared subobject up to `num_shards` times. This walks a shard chunk's columns recursively (pointer identity)
/// and, whenever the same column object is reached again, subtracts its bytes from `block_bytes`, so every
/// shared buffer is charged exactly once per block; a genuinely per-shard (distinct) buffer keeps its full
/// charge. `LowCardinality` needs an explicit case because `forEachSubcolumn` skips a shared dictionary
/// (the column does not own it) - the exact object this must de-duplicate.
void subtractDuplicateSharedSubobjects(const IColumn & column, std::unordered_set<const void *> & counted, Int64 & block_bytes)
{
    if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column))
    {
        if (!counted.insert(lc->getDictionaryPtr().get()).second)
            block_bytes -= static_cast<Int64>(lc->getDictionary().allocatedBytes());
        /// The index column is owned per shard and already counted; the dictionary is the only shared buffer.
        return;
    }

    column.forEachSubcolumn([&](const auto & subcolumn)
    {
        if (!counted.insert(subcolumn.get()).second)
        {
            /// This exact object was already counted through an earlier occurrence (its bytes are included in
            /// every referencing parent's `allocatedBytes()`), so remove the duplicate charge. Its subtree was
            /// de-duplicated together with the first occurrence, so don't descend into it again.
            block_bytes -= static_cast<Int64>(subcolumn->allocatedBytes());
            return;
        }
        subtractDuplicateSharedSubobjects(*subcolumn, counted, block_bytes);
    });
}

/// Adds `column`'s `allocatedBytes()` to `bytes`, de-duplicated against everything already charged through the
/// same `counted` set: a column object already charged in full through an earlier reference contributes nothing,
/// and a distinct column has any subobject it shares with a previously charged column discounted by the walk
/// above. Sharing does not only appear across a block's shard chunks after `scatter`: a single source chunk can
/// reference the same physical buffer more than once (e.g. one `ColumnConst` literal projected into two columns
/// of the block holds the same payload twice), so the pre-split admission charge and the post-split resident
/// charge both run this same ownership-aware accounting - a naive `allocatedBytes()` sum would charge such a
/// buffer once per reference and could reject a chunk whose actual bytes fit the budget.
void addAllocatedBytesOnce(const IColumn & column, std::unordered_set<const void *> & counted, Int64 & bytes)
{
    if (!counted.insert(&column).second)
        return;
    bytes += static_cast<Int64>(column.allocatedBytes());
    subtractDuplicateSharedSubobjects(column, counted, bytes);
}

}

BufferedShardByHashTransform::BufferedShardByHashTransform(
    SharedHeader header,
    size_t num_shards_,
    ColumnNumbers key_columns_,
    size_t max_queue_length_,
    size_t max_buffered_bytes_,
    std::shared_ptr<std::atomic<Int64>> total_buffered_bytes_)
    : IProcessor(InputPorts{header}, OutputPorts{num_shards_, header})
    , num_shards(num_shards_)
    , key_columns(std::move(key_columns_))
    , max_queue_length(max_queue_length_)
    , max_buffered_bytes(max_buffered_bytes_)
    , total_buffered_bytes(total_buffered_bytes_ ? std::move(total_buffered_bytes_) : std::make_shared<std::atomic<Int64>>(0))
    , output_queues(num_shards)
    , port_resident_block(num_shards)
    , shard_columns(num_shards)
{
    chassert(num_shards > 0);
}

void BufferedShardByHashTransform::enqueue(size_t shard, Chunk chunk, size_t block_id)
{
    ++block_budgets[block_id].outstanding_chunks;
    output_queues[shard].push_back(QueuedChunk{std::move(chunk), block_id});
}

BufferedShardByHashTransform::QueuedChunk BufferedShardByHashTransform::dequeue(size_t shard)
{
    QueuedChunk queued = std::move(output_queues[shard].front());
    output_queues[shard].pop_front();
    /// The chunk is not released here: it moves from the queue into the output port and stays resident in the
    /// port state (still consuming memory) until the downstream merge pulls it. `reclaimPortResidentChunks`
    /// releases its charge once that happens; until then it is tracked via `port_resident_block`.
    return queued;
}

void BufferedShardByHashTransform::clearQueue(size_t shard)
{
    for (const auto & queued : output_queues[shard])
        releaseQueuedChunk(queued.block_id);
    output_queues[shard].clear();
}

void BufferedShardByHashTransform::releaseQueuedChunk(size_t block_id)
{
    auto it = block_budgets.find(block_id);
    chassert(it != block_budgets.end());
    /// The block's whole charge is released only when its last buffered shard chunk is gone, so a buffer
    /// shared across the shards (e.g. a `LowCardinality` dictionary) stays charged for exactly as long as
    /// the block keeps it alive.
    if (--it->second.outstanding_chunks == 0)
    {
        total_buffered_bytes->fetch_sub(it->second.bytes, std::memory_order_relaxed);
        block_budgets.erase(it);
    }
}

void BufferedShardByHashTransform::reclaimPortResidentChunks()
{
    /// A chunk pushed to an output port stays buffered in the port state until the downstream merge pulls it
    /// (`hasData()` flips back to false). Releasing its charge any earlier (e.g. when it left the local
    /// queue) would let bytes still resident between the scatter and the merge escape the shared budget: a
    /// block that hashes entirely to one shard could park a full block in each of the `num_shards` ports
    /// while the counter reads zero, defeating `aggregation_in_order_shuffle_max_buffered_bytes`.
    ///
    /// A parked chunk on a *finished* port can only mean the downstream closed the port without pulling
    /// (cancellation, LIMIT): OutputPort::finish() and InputPort::close() both set the same IS_FINISHED
    /// flag, but this transform never finishes a port that still holds a parked chunk (see the EOF drain in
    /// prepare()) and never pushes to a finished port, so IS_FINISHED here is always the downstream's doing.
    /// Such a chunk is unreachable - nothing can pull it, its memory is freed only at pipeline teardown - so
    /// its charge is released rather than kept: keeping it could only make sibling scatters throw for bytes
    /// nobody can reclaim, while that subtree of the pipeline is shutting down anyway.
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (port_resident_block[shard].has_value() && (output_it->isFinished() || !output_it->hasData()))
        {
            releaseQueuedChunk(*port_resident_block[shard]);
            port_resident_block[shard].reset();
        }
    }
}

void BufferedShardByHashTransform::chargePendingInput(Int64 already_reserved)
{
    /// Charge the chunk's measured size the moment it is pulled, before it is split, so the shared counter
    /// accounts for the in-flight read-ahead of every scatter and the admission decision in prepare() runs on
    /// measured bytes. The measurement is ownership-aware, like the post-split accounting: a source chunk can
    /// reference the same physical buffer more than once (e.g. one `ColumnConst` literal projected into two
    /// columns of the block), and a raw `allocatedBytes()` sum would charge such a buffer once per reference,
    /// arming the admission rejection for bytes the chunk does not actually hold. The pre-split size is only
    /// an estimate of the post-split resident bytes - `scatter` can grow per-shard buffers beyond the source (e.g.
    /// `ColumnString` regrows each shard's `chars`) - so once the split is done `generateOutputChunks`
    /// reconciles this charge to the exact bytes actually buffered.
    ///
    /// `already_reserved` bytes were added to the counter before the pull as a provisional reservation (see
    /// the admission path in prepare()), so add only the difference to reconcile the reservation to the chunk's
    /// exact size rather than double-charging it. Record that size as the estimate to reserve before the next
    /// pull.
    Int64 measured_bytes = 0;
    std::unordered_set<const void *> counted_subobjects;
    for (const auto & column : pending_input_chunk.getColumns())
        addAllocatedBytesOnce(*column, counted_subobjects, measured_bytes);

    pending_input_bytes = measured_bytes;
    total_buffered_bytes->fetch_add(pending_input_bytes - already_reserved, std::memory_order_relaxed);
    reservation_estimate = pending_input_bytes;
}

void BufferedShardByHashTransform::dischargePendingInput()
{
    total_buffered_bytes->fetch_sub(pending_input_bytes, std::memory_order_relaxed);
    pending_input_bytes = 0;
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

    /// Release the charge for any chunk the downstream merge has already pulled out of an output port (or that
    /// a finished output discarded), so port-resident bytes are counted for exactly as long as they are held.
    reclaimPortResidentChunks();

    /// Free queues for outputs closed by downstream
    bool all_finished = true;
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            clearQueue(shard);
        else
            all_finished = false;
    }

    if (all_finished)
    {
        /// Release the pending input's budget charge if we finish before splitting it, so a leftover charge
        /// never makes sibling scatters (sharing the counter) trip the budget spuriously.
        if (has_pending_input_chunk)
        {
            dischargePendingInput();
            pending_input_chunk = {};
            has_pending_input_chunk = false;
        }
        input.close();
        return Status::Finished;
    }

    /// Pending input chunk takes priority - split it before doing anything else.
    if (has_pending_input_chunk)
        return Status::Ready;

    /// Scan queues to decide what to do next.
    bool has_queued_chunks = false;         /// any shard has chunks waiting in its queue
    bool has_pushable_queued_chunks = false; /// at least one queued chunk can be pushed right now (port is ready)
    bool any_queue_at_capacity = false;      /// at least one shard's queue hit the back-pressure cap
    bool has_starving_ready_output = false;  /// at least one output wants data (canPush) but has nothing queued

    auto queued_output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++queued_output_it)
    {
        const auto & queue = output_queues[shard];
        const bool can_push = !queued_output_it->isFinished() && queued_output_it->canPush();
        if (max_queue_length != 0 && queue.size() >= max_queue_length)
            any_queue_at_capacity = true;
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
        auto drain_it = outputs.begin();
        for (size_t shard = 0; shard < num_shards; ++shard, ++drain_it)
        {
            if (output_queues[shard].empty() && !port_resident_block[shard].has_value())
                drain_it->finish();
            else
                fully_drained = false;
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

    /// Nothing can be pushed right now. Decide whether to pull a new input chunk.
    ///   - Demand-driven mode (max_queue_length == 0, used when a downstream *sorted* merge consumes the
    ///     shards selectively): pull only to feed an output that is ready AND starving (empty queue). This
    ///     never stalls the shared scatter on a slow lane — a slow lane just keeps its data buffered — so
    ///     there is no cross-lane deadlock, while a lane that is genuinely waiting for its next rows always
    ///     gets fed. The read-ahead this buffers is capped by max_buffered_bytes (shared across all scatters
    ///     of the stage): once the cap is hit, the next pull throws. Refusing to pull instead could deadlock
    ///     (a merge may need this scatter's EOF to make progress, and reaching EOF requires buffering
    ///     everything in between), so with a selective consumer the only bounded-memory behavior that cannot
    ///     hang is to fail the query.
    ///   - Bounded mode: classic back-pressure — pull unless some queue is at capacity.
    const bool may_pull = (max_queue_length == 0) ? has_starving_ready_output : !any_queue_at_capacity;
    if (may_pull)
    {
        const bool budget_enabled = max_queue_length == 0 && max_buffered_bytes != 0;

        /// Short-circuit: if the shared budget is already exhausted (e.g. a sibling scatter charged a chunk
        /// that crossed the cap) do not pull anything more - throw straight away.
        if (budget_enabled && total_buffered_bytes->load(std::memory_order_relaxed) > static_cast<Int64>(max_buffered_bytes))
        {
            budget_exceeded = true;
            return Status::Ready;
        }

        input.setNeeded();
        if (input.hasData())
        {
            Int64 reserved = 0;
            if (budget_enabled)
            {
                /// Publish a provisional reservation for this pull - this scatter's last pulled size (chunks
                /// from one input stream are ~uniform) - to the shared counter BEFORE pulling, so concurrent
                /// scatters (each prepare() runs under its own node mutex, not a stage-wide lock) observe each
                /// other's in-flight pulls and cannot all conclude at the same time that there is room for one
                /// more chunk each. The reservation is a soft gate: being only an estimate, it must never
                /// reject a chunk by itself - a wide block followed by a narrow one would fail the query even
                /// though the actual buffered bytes stay under the cap. The rejection decision is taken on
                /// measured sizes only: chargePendingInput reconciles the reservation to the pulled chunk's
                /// exact size right after the pull, and the check below rejects the chunk if the actual total
                /// crosses the cap.
                reserved = reservation_estimate;
                total_buffered_bytes->fetch_add(reserved, std::memory_order_relaxed);
            }

            pending_input_chunk = input.pull();
            has_pending_input_chunk = true;
            /// Reconcile the reservation to the pulled chunk's exact size, before it is split in work().
            chargePendingInput(reserved);

            /// Check the budget *after* charging, when the counter reflects the chunk's exact measured size
            /// plus any in-flight charges from concurrent scatters. This is the admission decision, taken on
            /// measured bytes only: it rejects the chunk that itself crosses the cap. Without it the very chunk
            /// that overshoots would still be split, and if it were the last chunk (input then reaches EOF,
            /// budget never re-checked) or several scatters admitted concurrently, the stage could exceed
            /// `max_buffered_bytes` and finish without ever throwing. work() throws before splitting the chunk,
            /// so no over-budget data buffers.
            if (budget_enabled && total_buffered_bytes->load(std::memory_order_relaxed) > static_cast<Int64>(max_buffered_bytes))
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

        if (has_pending_input_chunk)
        {
            dischargePendingInput();
            pending_input_chunk = {};
            has_pending_input_chunk = false;
        }
        budget_exceeded = false;
        return;
    }

    if (has_pending_input_chunk)
    {
        generateOutputChunks();
        has_pending_input_chunk = false;

        /// `generateOutputChunks` reconciled the provisional pre-split charge to the exact post-split resident
        /// bytes, which `scatter` can grow beyond that pre-split estimate (e.g. `ColumnString` does not reserve
        /// `chars`, so each shard regrows its own `chars` buffer). If that reconciliation pushed the shared
        /// counter past the cap, fail here: prepare()'s budget checks only run on the `may_pull` path before a
        /// pull, and the last scatter can reach EOF and drain straight to Finished without another pull, so the
        /// query could otherwise complete having buffered more than `max_buffered_bytes`. The carve-out matches
        /// the pre-split path: once every output is finished the buffered data is needed by nobody.
        ///
        /// This check running only after the split is the enforcement granularity of the budget (see the
        /// constructor comment): the split of one already-admitted block can transiently exceed the cap before
        /// the throw. A pre-split bound on `scatter` amplification would have to be an estimate, and rejecting
        /// on an estimate fails queries whose actual footprint fits (the same false-positive class the soft
        /// admission gate avoids). The budget is a guardrail against unbounded read-ahead with an actionable
        /// error, not a byte-exact cap; the query memory tracker (`max_memory_usage`) accounts these
        /// allocations as they are made and remains the hard memory limit.
        const bool budget_enabled = max_queue_length == 0 && max_buffered_bytes != 0;
        if (budget_enabled
            && total_buffered_bytes->load(std::memory_order_relaxed) > static_cast<Int64>(max_buffered_bytes)
            && !allOutputsFinished())
            throwBufferBudgetExceeded();
    }

    /// Push one queued chunk per shard (if the port can accept it).
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        auto & queue = output_queues[shard];

        if (output_it->isFinished())
        {
            /// The output closed since prepare(): drop the chunk parked in its port (if any) and its queue.
            if (port_resident_block[shard].has_value())
            {
                releaseQueuedChunk(*port_resident_block[shard]);
                port_resident_block[shard].reset();
            }
            clearQueue(shard);
            continue;
        }

        if (queue.empty())
            continue;

        if (!output_it->canPush())
            continue;

        /// canPush() implies the port holds no data, so any previously pushed chunk was already reclaimed in
        /// prepare(); record this chunk as resident until the merge pulls it (see `reclaimPortResidentChunks`).
        QueuedChunk queued = dequeue(shard);
        port_resident_block[shard] = queued.block_id;
        output_it->push(std::move(queued.chunk));
    }
}

void BufferedShardByHashTransform::generateOutputChunks()
{
    const auto num_rows = pending_input_chunk.getNumRows();
    /// The provisional charge added when this chunk was pulled (`chargePendingInput`, the whole pre-split
    /// block via `allocatedBytes()`) is already in the shared counter. Below we replace it with the exact
    /// bytes actually resident after the split and reconcile the counter by the difference.
    const Int64 provisional_bytes = pending_input_bytes;
    pending_input_bytes = 0;
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
        auto split = column->scatter(num_shards, selector);
        for (size_t s = 0; s < num_shards; ++s)
            shard_columns[s].push_back(std::move(split[s]));
    }

    /// Charge the exact bytes resident after the split. `scatter` copies each column into independent per-shard
    /// buffers, so the post-split total can exceed the pre-split block (e.g. `ColumnString` does not reserve
    /// `chars`, so each shard regrows its own `chars` buffer by doubling); the pre-split provisional estimate
    /// would under-count that. Sum each buffered shard chunk's `allocatedBytes()`, then discount any buffer
    /// `scatter` shares across the shards (a `LowCardinality` dictionary, a `ColumnConst` payload) so it is
    /// charged exactly once per block (see `subtractDuplicateSharedSubobjects`) - counting a shared buffer once
    /// per shard would inflate the counter up to `num_shards` times and trip the budget spuriously on safe
    /// inputs.
    const size_t block_id = next_block_id++;
    Int64 block_bytes = 0;
    std::unordered_set<const void *> counted_subobjects;
    size_t enqueued_chunks = 0;
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            continue;

        const size_t shard_rows = shard_columns[shard][0]->size();
        if (shard_rows == 0)
            continue;

        for (const auto & column : shard_columns[shard])
            addAllocatedBytesOnce(*column, counted_subobjects, block_bytes);

        enqueue(shard, Chunk(std::move(shard_columns[shard]), shard_rows), block_id);
        ++enqueued_chunks;
    }

    if (enqueued_chunks > 0)
    {
        /// The block keeps its buffers (including any shared dictionaries) alive as long as any of its shard
        /// chunks is buffered, so the bytes are part of the block's charge and released with it (when its last
        /// shard chunk drains). Reconcile the provisional charge already in the counter to these exact bytes.
        total_buffered_bytes->fetch_add(block_bytes - provisional_bytes, std::memory_order_relaxed);
        block_budgets[block_id].bytes = block_bytes;
    }
    else
    {
        /// No shard chunk was buffered (every row went to an already-finished output), so release the charge.
        total_buffered_bytes->fetch_sub(provisional_bytes, std::memory_order_relaxed);
    }
}

}
