#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/MapToRange.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
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
    , shard_columns(num_shards)
{
    chassert(num_shards > 0);
}

void BufferedShardByHashTransform::enqueue(size_t shard, Chunk chunk)
{
    total_buffered_bytes->fetch_add(static_cast<Int64>(chunk.allocatedBytes()), std::memory_order_relaxed);
    output_queues[shard].push_back(std::move(chunk));
}

Chunk BufferedShardByHashTransform::dequeue(size_t shard)
{
    Chunk chunk = std::move(output_queues[shard].front());
    output_queues[shard].pop_front();
    total_buffered_bytes->fetch_sub(static_cast<Int64>(chunk.allocatedBytes()), std::memory_order_relaxed);
    return chunk;
}

void BufferedShardByHashTransform::clearQueue(size_t shard)
{
    Int64 bytes = 0;
    for (const auto & chunk : output_queues[shard])
        bytes += static_cast<Int64>(chunk.allocatedBytes());
    total_buffered_bytes->fetch_sub(bytes, std::memory_order_relaxed);
    output_queues[shard].clear();
}

void BufferedShardByHashTransform::chargePendingInput()
{
    pending_input_bytes = static_cast<Int64>(pending_input_chunk.allocatedBytes());
    total_buffered_bytes->fetch_add(pending_input_bytes, std::memory_order_relaxed);
}

void BufferedShardByHashTransform::dischargePendingInput()
{
    total_buffered_bytes->fetch_sub(pending_input_bytes, std::memory_order_relaxed);
    pending_input_bytes = 0;
}

IProcessor::Status BufferedShardByHashTransform::prepare()
{
    auto & input = getInputs().front();

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
        /// No more input will arrive, so finish every output whose queue is already drained. This is
        /// essential when a downstream *sorted* merge consumes the shards: a merge waits for EOF (or data)
        /// on every open input, and different shards drain at different times, so an already-empty output
        /// left open would make the merge wait forever while this scatter still holds data for other shards.
        bool any_queue_non_empty = false;
        auto drain_it = outputs.begin();
        for (size_t shard = 0; shard < num_shards; ++shard, ++drain_it)
        {
            if (output_queues[shard].empty())
                drain_it->finish();
            else
                any_queue_non_empty = true;
        }
        if (!any_queue_non_empty)
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
        if (max_queue_length == 0 && max_buffered_bytes != 0
            && total_buffered_bytes->load(std::memory_order_relaxed) > static_cast<Int64>(max_buffered_bytes))
        {
            budget_exceeded = true;
            return Status::Ready;
        }

        input.setNeeded();
        if (input.hasData())
        {
            pending_input_chunk = input.pull();
            has_pending_input_chunk = true;
            /// Charge the pulled chunk against the shared budget right away, before it is split in work().
            /// This closes the pre-admission gap the plain "check-then-pull, charge-on-enqueue" ordering left
            /// open: without it, every scatter could pull one full chunk past the cap before any charge landed,
            /// overshooting the budget by up to one chunk per scatter on wide pipelines.
            chargePendingInput();
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
        throw Exception(ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "Shuffled aggregation-in-order buffered more than {} bytes while repartitioning the input: "
            "the data distribution requires reading too far ahead (e.g. long runs of a single set of GROUP BY keys). "
            "Increase the setting `aggregation_in_order_shuffle_max_buffered_bytes` or disable the setting "
            "`aggregation_in_order_shuffle`",
            max_buffered_bytes);

    if (has_pending_input_chunk)
    {
        generateOutputChunks();
        has_pending_input_chunk = false;
    }

    /// Push one queued chunk per shard (if the port can accept it).
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        auto & queue = output_queues[shard];

        if (output_it->isFinished())
        {
            clearQueue(shard);
            continue;
        }

        if (queue.empty())
            continue;

        if (!output_it->canPush())
            continue;

        output_it->push(dequeue(shard));
    }
}

void BufferedShardByHashTransform::generateOutputChunks()
{
    const auto num_rows = pending_input_chunk.getNumRows();
    auto columns = pending_input_chunk.detachColumns();

    /// Release the input chunk's budget charge: its rows are re-charged per shard as they are enqueued below,
    /// so the shared counter keeps tracking exactly the bytes currently buffered.
    dischargePendingInput();

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

    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            continue;

        const size_t shard_rows = shard_columns[shard][0]->size();
        if (shard_rows == 0)
            continue;

        enqueue(shard, Chunk(std::move(shard_columns[shard]), shard_rows));
    }
}

}
