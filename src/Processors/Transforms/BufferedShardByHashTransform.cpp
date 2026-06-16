#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
#include <Common/HashTable/Hash.h>
#include <Common/MapToRange.h>

namespace DB
{

BufferedShardByHashTransform::BufferedShardByHashTransform(SharedHeader header, size_t num_shards_, ColumnNumbers key_columns_)
    : IProcessor(InputPorts{header}, OutputPorts{num_shards_, header})
    , num_shards(num_shards_)
    , key_columns(std::move(key_columns_))
    , output_queues(num_shards)
    , shard_columns(num_shards)
{
    chassert(num_shards > 0);
}

IProcessor::Status BufferedShardByHashTransform::prepare()
{
    auto & input = getInputs().front();
    const bool input_finished = input.isFinished();

    /// First pass over outputs: clear queues of finished outputs, and finish outputs whose
    /// queue is empty once the input is exhausted (no chunk pending). Without finishing
    /// empty-queue outputs eagerly here, a downstream consumer that activates inputs
    /// sequentially (e.g. `ConcatProcessor`) waits forever on the empty path because it
    /// never receives a finish signal, while the queued chunks on the other shards
    /// can never drain because the consumer never advances to them.
    bool all_finished = true;
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
        {
            output_queues[shard].clear();
            continue;
        }
        if (input_finished && !has_pending_input_chunk && output_queues[shard].empty())
        {
            output_it->finish();
            continue;
        }
        all_finished = false;
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    /// Pending input chunk takes priority - split it before doing anything else.
    if (has_pending_input_chunk)
        return Status::Ready;

    /// Scan queues to decide what to do next. Skip outputs we have already finished.
    bool has_queued_chunks = false; /// any active shard has chunks waiting in its queue
    bool has_pushable_queued_chunks = false; /// at least one queued chunk can be pushed right now
    bool has_pushable_empty_port = false; /// an active shard whose queue is empty AND downstream is asking
    bool any_queue_at_capacity = false; /// at least one shard's queue hit the back-pressure cap

    auto queued_output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++queued_output_it)
    {
        if (queued_output_it->isFinished())
            continue;

        const auto & queue = output_queues[shard];
        if (queue.size() >= MAX_QUEUE_LENGTH)
            any_queue_at_capacity = true;
        if (queue.empty())
        {
            if (queued_output_it->canPush())
                has_pushable_empty_port = true;
            continue;
        }
        has_queued_chunks = true;
        if (queued_output_it->canPush())
            has_pushable_queued_chunks = true;
    }

    /// Input exhausted - drain remaining queues, then finish.
    /// All empty-queue active outputs were finished in the first pass, so if we got here
    /// then at least one queue is non-empty.
    if (input_finished)
    {
        chassert(has_queued_chunks);
        return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;
    }

    /// `PortFull` is correct only when we cannot make forward progress:
    ///  - no queued chunk is pushable right now, AND
    ///  - no empty port is waiting for fresh data we could route to it.
    /// Otherwise we must keep pulling input. A `ConcatProcessor` downstream activates
    /// inputs sequentially: if we back-pressure here, the active branch (an empty-queue
    /// port that has `canPush`) waits forever, and the queued chunks on the other
    /// shards never get drained because `Concat` never advances to them.
    if (!has_pushable_queued_chunks && !has_pushable_empty_port)
    {
        if (has_queued_chunks)
            return Status::PortFull;
        /// All active queues are empty and no downstream is asking - nothing to do until
        /// either input arrives or downstream demand appears.
    }

    /// Back-pressure on the soft cap, but only when there is no `canPush` empty port.
    /// When such a port exists, the deadlock with sequential consumers takes priority
    /// over the soft memory bound: we let queues overshoot to feed the asking path.
    /// Under pathological skew (all rows hashing to one shard while a sibling port is
    /// asking) this can buffer most of the input on the receiving shard; memory is then
    /// bounded by `max_memory_usage`, not by the soft cap (see comment in the header).
    /// Once input finishes, the first pass will finalize the empty ports.
    if (any_queue_at_capacity && !has_pushable_empty_port)
        return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;

    /// Try to pull a new input chunk.
    input.setNeeded();
    if (input.hasData())
    {
        pending_input_chunk = input.pull();
        has_pending_input_chunk = true;
        return Status::Ready;
    }

    /// No new input yet - drain what we can while waiting.
    if (has_pushable_queued_chunks)
        return Status::Ready;
    return Status::NeedData;
}

/// Split pending input chunk into per-shard queues, then drain queues to output ports.
void BufferedShardByHashTransform::work()
{
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
            queue.clear();
            continue;
        }

        if (queue.empty())
            continue;

        if (!output_it->canPush())
            continue;

        output_it->push(std::move(queue.front()));
        queue.pop_front();
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

        output_queues[shard].push_back(Chunk(std::move(shard_columns[shard]), shard_rows));
    }
}

}
