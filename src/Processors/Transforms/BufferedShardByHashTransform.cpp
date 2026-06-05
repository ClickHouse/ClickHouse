#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Processors/Transforms/BufferedShardByHashTransform.h>
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

    /// Free queues for outputs closed by downstream
    bool all_finished = true;
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            output_queues[shard].clear();
        else
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

    /// Scan queues to decide what to do next.
    bool has_queued_chunks = false; /// any shard has chunks waiting in its queue
    bool has_pushable_queued_chunks = false; /// at least one queued chunk can be pushed right now (port is ready)
    bool any_queue_at_capacity = false; /// at least one shard's queue hit the back-pressure cap

    auto queued_output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++queued_output_it)
    {
        const auto & queue = output_queues[shard];
        if (queue.size() >= MAX_QUEUE_LENGTH)
            any_queue_at_capacity = true;
        if (!queue.empty())
        {
            has_queued_chunks = true;
            if (!queued_output_it->isFinished() && queued_output_it->canPush())
                has_pushable_queued_chunks = true;
        }
    }

    /// Input exhausted - drain remaining queues, then finish.
    if (input.isFinished())
    {
        if (has_queued_chunks)
            return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;

        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    /// Cannot push any output port
    if (has_queued_chunks && !has_pushable_queued_chunks)
        return Status::PortFull;

    if (any_queue_at_capacity)
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
    /// Fixed-width columns use fmix32 (SIMD-vectorised); ColumnString uses CRC32C.
    hash_buffer.resize(num_rows);
    bool initial = true;
    for (auto column_number : key_columns)
    {
        columns[column_number]->computeHashInto(0, num_rows, hash_buffer.data(), initial);
        initial = false;
    }

    /// Partition rows by shard using Lemire fastrange.
    /// The hash has full 32-bit avalanche so the high bits fed into the shift are
    /// well-distributed — no intHashCRC32 finalizer is needed.
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
