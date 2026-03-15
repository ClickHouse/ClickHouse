#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Processors/Transforms/ScatterByHashTransform.h>

namespace DB
{

ScatterByHashTransform::ScatterByHashTransform(
    SharedHeader input_header, SharedHeader output_header, size_t num_shards_, AggregatingTransformParamsPtr params_)
    : IProcessor(InputPorts{std::move(input_header)}, OutputPorts{num_shards_, std::move(output_header)})
    , num_shards(num_shards_)
    , params(std::move(params_))
    , output_queues(num_shards)
{
    chassert(num_shards > 0);
    chassert(params->params.keys_size == 1);
}

IProcessor::Status ScatterByHashTransform::prepare()
{
    auto & input = getInputs().front();

    /// Free queues for outputs that have been closed by downstream.
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            output_queues[shard].clear();
    }

    /// All downstream consumers are done - nothing left to do.
    bool all_finished = true;
    for (auto & output : outputs)
    {
        if (!output.isFinished())
        {
            all_finished = false;
            break;
        }
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
    bool has_full_queued_shard = false; /// at least one shard's queue reached max_queued_chunks_per_shard

    auto queued_output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++queued_output_it)
    {
        const auto & queue = output_queues[shard];
        if (!queue.empty())
        {
            has_queued_chunks = true;
            if (!queued_output_it->isFinished() && queued_output_it->canPush())
                has_pushable_queued_chunks = true;
        }

        chassert(queue.size() <= max_queued_chunks_per_shard);
        if (!queued_output_it->isFinished() && queue.size() == max_queued_chunks_per_shard)
            has_full_queued_shard = true;
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

    /// At least one queue is full - drain before pulling more input.
    if (has_full_queued_shard)
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
void ScatterByHashTransform::work()
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

void ScatterByHashTransform::generateOutputChunks()
{
    const auto num_rows = pending_input_chunk.getNumRows();

    auto columns = pending_input_chunk.detachColumns();
    chassert(!columns.empty());

    /// Materialize key + argument columns and compute per-row hashes.
    auto [payload_columns, key_hashes] = params->aggregator.prepareColumnsForSharding(columns, num_rows, cached_hash_variants);
    chassert(payload_columns.size() == outputs.front().getHeader().columns());
    chassert(key_hashes->size() == num_rows);
    chassert(std::all_of(payload_columns.begin(), payload_columns.end(), [num_rows](const auto & col) { return col->size() == num_rows; }));

    /// Partition rows by hash(key) % num_shards.
    std::vector<IColumn::Selector> per_shard_indices(num_shards);
    for (size_t shard = 0; shard < num_shards; ++shard)
        /// Assume uniform distribution of keys.
        per_shard_indices[shard].reserve(num_rows / num_shards);

    for (size_t row = 0; row < num_rows; ++row)
    {
        size_t shard = (*key_hashes)[row] % num_shards;
        per_shard_indices[shard].push_back(row);
    }

    /// Emit one chunk per shard. Every chunk shares the same full-size payload columns
    /// (zero-copy via ColumnPtr) and carries a ShardedChunkInfo with shared hashes
    /// plus that shard's row indices.
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            continue;

        if (per_shard_indices[shard].empty())
            continue;

        /// num_rows is the full input chunk row count, not this shard's subset, because
        /// the payload columns are shared full-size. ShardedAggregatingTransform uses
        /// ShardedChunkInfo::row_indices for the actual row count.
        Chunk output_chunk(payload_columns, num_rows);
        output_chunk.getChunkInfos().add(std::make_shared<ShardedChunkInfo>(key_hashes, std::move(per_shard_indices[shard])));
        output_queues[shard].push_back(std::move(output_chunk));
    }
}

}
