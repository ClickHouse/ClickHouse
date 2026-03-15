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
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            output_queues[shard].clear();
    }

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

    if (has_pending_input_chunk)
        return Status::Ready;

    /// Any shard has chunks waiting in its queue
    bool has_queued_chunks = false;

    /// At least one queued chunk can be pushed right now (port is ready)
    bool has_pushable_queued_chunks = false;

    /// At least one shard's queue reached max_queued_chunks_per_shard
    bool has_full_queued_shard = false;

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

    if (input.isFinished())
    {
        if (has_queued_chunks)
            return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;

        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    /// If all pending shard queues are blocked, do not keep pulling input.
    if (has_queued_chunks && !has_pushable_queued_chunks)
        return Status::PortFull;

    /// Keep at most max_queued_chunks_per_shard buffered chunks per live shard.
    if (has_full_queued_shard)
        return has_pushable_queued_chunks ? Status::Ready : Status::PortFull;

    input.setNeeded();
    if (input.hasData())
    {
        /// Pull one input chunk and split it into per-shard chunks in work().
        pending_input_chunk = input.pull();
        has_pending_input_chunk = true;
        return Status::Ready;
    }

    if (has_pushable_queued_chunks)
        return Status::Ready;
    return Status::NeedData;
}

void ScatterByHashTransform::work()
{
    if (has_pending_input_chunk)
    {
        /// Split the newly pulled input chunk and append per-shard chunks to output queues.
        generateOutputChunks();
        has_pending_input_chunk = false;
    }

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

    auto [payload_columns, key_hashes] = params->aggregator.prepareColumnsForSharding(columns, num_rows, cached_hash_variants);
    chassert(payload_columns.size() == outputs.front().getHeader().columns());
    chassert(key_hashes->size() == num_rows);
    chassert(std::all_of(payload_columns.begin(), payload_columns.end(), [num_rows](const auto & col) { return col->size() == num_rows; }));

    /// Build per-shard row indices.
    std::vector<IColumn::Selector> per_shard_indices(num_shards);
    for (size_t shard = 0; shard < num_shards; ++shard)
        /// Assume uniform distribution of keys
        per_shard_indices[shard].reserve(num_rows / num_shards);

    for (size_t row = 0; row < num_rows; ++row)
    {
        size_t shard = (*key_hashes)[row] % num_shards;
        per_shard_indices[shard].push_back(row);
    }

    /// Emit one chunk per shard. Every chunk shares the same full-size payload columns
    /// (via ColumnPtr copies) and carries a ShardedChunkInfo with shared hashes
    /// plus that shard's row indices.
    auto output_it = outputs.begin();
    for (size_t shard = 0; shard < num_shards; ++shard, ++output_it)
    {
        if (output_it->isFinished())
            continue;

        if (per_shard_indices[shard].empty())
            continue;

        Chunk output_chunk(payload_columns, num_rows);
        output_chunk.getChunkInfos().add(std::make_shared<ShardedChunkInfo>(key_hashes, std::move(per_shard_indices[shard])));
        output_queues[shard].push_back(std::move(output_chunk));
    }
}

}
