#include <Processors/Port.h>
#include <Processors/Transforms/ScatterByHashTransform.h>
#include <Processors/Transforms/ShardedAggregatingTransform.h>

namespace DB
{

ShardedAggregatingTransform::ShardedAggregatingTransform(SharedHeader header, AggregatingTransformParamsPtr params_)
    : IProcessor({std::move(header)}, {std::make_shared<const Block>(params_->getHeader())})
    , params(std::move(params_))
{
}

IProcessor::Status ShardedAggregatingTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Downstream closed — nothing to do.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    /// Output phase: push converted chunks one by one.
    if (is_generate_initialized)
    {
        if (!output.canPush())
            return Status::PortFull;

        if (output_chunk_idx < output_chunks.size())
        {
            output.push(std::move(output_chunks[output_chunk_idx++]));
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

    /// All input consumed -> output blocks from hash table in work().
    if (is_consume_finished)
    {
        input.close();
        return Status::Ready;
    }

    /// Consume phase: pull one chunk at a time for aggregation in work().
    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        is_consume_finished = true;
        return Status::Ready;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull();
    has_input = true;
    return Status::Ready;
}

void ShardedAggregatingTransform::work()
{
    if (is_consume_finished)
    {
        initGenerate();
        return;
    }

    consume(std::move(current_chunk));
    has_input = false;
}

/// Aggregate one chunk's shard rows into this shard's hash table.
void ShardedAggregatingTransform::consume(Chunk chunk)
{
    /// chunk.getNumRows() is the full input chunk size (all shards share the same columns).
    /// The actual rows for this shard are in shard_info->row_indices.
    auto shard_info = chunk.getChunkInfos().get<ShardedChunkInfo>();
    chassert(shard_info);
    chassert(!shard_info->row_indices.empty());
    chassert(shard_info->key_hashes && !shard_info->key_hashes->empty());

    const auto & aggregator = params->aggregator;
    auto payload_columns = chunk.detachColumns();

    /// We only build instructions on first chunk; Subsequent calls just update column pointers.
    aggregator.prepareInstructionsForSharding(payload_columns, aggregate_columns_holder, aggregate_instructions);

    /// First keys_size columns in the payload are the key columns.
    ColumnRawPtrs key_columns(aggregator.getParams().keys_size);
    for (size_t i = 0; i < key_columns.size(); ++i)
        key_columns[i] = payload_columns[i].get();

    /// Insert this shard's rows into the hash table using precomputed hashes.
    aggregator.executeOnSubsetRows(variants, shard_info->row_indices, shard_info->key_hashes->data(), key_columns, aggregate_instructions.data());
}

/// Convert the hash table into output chunks. Called once after all input is consumed.
void ShardedAggregatingTransform::initGenerate()
{
    is_generate_initialized = true;

    /// Sharded aggregation does not support spilling to disk.
    chassert(!params->aggregator.hasTemporaryData());

    if (variants.empty())
        return;

    auto agg_chunks = params->aggregator.convertToChunks(variants, params->final);

    for (auto & agg_chunk : agg_chunks)
    {
        if (agg_chunk.chunk.getNumRows() == 0)
            continue;

        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = agg_chunk.bucket_num;
        info->is_overflows = agg_chunk.is_overflows;

        agg_chunk.chunk.getChunkInfos().add(std::move(info));
        output_chunks.push_back(std::move(agg_chunk.chunk));
    }
}

}
