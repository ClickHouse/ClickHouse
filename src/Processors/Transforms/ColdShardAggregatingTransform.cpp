#include <Processors/Transforms/ColdShardAggregatingTransform.h>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace DB
{

ColdShardAggregatingTransform::ColdShardAggregatingTransform(
    SharedHeader raw_header,
    SharedHeader intermediate_header,
    SharedHeader output_header,
    AggregatingTransformParamsPtr params_,
    bool final_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor(InputPorts{std::move(raw_header), std::move(intermediate_header)}, OutputPorts{std::move(output_header)})
    , params(std::move(params_))
    , final(final_)
    , updater(std::move(updater_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
{
}

/// Mirror of the file-static helper in `AggregatingTransform.cpp`: wrap a converted aggregate chunk and
/// attach its `AggregatedChunkInfo`.
Chunk ColdShardAggregatingTransform::convertAggregatedChunk(Aggregator::AggregatedChunk && agg_chunk)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = agg_chunk.bucket_num;
    info->is_overflows = agg_chunk.is_overflows;

    agg_chunk.chunk.getChunkInfos().add(std::move(info));
    return std::move(agg_chunk.chunk);
}

IProcessor::Status ColdShardAggregatingTransform::prepare()
{
    auto & output = outputs.front();
    auto & raw = inputs.front();
    auto & states = inputs.back();

    if (output.isFinished())
    {
        raw.close();
        states.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        raw.setNotNeeded();
        states.setNotNeeded();
        return Status::PortFull;
    }

    /// Generating phase: both inputs were drained, the hashtable was converted in `work()`.
    if (converted)
    {
        raw.close();
        states.close();

        if (!output_chunks.empty())
        {
            output.push(convertAggregatedChunk(std::move(output_chunks.front())));
            output_chunks.pop_front();
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

    /// A chunk is waiting to be folded into the hashtable.
    if (has_pending)
        return Status::Ready;

    /// Both inputs exhausted (or we hit a row limit): convert in `work()`.
    if (consume_finished_early || (raw.isFinished() && states.isFinished()))
        return Status::Ready;

    /// Pull from whichever input has data. Order does not matter: `executeOnBlock` and `mergeOnBlock` merge
    /// into the same `variants` commutatively. We try the raw input first because it streams throughout,
    /// while the hot states only arrive once the hot aggregators reach EOF.
    if (!raw.isFinished())
    {
        raw.setNeeded();
        if (raw.hasData())
        {
            pending_chunk = raw.pull();
            pending_is_merge = false;
            has_pending = true;
            return Status::Ready;
        }
    }

    if (!states.isFinished())
    {
        states.setNeeded();
        if (states.hasData())
        {
            pending_chunk = states.pull();
            pending_is_merge = true;
            has_pending = true;
            return Status::Ready;
        }
    }

    return Status::NeedData;
}

void ColdShardAggregatingTransform::work()
{
    if (has_pending)
    {
        const size_t num_rows = pending_chunk.getNumRows();

        if (pending_is_merge)
        {
            /// Hot-key intermediate states: materialize (constant/sparse columns are not supported as
            /// merge input) and fold into the hashtable. A key already present as residue is combined.
            materializeChunk(pending_chunk);
            if (!params->aggregator.mergeOnBlock(pending_chunk.detachColumns(), num_rows, false, variants, no_more_keys, is_cancelled))
                consume_finished_early = true;
        }
        else
        {
            if (!params->aggregator.executeOnBlock(
                    pending_chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys))
                consume_finished_early = true;
        }

        has_pending = false;
        return;
    }

    /// Both inputs drained: finalize the shard in one shot. Sharded aggregation keeps the hashtable
    /// single-level and in memory, so a direct conversion is enough.
    output_chunks = params->aggregator.convertToChunks(variants, final);

    /// We bypass `ConvertingAggregatedToChunksTransform`, which is what feeds the dataflow-statistics cache;
    /// mark the case unsupported rather than recording partial sizes.
    if (updater)
        updater->markUnsupportedCase();

    converted = true;
}

}
