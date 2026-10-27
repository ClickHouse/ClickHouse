#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/receiveExchangeStreams.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

void ShuffleReceiveStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const String bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();

    VectorWithMemoryTracking<ExchangeStreamId> stream_ids;
    for (const String & shard_id : source_shards)
        stream_ids.emplace_back(exchange_id, shard_id, bucket_id);

    /// The order of the chunks does not matter after a shuffle, so the receive runs on all threads.
    pipeline = receiveExchangeStreams(output_header, exchange_id, stream_ids, settings, /*spread_over_max_threads=*/ true);
    processors = pipeline.getProcessors();
}

void ShuffleReceiveStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    writeVarUInt(source_shards.size(), ctx.out);
    for (const String & shard_id : source_shards)
        writeStringBinary(shard_id, ctx.out);
}

std::unique_ptr<IQueryPlanStep> ShuffleReceiveStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);
    size_t shard_id_count = 0;
    readVarUInt(shard_id_count, ctx.in);
    Strings list_of_shard_ids;
    list_of_shard_ids.reserve(shard_id_count);
    for (size_t i = 0; i < shard_id_count; ++i)
    {
        String shard_id;
        readStringBinary(shard_id, ctx.in);
        list_of_shard_ids.push_back(std::move(shard_id));
    }
    return std::make_unique<ShuffleReceiveStep>(ctx.output_header, exchange_id, list_of_shard_ids);
}

void registerShuffleReceiveStep(QueryPlanStepRegistry & registry);
void registerShuffleReceiveStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ShuffleReceive", ShuffleReceiveStep::deserialize);
}

}
