#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

void ShuffleReceiveStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const String bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();

    std::vector<std::unique_ptr<QueryPipelineBuilder>> pipelines;

    /// Read all shards
    for (const String & shard_id : source_shards)
    {
        std::unique_ptr<QueryPipelineBuilder> pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
        pipeline_ptr->init(Pipe(settings.exchange_lookup->createSource(output_header.value(), ExchangeStreamId(exchange_id, shard_id, bucket_id))));
        pipelines.emplace_back(std::move(pipeline_ptr));
    }

    pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), 0, &processors);
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
    size_t shard_id_count;
    readVarUInt(shard_id_count, ctx.in);
    Strings list_of_shard_ids;
    list_of_shard_ids.reserve(shard_id_count);
    for (size_t i = 0; i < shard_id_count; ++i)
    {
        String shard_id;
        readStringBinary(shard_id, ctx.in);
        list_of_shard_ids.push_back(std::move(shard_id));
    }
    return std::make_unique<ShuffleReceiveStep>(*ctx.output_header, exchange_id, list_of_shard_ids);
}

void registerShuffleReceiveStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ShuffleReceive", ShuffleReceiveStep::deserialize);
}

}
