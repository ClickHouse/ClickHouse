#include <Processors/QueryPlan/ShuffleReceiveStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

/// TODO: include it
String fileNameForShuffleExchange(const String & exchange_id, size_t bucket);


void ShuffleReceiveStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const size_t bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<UInt64>();

    std::vector<std::unique_ptr<QueryPipelineBuilder>> pipelines;

    /// Read all shards
    for (const String & shard_id : list_of_shard_ids)
    {
        const auto shard_file_name = fileNameForShuffleExchange(exchange_id + "_shard_" + shard_id, bucket_id);
        std::unique_ptr<QueryPipelineBuilder> pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
        pipeline_ptr->init(Pipe(std::make_shared<NativeCompressedSource>(output_header.value(), settings.temporary_file_lookup->getTemporaryFileForReading(shard_file_name))));
        pipelines.emplace_back(std::move(pipeline_ptr));
    }

    pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), 0, &processors);
}

void ShuffleReceiveStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    writeVarUInt(list_of_shard_ids.size(), ctx.out);
    for (const String & shard_id : list_of_shard_ids)
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
