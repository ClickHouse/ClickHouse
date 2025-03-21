#include <Processors/QueryPlan/GatherReceiveStep.h>
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

void GatherReceiveStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    std::vector<std::unique_ptr<QueryPipelineBuilder>> pipelines;

    /// Read from all buckets
    for (size_t i = 0; i < num_buckets; ++i)
    {
        std::unique_ptr<QueryPipelineBuilder> pipeline_ptr = std::make_unique<QueryPipelineBuilder>();
        pipeline_ptr->init(Pipe(settings.exchange_lookup->createSource(output_header.value(), exchange_id, toString(i), "0")));
        pipelines.emplace_back(std::move(pipeline_ptr));
    }

    pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines));
}

void GatherReceiveStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
    writeVarUInt(num_buckets, ctx.out);
}

std::unique_ptr<IQueryPlanStep> GatherReceiveStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);

    size_t num_buckets;
    readVarUInt(num_buckets, ctx.in);

    return std::make_unique<GatherReceiveStep>(*ctx.output_header, exchange_id, num_buckets);
}

void registerGatherReceiveStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("GatherReceive", GatherReceiveStep::deserialize);
}

}
