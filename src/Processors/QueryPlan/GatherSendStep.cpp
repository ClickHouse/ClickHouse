#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

QueryPipelineBuilderPtr GatherSendStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GatherSendStep expects single input step");

    auto & pipeline = *pipelines.front();
    Block stream_header = pipeline.getHeader();

    const String bucket = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();
    auto file_name = fileNameForExchange(exchange_id, bucket, "0");

    /// Cannot have multiple sinks writing to the same file concurrently.
    pipeline.resize(1);

    pipeline.setSinks([&](const Block & header, Pipe::StreamType stream_type)
    {
        chassert(stream_type == Pipe::StreamType::Main);
        return std::make_shared<NativeCompressedSink>(header, settings.temporary_file_lookup->getTemporaryFileForWriting(file_name));
    });

    return std::move(pipelines.front());
}

void GatherSendStep::serialize(Serialization & ctx) const
{
    writeStringBinary(exchange_id, ctx.out);
}

std::unique_ptr<IQueryPlanStep> GatherSendStep::deserialize(Deserialization & ctx)
{
    String exchange_id;
    readStringBinary(exchange_id, ctx.in);
    return std::make_unique<GatherSendStep>(ctx.input_headers.front(), exchange_id);
}

void registerGatherSendStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("GatherSend", GatherSendStep::deserialize);
}

}
