#include <Processors/QueryPlan/Streaming/WatermarkStep.h>

#include <Processors/Transforms/Streaming/WatermarkTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace Streaming
{
namespace
{
DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}
WatermarkStep::WatermarkStep(const DataStream & input_stream_, WatermarkStamperParamsPtr params_, Poco::Logger * log_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits()), params(std::move(params_)), log(log_)
{
}

void WatermarkStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<WatermarkTransform>(header, params, log);
    });
}

void WatermarkStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),  input_streams.front().header, getDataStreamTraits());
}
}
}
