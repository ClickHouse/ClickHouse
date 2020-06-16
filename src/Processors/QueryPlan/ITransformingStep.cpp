#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ITransformingStep::ITransformingStep(DataStream input_stream, DataStream output_stream_)
{
    input_streams.emplace_back(std::move(input_stream));
    output_stream = std::move(output_stream_);
}

QueryPipelinePtr ITransformingStep::updatePipeline(QueryPipelines pipelines)
{
    transformPipeline(*pipelines.front());
    return std::move(pipelines.front());
}

}
