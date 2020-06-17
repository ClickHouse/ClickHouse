#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/Transforms/CubeTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

CubeStep::CubeStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_)
    : ITransformingStep(input_stream_, DataStream{.header = params_->getHeader()})
    , params(std::move(params_))
{
}

void CubeStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        return std::make_shared<CubeTransform>(header, std::move(params));
    });
}

}
