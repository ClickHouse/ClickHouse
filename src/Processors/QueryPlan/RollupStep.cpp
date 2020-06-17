#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/Transforms/RollupTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

RollupStep::RollupStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_)
    : ITransformingStep(input_stream_, DataStream{.header = params_->getHeader()})
    , params(std::move(params_))
{
}

void RollupStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipeline::StreamType::Totals)
            return nullptr;

        return std::make_shared<RollupTransform>(header, std::move(params));
    });
}

}
