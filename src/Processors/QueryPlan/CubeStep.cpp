#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/Transforms/CubeTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

CubeStep::CubeStep(const DataStream & input_stream_, AggregatingTransformParamsPtr params_)
    : ITransformingStep(input_stream_, params_->getHeader(), getTraits())
    , params(std::move(params_))
{
    /// Aggregation keys are distinct
    for (auto key : params->params.keys)
        output_stream->distinct_columns.insert(params->params.src_header.getByPosition(key).name);
}

void CubeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return nullptr;

        return std::make_shared<CubeTransform>(header, std::move(params));
    });
}

const Aggregator::Params & CubeStep::getParams() const
{
    return params->params;
}

}
