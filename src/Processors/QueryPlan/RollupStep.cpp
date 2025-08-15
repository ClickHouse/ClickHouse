#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/Transforms/RollupTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/AggregatingStep.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

RollupStep::RollupStep(const DataStream & input_stream_, Aggregator::Params params_, bool final_, bool use_nulls_)
    : ITransformingStep(input_stream_, generateOutputHeader(params_.getHeader(input_stream_.header, final_), params_.keys, use_nulls_), getTraits())
    , params(std::move(params_))
    , keys_size(params.keys_size)
    , final(final_)
    , use_nulls(use_nulls_)
{
}

ProcessorPtr addGroupingSetForTotals(const Block & header, const Names & keys, bool use_nulls, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number);

void RollupStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return addGroupingSetForTotals(header, params.keys, use_nulls, settings, keys_size);

        auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), true);
        return std::make_shared<RollupTransform>(header, std::move(transform_params), use_nulls);
    });
}

void RollupStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        generateOutputHeader(params.getHeader(input_streams.front().header, final), params.keys, use_nulls),
        getDataStreamTraits());
}


}
