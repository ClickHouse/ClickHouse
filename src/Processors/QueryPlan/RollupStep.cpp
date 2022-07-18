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

RollupStep::RollupStep(const DataStream & input_stream_, Aggregator::Params params_, bool final_)
    : ITransformingStep(input_stream_, appendGroupingSetColumn(params_.getHeader(input_stream_.header, final_)), getTraits())
    , params(std::move(params_))
    , keys_size(params.keys_size)
    , final(final_)
{
    /// Aggregation keys are distinct
    for (const auto & key : params.keys)
        output_stream->distinct_columns.insert(key);
}

ProcessorPtr addGroupingSetForTotals(const Block & header, const BuildQueryPipelineSettings & settings, UInt64 grouping_set_number);

void RollupStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return addGroupingSetForTotals(header, settings, keys_size);

        auto transform_params = std::make_shared<AggregatingTransformParams>(header, std::move(params), true);
        return std::make_shared<RollupTransform>(header, std::move(transform_params));
    });
}

void RollupStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), appendGroupingSetColumn(params.getHeader(input_streams.front().header, final)), getDataStreamTraits());

    /// Aggregation keys are distinct
    for (const auto & key : params.keys)
        output_stream->distinct_columns.insert(key);
}


}
