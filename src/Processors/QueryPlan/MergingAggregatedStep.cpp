#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

namespace DB
{

static ITransformingStep::Traits getTraits(bool should_produce_results_in_order_of_bucket_number)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = should_produce_results_in_order_of_bucket_number,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

MergingAggregatedStep::MergingAggregatedStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_,
    bool should_produce_results_in_order_of_bucket_number_)
    : ITransformingStep(
        input_stream_, params_.getHeader(input_stream_.header, final_), getTraits(should_produce_results_in_order_of_bucket_number_))
    , params(std::move(params_))
    , final(final_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(max_threads_)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
{
    /// Aggregation keys are distinct
    for (const auto & key : params.keys)
        output_stream->distinct_columns.insert(key);
}

void MergingAggregatedStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform_params = std::make_shared<AggregatingTransformParams>(pipeline.getHeader(), std::move(params), final);
    if (!memory_efficient_aggregation)
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
                                    { return std::make_shared<MergingAggregatedTransform>(header, transform_params, max_threads); });
    }
    else
    {
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? static_cast<size_t>(memory_efficient_merge_threads)
                                 : static_cast<size_t>(max_threads);

        pipeline.addMergingAggregatedMemoryEfficientTransform(transform_params, num_merge_threads);
    }

    pipeline.resize(should_produce_results_in_order_of_bucket_number ? 1 : max_threads);
}

void MergingAggregatedStep::describeActions(FormatSettings & settings) const
{
    return params.explain(settings.out, settings.offset);
}

void MergingAggregatedStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void MergingAggregatedStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), params.getHeader(input_streams.front().header, final), getDataStreamTraits());

    /// Aggregation keys are distinct
    for (const auto & key : params.keys)
        output_stream->distinct_columns.insert(key);
}


}
