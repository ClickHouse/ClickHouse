#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

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

MergingAggregatedStep::MergingAggregatedStep(
    const DataStream & input_stream_,
    AggregatingTransformParamsPtr params_,
    bool memory_efficient_aggregation_,
    size_t max_threads_,
    size_t memory_efficient_merge_threads_)
    : ITransformingStep(input_stream_, params_->getHeader(), getTraits())
    , params(params_)
    , memory_efficient_aggregation(memory_efficient_aggregation_)
    , max_threads(max_threads_)
    , memory_efficient_merge_threads(memory_efficient_merge_threads_)
{
    /// Aggregation keys are distinct
    for (auto key : params->params.keys)
        output_stream->distinct_columns.insert(params->params.intermediate_header.getByPosition(key).name);
}

void MergingAggregatedStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    if (!memory_efficient_aggregation)
    {
        /// We union several sources into one, paralleling the work.
        pipeline.resize(1);

        /// Now merge the aggregated blocks
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<MergingAggregatedTransform>(header, params, max_threads);
        });
    }
    else
    {
        auto num_merge_threads = memory_efficient_merge_threads
                                 ? static_cast<size_t>(memory_efficient_merge_threads)
                                 : static_cast<size_t>(max_threads);

        pipeline.addMergingAggregatedMemoryEfficientTransform(params, num_merge_threads);
    }
}

void MergingAggregatedStep::describeActions(FormatSettings & settings) const
{
    return params->params.explain(settings.out, settings.offset);
}

void MergingAggregatedStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params->params.explain(map);
}

}
