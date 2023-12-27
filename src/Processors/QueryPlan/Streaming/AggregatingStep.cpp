#include <Processors/QueryPlan/Streaming/AggregatingStep.h>

#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <memory>

namespace DB
{
namespace Streaming
{
namespace
{
}
ITransformingStep::Traits getTraits()
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

inline void convertToNullable(Block & header, const Names & keys)
{
    for (const auto & key : keys)
    {
        auto & column = header.getByName(key);

        column.type = makeNullableSafe(column.type);
        column.column = makeNullableSafe(column.column);
    }
}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_)
    : ITransformingStep(input_stream_, AggregatingTransformParams::getHeader(params_, final_), getTraits(), false)
    , params(std::move(params_))
    , final(std::move(final_))
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor>
        {
            return std::make_shared<GlobalAggregatingTransform>(
                header, transform_params, many_data, counter++, merge_threads);
        });

        pipeline.resize(1);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor>
        {
            return std::make_shared<GlobalAggregatingTransform>(header, transform_params);
        });
    }

    aggregating = collector.detachProcessors(0);
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

void AggregatingStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        AggregatingTransformParams::getHeader(params, final),
        getDataStreamTraits());
}

}
}
