#include <Processors/QueryPlan/FinishSortingStep.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/FinishSortingTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits(size_t limit)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

FinishSortingStep::FinishSortingStep(
    const DataStream & input_stream_,
    SortDescription prefix_description_,
    SortDescription result_description_,
    size_t max_block_size_,
    UInt64 limit_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits(limit_))
    , prefix_description(std::move(prefix_description_))
    , result_description(std::move(result_description_))
    , max_block_size(max_block_size_)
    , limit(limit_)
{
    /// TODO: check input_stream is sorted by prefix_description.
    output_stream->sort_description = result_description;
    output_stream->sort_mode = DataStream::SortMode::Stream;
}

void FinishSortingStep::updateLimit(size_t limit_)
{
    if (limit_ && (limit == 0 || limit_ < limit))
    {
        limit = limit_;
        transform_traits.preserves_number_of_rows = false;
    }
}

void FinishSortingStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    bool need_finish_sorting = (prefix_description.size() < result_description.size());
    if (pipeline.getNumStreams() > 1)
    {
        UInt64 limit_for_merging = (need_finish_sorting ? 0 : limit);
        auto transform = std::make_shared<MergingSortedTransform>(
                pipeline.getHeader(),
                pipeline.getNumStreams(),
                prefix_description,
                max_block_size, limit_for_merging);

        pipeline.addTransform(std::move(transform));
    }

    if (need_finish_sorting)
    {
        pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipeline::StreamType::Main)
                return nullptr;

            return std::make_shared<PartialSortingTransform>(header, result_description, limit);
        });

        /// NOTE limits are not applied to the size of temporary sets in FinishSortingTransform
        pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr
        {
            return std::make_shared<FinishSortingTransform>(
                header, prefix_description, result_description, max_block_size, limit);
        });
    }
}

void FinishSortingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix << "Prefix sort description: ";
    dumpSortDescription(prefix_description, input_streams.front().header, settings.out);
    settings.out << '\n';

    settings.out << prefix << "Result sort description: ";
    dumpSortDescription(result_description, input_streams.front().header, settings.out);
    settings.out << '\n';

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void FinishSortingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Prefix Sort Description", explainSortDescription(prefix_description, input_streams.front().header));
    map.add("Result Sort Description", explainSortDescription(result_description, input_streams.front().header));

    if (limit)
        map.add("Limit", limit);
}

}
