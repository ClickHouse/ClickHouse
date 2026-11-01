#include <Processors/QueryPlan/ShuffleStep.h>

#include <IO/Operators.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/ShuffleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>


namespace DB
{

static ITransformingStep::Traits getTraits(UInt64 limit)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

ShuffleStep::ShuffleStep(
    const SharedHeader & input_header,
    String key_column_name_,
    int direction_,
    UInt64 limit_,
    size_t max_block_size_)
    : ITransformingStep(input_header, input_header, getTraits(limit_))
    , key_column_name(std::move(key_column_name_))
    , direction(direction_)
    , limit(limit_)
    , max_block_size(max_block_size_)
{
}

SortDescription ShuffleStep::getKeySortDescription() const
{
    SortDescription description;
    description.emplace_back(key_column_name, direction);
    return description;
}

void ShuffleStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const size_t key_position = pipeline.getHeader().getPositionByName(key_column_name);

    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return std::make_shared<PartialShuffleTransform>(header, key_position, direction, limit, max_block_size);
        });

    /// Merging of shuffled streams: every stream is sorted by the key column, and the keys are independent
    /// uniformly distributed random numbers, so the merge is a uniformly random interleaving of the streams.
    if (pipeline.getNumStreams() > 1)
    {
        auto transform = std::make_shared<MergingSortedTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            getKeySortDescription(),
            max_block_size,
            /*max_block_size_bytes=*/0,
            /*max_dynamic_subcolumns=*/std::nullopt,
            SortingQueueStrategy::Batch,
            limit);

        pipeline.addTransform(std::move(transform));
    }
}

void ShuffleStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Shuffle key: " << key_column_name << (direction < 0 ? " DESC" : " ASC") << '\n';
    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void ShuffleStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Shuffle Key", key_column_name);
    map.add("Direction", direction < 0 ? "DESC" : "ASC");
    if (limit)
        map.add("Limit", limit);
}

QueryPlanStepPtr ShuffleStep::clone() const
{
    return std::make_unique<ShuffleStep>(*this);
}

}
