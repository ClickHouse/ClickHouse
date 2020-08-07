#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true
    };
}


DistinctStep::DistinctStep(
    const DataStream & input_stream_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
{
    auto & distinct_columns = pre_distinct ? output_stream->local_distinct_columns
                                           : output_stream->distinct_columns;

    /// Add more distinct columns.
    for (const auto & name : columns)
        distinct_columns.insert(name);
}

static bool checkColumnsAlreadyDistinct(const Names & columns, const NameSet & distinct_names)
{
    bool columns_already_distinct = true;
    for (const auto & name : columns)
        if (distinct_names.count(name) == 0)
            columns_already_distinct = false;

    return columns_already_distinct;
}

void DistinctStep::transformPipeline(QueryPipeline & pipeline)
{
    if (checkColumnsAlreadyDistinct(columns, input_streams.front().distinct_columns))
        return;

    if ((pre_distinct || pipeline.getNumStreams() <= 1)
        && checkColumnsAlreadyDistinct(columns, input_streams.front().local_distinct_columns))
            return;

    if (!pre_distinct)
        pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;

        return std::make_shared<DistinctTransform>(header, set_size_limits, limit_hint, columns);
    });
}

}
