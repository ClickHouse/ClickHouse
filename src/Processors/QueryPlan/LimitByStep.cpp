#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true,
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
    };
}

LimitByStep::LimitByStep(
    const DataStream & input_stream_,
    size_t group_length_, size_t group_offset_, const Names & columns_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , group_length(group_length_)
    , group_offset(group_offset_)
    , columns(columns_)
{
}


void LimitByStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.resize(1);

    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;

        return std::make_shared<LimitByTransform>(header, group_length, group_offset, columns);
    });
}

}
