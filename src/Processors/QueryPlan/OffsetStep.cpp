#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/OffsetTransform.h>
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

OffsetStep::OffsetStep(const DataStream & input_stream_, size_t offset_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , offset(offset_)
{
}

void OffsetStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;

        return std::make_shared<OffsetTransform>(header, offset, 1);
    });
}

Strings OffsetStep::describeActions() const
{
    return {"Offset " + std::to_string(offset)};
}

}
