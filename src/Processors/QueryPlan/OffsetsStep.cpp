#include <Processors/QueryPlan/OffsetsStep.h>
#include <Processors/OffsetTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

OffsetsStep::OffsetsStep(const DataStream & input_stream_, size_t offsets_)
    : ITransformingStep(input_stream_, input_stream_)
    , offset(offsets_)
{
}

void OffsetsStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipeline::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type != QueryPipeline::StreamType::Main)
            return nullptr;

        return std::make_shared<OffsetTransform>(header, offset, 1);
    });
}

}
