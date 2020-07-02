#include <Processors/QueryPlan/OffsetsStep.h>
#include <Processors/OffsetTransform.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
    };
}

OffsetsStep::OffsetsStep(const DataStream & input_stream_, size_t offset_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , offset(offset_)
{
}

void OffsetsStep::transformPipeline(QueryPipeline & pipeline)
{
    auto transform = std::make_shared<OffsetTransform>(
            pipeline.getHeader(), offset, pipeline.getNumStreams());

    pipeline.addPipe({std::move(transform)});
}

}
