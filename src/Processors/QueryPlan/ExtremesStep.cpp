#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits{
            .preserves_distinct_columns = true
    };
}

ExtremesStep::ExtremesStep(const DataStream & input_stream_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
{
}

void ExtremesStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addExtremesTransform();
}

}
