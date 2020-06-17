#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

ExtremesStep::ExtremesStep(const DataStream & input_stream_) : ITransformingStep(input_stream_, input_stream_) {}


void ExtremesStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addExtremesTransform();
}

}
