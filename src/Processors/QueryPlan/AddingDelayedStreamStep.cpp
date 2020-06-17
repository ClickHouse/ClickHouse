#include <Processors/QueryPlan/AddingDelayedStreamStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

AddingDelayedStreamStep::AddingDelayedStreamStep(
    const DataStream & input_stream_,
    ProcessorPtr source_)
    : ITransformingStep(input_stream_, input_stream_)
    , source(std::move(source_))
{
}

void AddingDelayedStreamStep::transformPipeline(QueryPipeline & pipeline)
{
    pipeline.addDelayedStream(source);
}

}
