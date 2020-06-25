#include <Processors/QueryPlan/AddingDelayedStreamStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
        .preserves_distinct_columns = false
    };
}

AddingDelayedStreamStep::AddingDelayedStreamStep(
    const DataStream & input_stream_,
    ProcessorPtr source_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , source(std::move(source_))
{
}

void AddingDelayedStreamStep::transformPipeline(QueryPipeline & pipeline)
{
    source->setQueryPlanStep(this);
    pipeline.addDelayedStream(source);
}

}
