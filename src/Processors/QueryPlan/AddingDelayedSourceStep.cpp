#include <Processors/QueryPlan/AddingDelayedSourceStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
        .preserves_distinct_columns = false,
        .returns_single_stream = false,
        .preserves_number_of_streams = false,
        .preserves_number_of_rows = false, /// New rows are added from delayed stream
        .preserves_sorting = false,
    };
}

AddingDelayedSourceStep::AddingDelayedSourceStep(
    const DataStream & input_stream_,
    ProcessorPtr source_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , source(std::move(source_))
{
}

void AddingDelayedSourceStep::transformPipeline(QueryPipeline & pipeline)
{
    source->setQueryPlanStep(this);
    pipeline.addDelayedStream(source);
}

}
