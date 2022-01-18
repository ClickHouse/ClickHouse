#include <Processors/QueryPlan/AddingDelayedSourceStep.h>
#include <Processors/QueryPipeline.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false, /// New rows are added from delayed stream
        }
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

    /// Now, after adding delayed stream, it has implicit dependency on other port.
    /// Here we add resize processor to remove this dependency.
    /// Otherwise, if we add MergeSorting + MergingSorted transform to pipeline, we could get `Pipeline stuck`
    pipeline.resize(pipeline.getNumStreams(), true);
}

}
