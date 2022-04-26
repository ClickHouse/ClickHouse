#include <Processors/QueryPlan/ResizeStreamsStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
namespace DB
{
static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ResizeStreamsStep::ResizeStreamsStep(const DataStream & input_stream_, size_t pipeline_streams_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , pipeline_streams(pipeline_streams_)
{
}

void ResizeStreamsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(pipeline_streams);
}
}
