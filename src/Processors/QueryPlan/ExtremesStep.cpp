#include <Processors/QueryPlan/ExtremesStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

ExtremesStep::ExtremesStep(const Header & input_header)
    : ITransformingStep(input_header, input_header, getTraits())
{
}

void ExtremesStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addExtremesTransform();
}

}
