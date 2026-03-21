#include <Processors/QueryPlan/ShuffleStep.h>
#include <Processors/Transforms/ShuffleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,          /// shuffle merges all streams into one
            .preserves_number_of_streams = false,    /// changes from N streams to 1
            .preserves_sorting = false,              /// destroys any existing sort order
        },
        {
            .preserves_number_of_rows = false,       /// SHUFFLE LIMIT k reduces rows
        }
    };
}

ShuffleStep::ShuffleStep(SharedHeader header, size_t limit_)
    : ITransformingStep(header, header, getTraits())
    , limit(limit_)
{
}

void ShuffleStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](SharedHeader header)
    {
        return std::make_shared<ShuffleTransform>(header, limit);
    });
}
}
