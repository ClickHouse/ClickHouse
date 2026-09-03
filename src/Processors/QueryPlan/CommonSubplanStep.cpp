#include <Processors/QueryPlan/CommonSubplanStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;

}

namespace
{

constexpr ITransformingStep::Traits getTraits()
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

}

CommonSubplanStep::CommonSubplanStep(const SharedHeader & header_)
    : ITransformingStep(header_, header_, getTraits())
{}

void CommonSubplanStep::transformPipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Subplan cannot be used to build pipeline");
}

}
