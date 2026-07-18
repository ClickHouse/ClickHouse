#include <Processors/QueryPlan/DefaultTotalsColumnsStep.h>

#include <Processors/Transforms/DefaultTotalsColumnsTransform.h>
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

DefaultTotalsColumnsStep::DefaultTotalsColumnsStep(const SharedHeader & input_header_, std::vector<size_t> positions_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , positions(std::move(positions_))
{
}

void DefaultTotalsColumnsStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<DefaultTotalsColumnsTransform>(header, positions, on_totals);
    });
}

void DefaultTotalsColumnsStep::updateOutputHeader()
{
    /// The transform never changes the header (only column values on the totals row).
    output_header = input_headers.front();
}

QueryPlanStepPtr DefaultTotalsColumnsStep::clone() const
{
    return std::make_unique<DefaultTotalsColumnsStep>(*this);
}

}
