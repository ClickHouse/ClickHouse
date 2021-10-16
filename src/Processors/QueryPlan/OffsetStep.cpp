#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/OffsetTransform.h>
#include <Processors/QueryPipeline.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

OffsetStep::OffsetStep(const DataStream & input_stream_, size_t offset_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , offset(offset_)
{
}

void OffsetStep::transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<OffsetTransform>(
            pipeline.getHeader(), offset, pipeline.getNumStreams());

    pipeline.addTransform(std::move(transform));
}

void OffsetStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ') << "Offset " << offset << '\n';
}

void OffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Offset", offset);
}

}
