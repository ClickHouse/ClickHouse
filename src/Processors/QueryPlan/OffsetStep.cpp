#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/OffsetTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

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
            .preserves_number_of_rows = false,
        }
    };
}

OffsetStep::OffsetStep(const DataStream & input_stream_, size_t offset_, bool is_negative_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , offset(offset_), is_negative(is_negative_)
{
}

void OffsetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto transform = std::make_shared<OffsetTransform>(
            pipeline.getHeader(), offset, pipeline.getNumStreams(), is_negative);

    pipeline.addTransform(std::move(transform));
}

void OffsetStep::describeActions(FormatSettings & settings) const
{
    if (is_negative)
        settings.out << String(settings.offset, ' ') << "Offset " << "-" << offset << '\n';
    else
        settings.out << String(settings.offset, ' ') << "Offset " << offset << '\n';
}

void OffsetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Offset", offset);
}

}
