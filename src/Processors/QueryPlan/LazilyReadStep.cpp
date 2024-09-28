#include <Processors/QueryPlan/LazilyReadStep.h>

#include <Common/JSONBuilder.h>
#include <Processors/Transforms/ColumnLazyTransform.h>
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

LazilyReadStep::LazilyReadStep(
    const DataStream & input_stream_,
    const LazilyReadInfoPtr & lazily_read_info_)
    : ITransformingStep(
    input_stream_,
    ColumnLazyTransform::transformHeader(input_stream_.header),
    getTraits())
    , lazily_read_info(lazily_read_info_)
{}

void LazilyReadStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ColumnLazyTransform>(header, lazily_read_info);
    });
}

void LazilyReadStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix << "Lazily read columns: ";

    bool first = true;
    for (const auto & column : lazily_read_info->lazily_read_columns)
    {
        if (!first)
            settings.out << ", ";
        first = false;

        settings.out << column.name;
    }

    settings.out << '\n';
}

void LazilyReadStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & column : lazily_read_info->lazily_read_columns)
        json_array->add(column.name);

    map.add("Lazily read columns", std::move(json_array));
}

void LazilyReadStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        ColumnLazyTransform::transformHeader(input_streams.front().header),
        getDataStreamTraits());
}

}
