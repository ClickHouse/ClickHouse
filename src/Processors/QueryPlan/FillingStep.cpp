#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/Transforms/FillingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FillingStep::FillingStep(
    const DataStream & input_stream_,
    SortDescription sort_description_,
    SortDescription fill_description_,
    InterpolateDescriptionPtr interpolate_description_,
    bool use_with_fill_by_sorting_prefix_)
    : ITransformingStep(input_stream_, FillingTransform::transformHeader(input_stream_.header, sort_description_), getTraits())
    , sort_description(std::move(sort_description_))
    , fill_description(std::move(fill_description_))
    , interpolate_description(interpolate_description_)
    , use_with_fill_by_sorting_prefix(use_with_fill_by_sorting_prefix_)
{
    if (!input_stream_.has_single_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FillingStep expects single input");
}

void FillingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return std::make_shared<FillingNoopTransform>(header, fill_description);

        return std::make_shared<FillingTransform>(
            header, sort_description, fill_description, std::move(interpolate_description), use_with_fill_by_sorting_prefix);
    });
}

void FillingStep::describeActions(FormatSettings & settings) const
{
    settings.out << String(settings.offset, ' ');
    dumpSortDescription(sort_description, settings.out);
    settings.out << '\n';
}

void FillingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description));
}

void FillingStep::updateOutputStream()
{
    if (!input_streams.front().has_single_port)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FillingStep expects single input");

    output_stream = createOutputStream(
        input_streams.front(), FillingTransform::transformHeader(input_streams.front().header, sort_description), getDataStreamTraits());
}
}
