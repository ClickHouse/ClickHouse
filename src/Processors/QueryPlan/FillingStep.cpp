#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/Transforms/FillingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
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
    const Header & input_header_,
    SortDescription sort_description_,
    SortDescription fill_description_,
    InterpolateDescriptionPtr interpolate_description_,
    bool use_with_fill_by_sorting_prefix_)
    : ITransformingStep(input_header_, FillingTransform::transformHeader(input_header_, sort_description_), getTraits())
    , sort_description(std::move(sort_description_))
    , fill_description(std::move(fill_description_))
    , interpolate_description(interpolate_description_)
    , use_with_fill_by_sorting_prefix(use_with_fill_by_sorting_prefix_)
{
}

void FillingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (pipeline.getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FillingStep expects single input");

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
    String prefix(settings.offset, settings.indent_char);
    settings.out << prefix;
    dumpSortDescription(sort_description, settings.out);
    settings.out << '\n';
    if (interpolate_description)
    {
        auto expression = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());
        expression->describeActions(settings.out, prefix);
    }
}

void FillingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description));
    if (interpolate_description)
    {
        auto expression = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());
        map.add("Expression", expression->toTree());
    }
}

void FillingStep::updateOutputHeader()
{
    output_header = FillingTransform::transformHeader(input_headers.front(), sort_description);
}
}
