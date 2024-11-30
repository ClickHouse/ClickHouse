#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/LimitInRangeStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/LimitInRangeTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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

LimitInRangeStep::LimitInRangeStep(
    const Header & input_header_, String from_filter_column_name_, String to_filter_column_name_,
    UInt64 limit_inrange_window_, bool remove_filter_column_)
    : ITransformingStep(
        input_header_,
        LimitInRangeTransform::transformHeader(
            input_header_, from_filter_column_name_, to_filter_column_name_, limit_inrange_window_, remove_filter_column_),
        getTraits())
    , from_filter_column_name(std::move(from_filter_column_name_))
    , to_filter_column_name(std::move(to_filter_column_name_))
    , limit_inrange_window(limit_inrange_window_)
    , remove_filter_column(remove_filter_column_)
{
}

void LimitInRangeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.addSimpleTransform(
        [&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
        {
            bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
            return std::make_shared<LimitInRangeTransform>(
                header, from_filter_column_name, to_filter_column_name, limit_inrange_window, remove_filter_column, on_totals);
        });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), *output_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(),
            output_header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
    }
}

void LimitInRangeStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);
    settings.out << prefix << "From filter column: " << from_filter_column_name;
    settings.out << prefix << "To filter column: " << to_filter_column_name;
    settings.out << prefix << "Window size: " << limit_inrange_window;

    if (remove_filter_column)
        settings.out << " (removed)";
    settings.out << '\n';
}

void LimitInRangeStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("From filter Column", from_filter_column_name);
    map.add("To filter Column", to_filter_column_name);
    map.add("Window Size", limit_inrange_window);
    map.add("Removes Filter", remove_filter_column);
}

void LimitInRangeStep::updateOutputHeader()
{
    output_header = LimitInRangeTransform::transformHeader(input_headers.front(), from_filter_column_name, to_filter_column_name, limit_inrange_window, remove_filter_column);
}

}
