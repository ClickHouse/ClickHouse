#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/LimitRangeTransform.h>
#include <Interpreters/ExpressionActions.h>
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

LimitRangeStep::LimitRangeStep(
    const SharedHeader & input_header_,
    std::optional<std::pair<ActionsDAG, String>> start_condition_,
    std::optional<std::pair<ActionsDAG, String>> end_condition_,
    size_t limit_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , start_condition(std::move(start_condition_))
    , end_condition(std::move(end_condition_))
    , limit(limit_)
{
}

void LimitRangeStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    ExpressionActionsPtr start_expression;
    String start_column_name;
    if (start_condition)
    {
        start_expression = std::make_shared<ExpressionActions>(
            start_condition->first.clone(), settings.getActionsSettings());
        start_column_name = start_condition->second;
    }

    ExpressionActionsPtr end_expression;
    String end_column_name;
    if (end_condition)
    {
        end_expression = std::make_shared<ExpressionActions>(
            end_condition->first.clone(), settings.getActionsSettings());
        end_column_name = end_condition->second;
    }

    pipeline.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<LimitRangeTransform>(
            header,
            start_expression,
            start_column_name,
            end_expression,
            end_column_name,
            limit);
    });
}

void LimitRangeStep::describeActions(FormatSettings & format_settings) const
{
    String prefix(format_settings.offset, ' ');
    format_settings.out << prefix << "LimitRange limit " << limit;
    if (start_condition)
        format_settings.out << " AFTER";
    if (end_condition)
        format_settings.out << " UNTIL";
    format_settings.out << '\n';
}

void LimitRangeStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Limit", limit);
    map.add("Has After", start_condition.has_value());
    map.add("Has Until", end_condition.has_value());
}

}
