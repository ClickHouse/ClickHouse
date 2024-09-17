#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Interpreters/JoinSwitcher.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits(const ActionsDAG & actions, const Block & header, const SortDescription & sort_description)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = actions.isSortingPreserved(header, sort_description),
        },
        {
            .preserves_number_of_rows = !actions.hasArrayJoin(),
        }
    };
}

ExpressionStep::ExpressionStep(const DataStream & input_stream_, ActionsDAG actions_dag_)
    : ITransformingStep(
        input_stream_,
        ExpressionTransform::transformHeader(input_stream_.header, actions_dag_),
        getTraits(actions_dag_, input_stream_.header, input_stream_.sort_description))
    , actions_dag(std::move(actions_dag_))
{
}

void ExpressionStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag), settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_stream->header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, convert_actions);
        });
    }
}

void ExpressionStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);
    auto expression = std::make_shared<ExpressionActions>(actions_dag.clone());
    expression->describeActions(settings.out, prefix);
}

void ExpressionStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag.clone());
    map.add("Expression", expression->toTree());
}

void ExpressionStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), ExpressionTransform::transformHeader(input_streams.front().header, actions_dag), getDataStreamTraits());

    if (!getDataStreamTraits().preserves_sorting)
        return;

    FindAliasForInputName alias_finder(actions_dag);
    const auto & input_sort_description = getInputStreams().front().sort_description;
    for (size_t i = 0, s = input_sort_description.size(); i < s; ++i)
    {
        const auto & original_column = input_sort_description[i].column_name;
        const auto * alias_node = alias_finder.find(original_column);
        if (alias_node)
            output_stream->sort_description[i].column_name = alias_node->result_name;
    }
}

void ExpressionStep::serialize(Serialization & ctx) const
{
    actions_dag.serialize(ctx.out, ctx.registry);
}

std::unique_ptr<IQueryPlanStep> ExpressionStep::deserialize(Deserialization & ctx)
{
    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry);
    if (ctx.input_streams.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ExpressionStep must have one input stream");

    return std::make_unique<ExpressionStep>(ctx.input_streams.front(), std::move(actions_dag));
}

void registerExpressionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Expression", ExpressionStep::deserialize);
}

}
