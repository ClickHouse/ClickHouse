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
#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits(const ActionsDAG & actions)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = !actions.hasArrayJoin(),
        }
    };
}

ExpressionStep::ExpressionStep(const Header & input_header_, ActionsDAG actions_dag_)
    : ITransformingStep(
        input_header_,
        ExpressionTransform::transformHeader(input_header_, actions_dag_),
        getTraits(actions_dag_))
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

    if (!blocksHaveEqualStructure(pipeline.getHeader(), *output_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_header->getColumnsWithTypeAndName(),
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

void ExpressionStep::updateOutputHeader()
{
    output_header = ExpressionTransform::transformHeader(input_headers.front(), actions_dag);
}

void ExpressionStep::serialize(Serialization & ctx) const
{
    actions_dag.serialize(ctx.out, ctx.registry);
}

std::unique_ptr<IQueryPlanStep> ExpressionStep::deserialize(Deserialization & ctx)
{
    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ExpressionStep must have one input stream");

    return std::make_unique<ExpressionStep>(ctx.input_headers.front(), std::move(actions_dag));
}

IQueryPlanStep::UnusedColumnRemovalResult ExpressionStep::removeUnusedColumns(const Names & required_outputs, bool remove_inputs)
{
    const auto split_results = actions_dag.splitPossibleOutputNames(required_outputs);
    const auto actions_dag_input_count_before = actions_dag.getInputs().size();
    const auto removed_any_actions = actions_dag.removeUnusedActions(split_results.output_names, remove_inputs);

    if (!removed_any_actions && output_header.has_value() && output_header->columns() == required_outputs.size())
        return UnusedColumnRemovalResult{false, false};

    auto & input_header = input_headers.front();
    // Number of columns that are not changed/removed by actions
    const auto pass_through_inputs = input_header.columns() - actions_dag_input_count_before;
    const auto update_inputs = remove_inputs
        && (actions_dag.getInputs().size() < actions_dag_input_count_before || pass_through_inputs > split_results.not_output_names.size());

    if (update_inputs)
    {
        std::unordered_set<String> required_inputs_set;

        for (const auto * input_node : actions_dag.getInputs())
            required_inputs_set.insert(input_node->result_name);

        for (const auto & pass_through_input : split_results.not_output_names)
            required_inputs_set.insert(pass_through_input);

        Header new_input_header{};

        for (const auto & col_type_and_name : input_header)
            if (required_inputs_set.contains(col_type_and_name.name))
                new_input_header.insert(col_type_and_name);

        updateInputHeader(std::move(new_input_header), 0);

        return UnusedColumnRemovalResult{true, true};
    }

    updateOutputHeader();

    return UnusedColumnRemovalResult{true, false};
}

bool ExpressionStep::canRemoveColumnsFromOutput() const
{
    return output_header.has_value() ? output_header->columns() > 0 : false;
}

void registerExpressionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Expression", ExpressionStep::deserialize);
}

}
