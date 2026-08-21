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
    extern const int LOGICAL_ERROR;
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

static bool containsCompiledFunction(const ActionsDAG::Node * node)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->is_function_compiled)
        return true;

    const auto & children = node->children;
    if (children.empty())
        return false;

    bool result = false;
    for (const auto & child : children)
        result |= containsCompiledFunction(child);
    return result;
}

static NameSet getColumnsContainCompiledFunction(const ActionsDAG & actions_dag)
{
    NameSet result;
    for (const auto * node : actions_dag.getOutputs())
    {
        if (containsCompiledFunction(node))
        {
            result.insert(node->result_name);
        }
    }
    return result;
}

ExpressionStep::ExpressionStep(SharedHeader input_header_, ActionsDAG actions_dag_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(ExpressionTransform::transformHeader(*input_header_, actions_dag_)),
        getTraits(actions_dag_))
    , actions_dag(std::move(actions_dag_))
{
}

void ExpressionStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag), settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const SharedHeader & header)
                                { return std::make_shared<ExpressionTransform>(header, expression, dataflow_cache_updater); });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), *output_header))
    {
        auto columns_contain_compiled_function = getColumnsContainCompiledFunction(expression->getActionsDAG());
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline.getHeader().getColumnsWithTypeAndName(),
            output_header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            nullptr, false, false, nullptr,
            &columns_contain_compiled_function);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const SharedHeader & header)
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
    output_header = std::make_shared<const Block>(ExpressionTransform::transformHeader(*input_headers.front(), actions_dag));
}

void ExpressionStep::serialize(Serialization & ctx) const
{
    actions_dag.serialize(ctx.out, ctx.registry);
}

QueryPlanStepPtr ExpressionStep::deserialize(Deserialization & ctx)
{
    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ExpressionStep must have one input stream");

    return std::make_unique<ExpressionStep>(ctx.input_headers.front(), std::move(actions_dag));
}

bool ExpressionStep::canRemoveUnusedColumns() const
{
    // At the time of writing ActionsDAG doesn't handle removal of unused actions well in case of duplicated names in input or outputs
    return !hasDuplicatedNamesInInputOrOutputs(actions_dag);
}

IQueryPlanStep::RemovedUnusedColumns ExpressionStep::removeUnusedColumns(NameMultiSet required_outputs, bool remove_inputs)
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    /// When extra columns were absorbed from a child step that cannot reduce its output,
    /// prevent input removal to avoid re-creating the mismatch on subsequent optimization passes.
    if (prevent_input_removal)
        remove_inputs = false;

    const auto required_output_count = required_outputs.size();
    auto split_results = actions_dag.splitPossibleOutputNames(std::move(required_outputs));
    const auto actions_dag_input_count_before = actions_dag.getInputs().size();

    const auto actions_dag_required_outputs = getRequiredOutputNamesInOrder(std::move(split_results.output_names), actions_dag);
    auto updated_actions = actions_dag.removeUnusedActions(actions_dag_required_outputs, remove_inputs);

    const auto & input_header = input_headers.front();
    // Number of input columns that are not removed by actions
    const auto pass_through_inputs = input_header->columns() - actions_dag_input_count_before;
    const auto has_to_remove_any_pass_through_input = pass_through_inputs > split_results.not_output_names.size();
    const auto has_to_add_input_to_actions = !remove_inputs && has_to_remove_any_pass_through_input;

    const auto build_required_inputs_set = [this, &not_output_names = split_results.not_output_names]()
    {
        std::unordered_set<String> required_inputs_set;

        for (const auto * input_node : actions_dag.getInputs())
            required_inputs_set.insert(input_node->result_name);

        for (const auto & pass_through_input : not_output_names)
            required_inputs_set.insert(pass_through_input);

        return required_inputs_set;
    };

    if (has_to_add_input_to_actions)
    {
        const auto required_inputs_set = build_required_inputs_set();

        for (const auto & name_and_type : *input_header)
            if (!required_inputs_set.contains(name_and_type.name))
                actions_dag.addInput(name_and_type.name, name_and_type.type);

        updated_actions = true;
    }

    // If the actions are not updated and no outputs has to be removed, then there is nothing to update
    // Note: required_outputs must be a subset of already existing outputs
    if (!updated_actions && output_header->columns() == required_output_count)
        return RemovedUnusedColumns::None;

    if (actions_dag.getInputs().size() > getInputHeaders().at(0)->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There cannot be more inputs in the DAG than columns in the input header");

    const auto actions_dag_has_less_inputs = actions_dag.getInputs().size() < actions_dag_input_count_before;
    const auto update_inputs = remove_inputs && (actions_dag_has_less_inputs || has_to_remove_any_pass_through_input);

    if (update_inputs)
    {
        const auto required_inputs_set = build_required_inputs_set();
        Block new_input_header{};

        for (const auto & col_type_and_name : *input_header)
            if (required_inputs_set.contains(col_type_and_name.name))
                new_input_header.insert(col_type_and_name);

        SharedHeader new_shared_input_header = std::make_shared<const Block>(std::move(new_input_header));
        updateInputHeader(std::move(new_shared_input_header), 0);

        return RemovedUnusedColumns::OutputAndInput;
    }

    updateOutputHeader();

    return RemovedUnusedColumns::OutputOnly;
}

bool ExpressionStep::canRemoveColumnsFromOutput() const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    return canRemoveUnusedColumns();
}

QueryPlanStepPtr ExpressionStep::clone() const
{
    return std::make_unique<ExpressionStep>(*this);
}

void registerExpressionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Expression", ExpressionStep::deserialize);
}

}
