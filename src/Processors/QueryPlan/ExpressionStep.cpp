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
    const String & prefix = settings.detail_prefix;
    auto expression = std::make_shared<ExpressionActions>(actions_dag.clone());
    if (!settings.compact)
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
    return true;
}

std::vector<std::vector<size_t>> ExpressionStep::removeUnusedColumns(std::vector<size_t> required_output_positions, bool remove_inputs)
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    /// When extra columns were absorbed from a child step that cannot reduce its output,
    /// prevent input removal to avoid re-creating the mismatch on subsequent optimization passes.
    if (prevent_input_removal)
        remove_inputs = false;

    const auto required_output_count = required_output_positions.size();
    const auto & input_header = input_headers.front();
    const auto actions_dag_input_count_before = actions_dag.getInputs().size();

    /// The output header is structured as:
    /// [DAG output 0, ..., DAG output N-1, pass-through input 0, pass-through input 1, ...]
    /// Split required positions into DAG output indices and pass-through input indices.
    auto [required_dag_indices, required_passthrough_indices]
        = actions_dag.splitOutputPositions(required_output_positions);

    /// Build the list of pass-through input columns (input header columns not consumed by DAG inputs).
    auto passthrough_input_header_positions = actions_dag.matchInputPositionsToHeader(*input_header).passthrough;

    /// Determine which pass-through inputs are required by the caller.
    std::set<size_t> required_passthrough_header_positions;
    for (size_t pt_idx : required_passthrough_indices)
    {
        if (pt_idx < passthrough_input_header_positions.size())
            required_passthrough_header_positions.insert(passthrough_input_header_positions[pt_idx]);
    }

    const auto num_passthrough = passthrough_input_header_positions.size();
    const auto has_to_remove_any_pass_through = num_passthrough > required_passthrough_indices.size();

    /// Keep only the required DAG output nodes.
    auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs new_dag_outputs;
    new_dag_outputs.reserve(required_dag_indices.size());
    for (size_t idx : required_dag_indices)
        new_dag_outputs.push_back(dag_outputs[idx]);
    dag_outputs = std::move(new_dag_outputs);

    auto updated_actions = actions_dag.removeUnusedActions(remove_inputs);

    /// If we cannot remove inputs but need to remove pass-through outputs,
    /// convert unrequired pass-through inputs into DAG inputs so they stop being pass-throughs.
    const auto has_to_add_input_to_actions = !remove_inputs && has_to_remove_any_pass_through;
    if (has_to_add_input_to_actions)
    {
        for (size_t pt_pos : passthrough_input_header_positions)
        {
            if (!required_passthrough_header_positions.contains(pt_pos))
            {
                const auto & col = input_header->getByPosition(pt_pos);
                actions_dag.addInput(col.name, col.type);
            }
        }
        updated_actions = true;
    }

    if (!updated_actions && output_header->columns() == required_output_count)
        return {};

    if (actions_dag.getInputs().size() > getInputHeaders().at(0)->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There cannot be more inputs in the DAG than columns in the input header");

    const auto actions_dag_has_less_inputs = actions_dag.getInputs().size() < actions_dag_input_count_before;
    const auto update_inputs = remove_inputs && (actions_dag_has_less_inputs || has_to_remove_any_pass_through);

    if (update_inputs)
    {
        /// Build the set of required input header positions: DAG inputs that survived pruning + required pass-throughs.
        auto matched_positions = actions_dag.matchInputPositionsToHeader(*input_header).matched;
        std::set<size_t> required_input_positions(matched_positions.begin(), matched_positions.end());

        /// Add required pass-through input positions.
        for (size_t pt_pos : required_passthrough_header_positions)
            required_input_positions.insert(pt_pos);

        /// Build the result vector and update the input header.
        std::vector<size_t> result_positions(required_input_positions.begin(), required_input_positions.end());

        Block new_input_header{};
        for (size_t pos : result_positions)
            new_input_header.insert(input_header->getByPosition(pos));

        SharedHeader new_shared_input_header = std::make_shared<const Block>(std::move(new_input_header));
        updateInputHeader(std::move(new_shared_input_header), 0);

        return {std::move(result_positions)};
    }

    updateOutputHeader();

    return {};
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
