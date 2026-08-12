#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
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

    /// All streams of the pipe have the same header, so compute the transformed header once
    /// instead of in every ExpressionTransform instance: the computation is linear in the size
    /// of the DAG, and there is one transform per stream.
    auto transformed_header = std::make_shared<const Block>(ExpressionTransform::transformHeader(pipeline.getHeader(), expression->getActionsDAG()));
    pipeline.addSimpleTransform([&](const SharedHeader & header)
                                { return std::make_shared<ExpressionTransform>(header, transformed_header, expression, dataflow_cache_updater); });

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

        auto converted_header = std::make_shared<const Block>(ExpressionTransform::transformHeader(pipeline.getHeader(), convert_actions->getActionsDAG()));
        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<ExpressionTransform>(header, converted_header, convert_actions);
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
    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ExpressionStep must have one input stream");

    return std::make_unique<ExpressionStep>(ctx.input_headers.front(), std::move(actions_dag));
}

bool ExpressionStep::canRemoveUnusedColumns() const
{
    return true;
}

ExpressionStep::RequiredColumnsPlan
ExpressionStep::analyzeRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    RequiredColumnsPlan plan;

    /// When extra columns were absorbed from a child step that cannot reduce its output,
    /// prevent input removal to avoid re-creating the mismatch on subsequent optimization passes.
    plan.remove_inputs = prevent_input_removal ? false : remove_inputs;

    const auto & input_header = input_headers.front();

    /// The output header is structured as:
    /// [DAG output 0, ..., DAG output N-1, pass-through input 0, pass-through input 1, ...]
    /// Split required positions into DAG output indices and pass-through input indices.
    auto [required_dag_positions, required_passthrough_positions]
        = actions_dag.splitOutputPositions(required_output_positions);
    plan.required_dag_positions = std::move(required_dag_positions);

    /// Build the list of pass-through input columns (input header columns not consumed by DAG inputs).
    const auto input_header_positions = mapInputsToHeaderPositions(actions_dag.getInputs(), *input_header);
    const auto & passthrough_input_header_positions = input_header_positions.passthrough;

    /// The caller's pass-through indices ascend, so one walk over the pass-through list splits it into
    /// the columns to keep and the columns to drop, both in header order.
    size_t next_required_passthrough = 0;
    for (size_t index = 0; index < passthrough_input_header_positions.size(); ++index)
    {
        const auto header_position = passthrough_input_header_positions[index];
        if (next_required_passthrough < required_passthrough_positions.size()
            && required_passthrough_positions[next_required_passthrough] == index)
        {
            ++next_required_passthrough;
            plan.required_passthrough_header_positions.push_back(header_position);
        }
        else
            plan.dropped_passthrough_header_positions.push_back(header_position);
    }

    if (next_required_passthrough != required_passthrough_positions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Required output position {} is out of range for pass-through inputs",
            required_passthrough_positions[next_required_passthrough]);

    /// What removeUnusedActions would keep once the outputs are pruned: the kept outputs are its roots,
    /// and so is every input while inputs may not be removed.
    const auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs roots;
    roots.reserve(plan.required_dag_positions.size() + (plan.remove_inputs ? 0 : actions_dag.getInputs().size()));
    for (size_t position : plan.required_dag_positions)
        roots.push_back(dag_outputs[position]);
    for (const auto & node : actions_dag.getNodes())
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            roots.push_back(&node);
    if (!plan.remove_inputs)
        for (const auto * input : actions_dag.getInputs())
            roots.push_back(input);

    /// removeUnusedActions folds constants before it collects the nodes to keep, and folding clears the
    /// children of a folded node, so those children are dropped. Stop at such a node to see the same.
    const auto is_folded_constant = [](const ActionsDAG::Node * node)
    {
        return node->column && !node->children.empty();
    };

    const auto surviving_nodes = findReachableNodes(roots, is_folded_constant);
    plan.removes_any_action = surviving_nodes.size() < actions_dag.getNodes().size();

    /// Converting the dropped pass-throughs into DAG inputs is a change of its own, see the apply step.
    const auto adds_input_to_actions = !plan.remove_inputs && !plan.dropped_passthrough_header_positions.empty();
    const auto updates_actions = plan.removes_any_action || adds_input_to_actions;

    if (!updates_actions && output_header->columns() == required_output_positions.size())
        return plan;

    /// Inputs are rebuilt only when some of them go away: the surviving inputs, each at the header
    /// position it reads, plus the required pass-throughs. Dropping a pass-through also shrinks the
    /// input header, hence the second term. Every input reads a header position of its own and a
    /// pass-through is a position no input reads, so a mask over the header holds all of them.
    std::vector<bool> is_required_input(input_header->columns(), false);
    for (size_t position : plan.required_passthrough_header_positions)
        is_required_input[position] = true;

    const auto & inputs = actions_dag.getInputs();
    size_t surviving_input_count = 0;
    for (size_t position = 0; position < inputs.size(); ++position)
    {
        if (!surviving_nodes.contains(inputs[position]))
            continue;

        ++surviving_input_count;
        is_required_input[input_header_positions.matched[position]] = true;
    }

    const auto has_less_inputs = surviving_input_count < inputs.size();

    if (plan.remove_inputs && (has_less_inputs || !plan.dropped_passthrough_header_positions.empty()))
    {
        std::vector<size_t> required_input_positions;
        for (size_t position = 0; position < is_required_input.size(); ++position)
            if (is_required_input[position])
                required_input_positions.push_back(position);

        plan.result = {true, {std::move(required_input_positions)}, required_output_positions};
        return plan;
    }

    /// Outputs change but inputs do not.
    plan.result = {true, {}, required_output_positions};
    return plan;
}

ExpressionStep::RemoveUnusedColumnsResult
ExpressionStep::getRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const
{
    return analyzeRequiredColumns(required_output_positions, remove_inputs).result;
}

ExpressionStep::RemoveUnusedColumnsResult ExpressionStep::removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs)
{
    const auto plan = analyzeRequiredColumns(required_output_positions, remove_inputs);
    if (!plan.result.changed)
        return {};

    const auto & input_header = input_headers.front();

    /// Keep only the required DAG output nodes.
    auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs new_dag_outputs;
    new_dag_outputs.reserve(plan.required_dag_positions.size());
    for (size_t position : plan.required_dag_positions)
        new_dag_outputs.push_back(dag_outputs[position]);
    dag_outputs = std::move(new_dag_outputs);

    const auto removed_any_action = actions_dag.removeUnusedActions(plan.remove_inputs);
    chassert(removed_any_action == plan.removes_any_action);

    /// If we cannot remove inputs but need to remove pass-through outputs,
    /// convert unrequired pass-through inputs into DAG inputs so they stop being pass-throughs.
    if (!plan.remove_inputs)
    {
        for (size_t position : plan.dropped_passthrough_header_positions)
        {
            const auto & column = input_header->getByPosition(position);
            actions_dag.addInput(column.name, column.type);
        }
    }

    if (actions_dag.getInputs().size() > input_header->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There cannot be more inputs in the DAG than columns in the input header");

    if (plan.result.required_input_positions.empty())
    {
        updateOutputHeader();
        return plan.result;
    }

    const auto & required_input_positions = plan.result.required_input_positions.front();
    Block new_input_header{};
    for (size_t position : required_input_positions)
        new_input_header.insert(input_header->getByPosition(position));

    updateInputHeader(std::make_shared<const Block>(std::move(new_input_header)), 0);

    return plan.result;
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

void registerExpressionStep(QueryPlanStepRegistry & registry);
void registerExpressionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Expression", ExpressionStep::deserialize);
}

}
