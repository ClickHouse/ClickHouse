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

bool ExpressionStep::RequiredColumnsPlan::removesAnyAction() const
{
    return remove_inputs ? removes_any_action_removing_inputs : removes_any_action_keeping_inputs;
}

std::vector<size_t> ExpressionStep::RequiredColumnsPlan::droppedPassThroughPositions() const
{
    std::vector<size_t> positions;
    for (size_t position = 0; position < input_columns.size(); ++position)
        if (input_columns[position] == InputColumn::PassesThroughDropped)
            positions.push_back(position);

    return positions;
}

bool ExpressionStep::RequiredColumnsPlan::changesAnything() const
{
    if (removes_any_output || removesAnyAction())
        return true;

    /// Where the inputs have to stay, a pass-through nobody asked for becomes an input of the DAG, so
    /// that it stops being part of the output header. That is a change of its own.
    return !remove_inputs && !droppedPassThroughPositions().empty();
}

IQueryPlanStep::RemoveUnusedColumnsResult ExpressionStep::RequiredColumnsPlan::toResult() const
{
    if (!changesAnything())
        return {};

    RemoveUnusedColumnsResult result{true, {}, required_output_positions};

    /// Nothing is asked of the child while the inputs have to stay.
    if (!remove_inputs)
        return result;

    const auto drops_an_input = std::ranges::any_of(input_columns, [](auto column)
    {
        return column == InputColumn::ReadNotNeeded || column == InputColumn::PassesThroughDropped;
    });

    if (!drops_an_input)
        return result;

    std::vector<size_t> required_input_positions;
    for (size_t position = 0; position < input_columns.size(); ++position)
    {
        const auto column = input_columns[position];
        if (column == InputColumn::ReadAndNeeded || column == InputColumn::PassesThroughNeeded)
            required_input_positions.push_back(position);
    }

    result.required_input_positions = {std::move(required_input_positions)};
    return result;
}

ExpressionStep::RequiredColumnsPlan
ExpressionStep::analyzeRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    RequiredColumnsPlan plan;
    plan.required_output_positions = required_output_positions;

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
    plan.removes_any_output = output_header->columns() != required_output_positions.size();

    /// What removeUnusedActions would keep once the outputs are pruned. Its other root is every input,
    /// but only while inputs may not be removed - and since an input has no children, adding those roots
    /// keeps nothing besides the inputs themselves, so one walk answers for both cases.
    ///
    /// It also folds constants before it collects the nodes to keep, and folding clears the children of
    /// a folded node, so those children are dropped. Stop at such a node to see the same.
    const auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs roots;
    roots.reserve(plan.required_dag_positions.size());
    for (size_t position : plan.required_dag_positions)
        roots.push_back(dag_outputs[position]);
    for (const auto & node : actions_dag.getNodes())
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            roots.push_back(&node);

    const auto is_folded_constant = [](const ActionsDAG::Node * node)
    {
        return node->column && !node->children.empty();
    };

    const auto surviving_nodes = findReachableNodes(roots, is_folded_constant);

    /// One entry per column of the input header: the input reading it, or nothing when it passes by.
    const auto header_columns = mapHeaderColumnsToInputs(actions_dag.getInputs(), *input_header);
    plan.input_columns.resize(header_columns.size());

    /// The caller's pass-through indices ascend, and so do the pass-through columns, so one walk over
    /// the header pairs them up.
    size_t passthrough_index = 0;
    size_t next_required_passthrough = 0;
    size_t unread_input_count = 0;

    for (size_t position = 0; position < header_columns.size(); ++position)
    {
        if (!header_columns.passesThrough(position))
        {
            const auto * input = actions_dag.getInputs()[header_columns.read_by[position]];
            const bool is_needed = surviving_nodes.contains(input);
            plan.input_columns[position] = is_needed ? InputColumn::ReadAndNeeded : InputColumn::ReadNotNeeded;
            unread_input_count += !is_needed;
            continue;
        }

        const bool is_required = next_required_passthrough < required_passthrough_positions.size()
            && required_passthrough_positions[next_required_passthrough] == passthrough_index;

        if (is_required)
            ++next_required_passthrough;

        plan.input_columns[position] = is_required ? InputColumn::PassesThroughNeeded : InputColumn::PassesThroughDropped;
        ++passthrough_index;
    }

    if (next_required_passthrough != required_passthrough_positions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Required output position {} is out of range for pass-through inputs",
            required_passthrough_positions[next_required_passthrough]);

    /// An input nothing reads is erased only where inputs may be removed; otherwise it stays as a root
    /// of its own, which is the whole difference between the two answers.
    const auto node_count = actions_dag.getNodes().size();
    plan.removes_any_action_removing_inputs = surviving_nodes.size() < node_count;
    plan.removes_any_action_keeping_inputs = surviving_nodes.size() + unread_input_count < node_count;

    return plan;
}

ExpressionStep::RemoveUnusedColumnsResult
ExpressionStep::getRequiredColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs) const
{
    return analyzeRequiredColumns(required_output_positions, remove_inputs).toResult();
}

ExpressionStep::RemoveUnusedColumnsResult ExpressionStep::removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs)
{
    const auto plan = analyzeRequiredColumns(required_output_positions, remove_inputs);
    const auto result = plan.toResult();
    if (!result.changed)
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
    chassert(removed_any_action == plan.removesAnyAction());

    /// If we cannot remove inputs but need to remove pass-through outputs,
    /// convert unrequired pass-through inputs into DAG inputs so they stop being pass-throughs.
    if (!plan.remove_inputs)
    {
        for (size_t position : plan.droppedPassThroughPositions())
        {
            const auto & column = input_header->getByPosition(position);
            actions_dag.addInput(column.name, column.type);
        }
    }

    if (actions_dag.getInputs().size() > input_header->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There cannot be more inputs in the DAG than columns in the input header");

    if (result.required_input_positions.empty())
    {
        updateOutputHeader();
        return result;
    }

    Block new_input_header{};
    for (size_t position : result.required_input_positions.front())
        new_input_header.insert(input_header->getByPosition(position));

    updateInputHeader(std::make_shared<const Block>(std::move(new_input_header)), 0);

    return result;
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
