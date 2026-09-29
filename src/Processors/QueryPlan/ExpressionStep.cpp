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

std::vector<size_t> ExpressionStep::RequiredColumnsPlan::requiredInputPositions() const
{
    std::vector<size_t> positions;
    for (size_t position = 0; position < input_columns.size(); ++position)
    {
        const auto column = input_columns[position];
        if (!remove_inputs || column == InputColumnUsage::ReadNeeded || column == InputColumnUsage::PassesThroughNeeded)
            positions.push_back(position);
    }

    return positions;
}

ExpressionStep::RequiredColumnsPlan
ExpressionStep::analyzeRequiredColumns(const std::vector<size_t> & required_output_positions) const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    RequiredColumnsPlan plan;
    plan.required_output_positions = required_output_positions;

    /// When extra columns were absorbed from a child step that cannot reduce its output,
    /// prevent input removal to avoid re-creating the mismatch on subsequent optimization passes.
    plan.remove_inputs = !prevent_input_removal;

    const auto & input_header = input_headers.front();

    /// The output header is structured as:
    /// [DAG output 0, ..., DAG output N-1, pass-through input 0, pass-through input 1, ...]
    /// so the positions below the number of DAG outputs are the DAG outputs the caller asked for, and
    /// the rest name pass-through columns, counting from that number. The positions are sorted, so the
    /// first group is a prefix of them and the second is the remaining suffix.
    chassert(std::ranges::is_sorted(required_output_positions));
    const auto dag_output_count = actions_dag.getOutputs().size();
    const auto first_passthrough
        = std::ranges::lower_bound(required_output_positions, dag_output_count) - required_output_positions.begin();

    plan.dag_position_count = first_passthrough;

    const auto required_passthrough_positions
        = std::span{required_output_positions}.subspan(first_passthrough);

    /// What removeUnusedActions would keep once the outputs are pruned.
    ///
    /// It also folds constants before it collects the nodes to keep, and folding clears the children of
    /// a folded node, so those children are dropped. Stop at such a node to see the same.
    const auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs roots;
    roots.reserve(plan.dag_position_count);
    for (size_t position : plan.requiredDAGPositions())
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

    for (size_t position = 0; position < header_columns.size(); ++position)
    {
        if (!header_columns.passesThrough(position))
        {
            const auto * input = actions_dag.getInputs()[header_columns.read_by[position]];
            const bool is_needed = surviving_nodes.contains(input);
            plan.input_columns[position] = is_needed ? InputColumnUsage::ReadNeeded : InputColumnUsage::ReadDropped;
            continue;
        }

        const bool is_required = next_required_passthrough < required_passthrough_positions.size()
            && required_passthrough_positions[next_required_passthrough] - dag_output_count == passthrough_index;

        if (is_required)
            ++next_required_passthrough;

        plan.input_columns[position] = is_required ? InputColumnUsage::PassesThroughNeeded : InputColumnUsage::PassesThroughDropped;
        ++passthrough_index;
    }

    if (next_required_passthrough != required_passthrough_positions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Required output position {} is out of range for the output header",
            required_passthrough_positions[next_required_passthrough]);

    return plan;
}

ExpressionStep::RequiredInputPositions ExpressionStep::getRequiredColumns(const std::vector<size_t> & required_output_positions) const
{
    return {analyzeRequiredColumns(required_output_positions).requiredInputPositions()};
}

ExpressionStep::RemoveUnusedColumnsResult
ExpressionStep::removeUnusedColumns(const std::vector<size_t> & required_output_positions, const std::vector<PrunedInput> & inputs)
{
    const auto plan = analyzeRequiredColumns(required_output_positions);
    const auto & pruned = inputs.at(0);
    const auto input_header = input_headers.front();

    /// Keep only the required DAG output nodes.
    auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs new_dag_outputs;
    new_dag_outputs.reserve(plan.dag_position_count);
    for (size_t position : plan.requiredDAGPositions())
        new_dag_outputs.push_back(dag_outputs[position]);
    dag_outputs = std::move(new_dag_outputs);

    RemoveUnusedColumnsResult result;
    result.dropped_output_positions = complementPositions(output_header->columns(), required_output_positions);

    const bool dag_changed = alignInputsWithPrunedChild(actions_dag, plan.input_columns, *input_header, pruned);
    result.step_changed = !result.dropped_output_positions.empty() || dag_changed
        || !blocksHaveEqualStructure(*input_header, *pruned.header);

    if (result.step_changed)
        updateInputHeader(pruned.header, 0);

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
