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

#include <span>
#include <unordered_set>


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

ActionsDAG::NodeRawConstPtrs ExpressionStep::UnneededColumnsPlan::neededDAGOutputs(const ActionsDAG::NodeRawConstPtrs & outputs) const
{
    ActionsDAG::NodeRawConstPtrs needed;
    needed.reserve(outputs.size() - unneeded_dag_position_count);

    size_t next_unneeded = 0;
    for (size_t position = 0; position < outputs.size(); ++position)
    {
        if (next_unneeded < unneeded_dag_position_count && unneeded_output_positions[next_unneeded] == position)
            ++next_unneeded;
        else
            needed.push_back(outputs[position]);
    }

    return needed;
}

std::vector<size_t> ExpressionStep::UnneededColumnsPlan::unneededInputPositions() const
{
    std::vector<size_t> positions;
    for (size_t position = 0; position < input_columns.size(); ++position)
    {
        const auto column = input_columns[position];
        if (column == InputColumnUsage::ReadDropped || column == InputColumnUsage::PassesThroughDropped)
            positions.push_back(position);
    }

    return positions;
}

ExpressionStep::UnneededColumnsPlan
ExpressionStep::analyzeUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in ExpressionStep");

    UnneededColumnsPlan plan;
    plan.unneeded_output_positions = unneeded_output_positions;

    const auto & input_header = input_headers.front();

    /// The output header is structured as:
    /// [DAG output 0, ..., DAG output N-1, pass-through input 0, pass-through input 1, ...]
    /// so the positions below the number of DAG outputs are the DAG outputs nobody needs, and the rest
    /// name pass-through columns, counting from that number. The positions are sorted, so the first
    /// group is a prefix of them and the second is the remaining suffix.
    chassert(std::ranges::is_sorted(unneeded_output_positions));
    const auto dag_output_count = actions_dag.getOutputs().size();
    const auto first_passthrough
        = std::ranges::lower_bound(unneeded_output_positions, dag_output_count) - unneeded_output_positions.begin();

    plan.unneeded_dag_position_count = first_passthrough;

    const auto unneeded_passthrough_positions
        = std::span{unneeded_output_positions}.subspan(first_passthrough);

    /// What removeUnusedActions would keep once the outputs are pruned.
    ///
    /// It also folds constants before it collects the nodes to keep, and folding clears the children of
    /// a folded node, so those children are dropped. Stop at such a node to see the same.
    auto roots = plan.neededDAGOutputs(actions_dag.getOutputs());
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
    size_t next_unneeded_passthrough = 0;

    /// A dropped column the child keeps is consumed by a new input, and inputs are paired with header columns by name,
    /// in order. So a pass-through column after a needed one of the same name cannot be consumed, and is kept.
    std::unordered_set<std::string_view> needed_passthrough_names;
    std::vector<size_t> kept_unneeded_positions;

    for (size_t position = 0; position < header_columns.size(); ++position)
    {
        if (!header_columns.passesThrough(position))
        {
            const auto * input = actions_dag.getInputs()[header_columns.read_by[position]];
            const bool is_needed = surviving_nodes.contains(input);
            plan.input_columns[position] = is_needed ? InputColumnUsage::ReadNeeded : InputColumnUsage::ReadDropped;
            continue;
        }

        const auto output_position = dag_output_count + passthrough_index;
        bool is_unneeded = next_unneeded_passthrough < unneeded_passthrough_positions.size()
            && unneeded_passthrough_positions[next_unneeded_passthrough] == output_position;

        if (is_unneeded)
            ++next_unneeded_passthrough;

        const auto & name = input_header->getByPosition(position).name;
        if (is_unneeded && needed_passthrough_names.contains(name))
        {
            is_unneeded = false;
            kept_unneeded_positions.push_back(output_position);
        }

        if (!is_unneeded)
            needed_passthrough_names.insert(name);

        plan.input_columns[position] = is_unneeded ? InputColumnUsage::PassesThroughDropped : InputColumnUsage::PassesThroughNeeded;
        ++passthrough_index;
    }

    if (next_unneeded_passthrough != unneeded_passthrough_positions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unneeded output position {} is out of range for the output header",
            unneeded_passthrough_positions[next_unneeded_passthrough]);

    /// Both lists are sorted, and the kept positions are pass-through ones, so they are not in the prefix of DAG outputs.
    std::erase_if(plan.unneeded_output_positions, [&](size_t position) { return std::ranges::binary_search(kept_unneeded_positions, position); });

    return plan;
}

ExpressionStep::UnneededInputPositions ExpressionStep::getUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const
{
    return {analyzeUnneededColumns(unneeded_output_positions).unneededInputPositions()};
}

ExpressionStep::RemoveUnusedColumnsResult
ExpressionStep::removeUnusedColumns(const std::vector<size_t> & unneeded_output_positions, const std::vector<PrunedInput> & inputs)
{
    const auto plan = analyzeUnneededColumns(unneeded_output_positions);
    const auto & pruned = inputs.at(0);
    const auto input_header = input_headers.front();

    actions_dag.getOutputs() = plan.neededDAGOutputs(actions_dag.getOutputs());

    RemoveUnusedColumnsResult result;
    result.dropped_output_positions = plan.unneeded_output_positions;

    const bool dag_changed = alignInputsWithPrunedChild(actions_dag, plan.input_columns, *input_header, pruned);
    result.step_changed = !result.dropped_output_positions.empty() || dag_changed
        || !blocksHaveEqualStructure(*input_header, *pruned.header);

    if (result.step_changed)
        updateInputHeader(pruned.header, 0);

    return result;
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
