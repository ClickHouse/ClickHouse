#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <limits>
#include <optional>
#include <ranges>
#include <set>
#include <stack>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilterStep::UnneededColumnsPlan FilterStep::analyzeUnneededColumns(
    const ActionsDAG & dag,
    const String & filter_column_name,
    bool remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & unneeded_output_positions)
{
    UnneededColumnsPlan plan;
    plan.remove_filter_column = remove_filter_column;

    const auto & old_outputs = dag.getOutputs();
    const size_t old_dag_outputs_size = old_outputs.size();

    /// The pre-erase output header (from ActionsDAG::updateHeader) is:
    /// [DAG output 0, ..., DAG output N-1, pass-through input 0, ...]
    /// When remove_filter_column is true, then the first column named filter_column_name is
    /// erased from the block, shifting subsequent positions by -1.
    /// Map the caller's positions (into the final output header) back to the pre-erase layout.

    /// Find the filter column's position in the pre-erase header.
    size_t filter_col_pre_erase_pos = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < old_dag_outputs_size; ++i)
    {
        if (old_outputs[i]->result_name == filter_column_name)
        {
            filter_col_pre_erase_pos = i;
            break;
        }
    }
    if (filter_col_pre_erase_pos == std::numeric_limits<size_t>::max())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Filter column {} not found in DAG outputs: [{}]",
            filter_column_name,
            fmt::join(dag.getNames(), ", "));

    /// Map positions from the final (post-erase) header to the pre-erase header. The mapping depends on
    /// the incoming value of the flag, not on the value the pruning settles on. An erased filter column is not
    /// in the final header, so no position maps to it.
    auto map_to_pre_erase_pos = [filter_col_pre_erase_pos, remove_filter_column](size_t pos) -> size_t
    {
        if (!remove_filter_column)
            return pos;
        return pos >= filter_col_pre_erase_pos ? pos + 1 : pos;
    };

    /// Map positions from post-erase to pre-erase layout, then split into DAG vs pass-through.
    std::vector<size_t> pre_erase_positions;
    pre_erase_positions.reserve(unneeded_output_positions.size());
    for (size_t pos : unneeded_output_positions)
        pre_erase_positions.push_back(map_to_pre_erase_pos(pos));

    auto [unneeded_dag_indices, unneeded_passthrough_indices] = dag.splitOutputPositions(pre_erase_positions);

    /// One entry per column of the input header: the input reading it, or nothing when it passes by.
    /// The caller's pass-through indices ascend, and so do the pass-through columns, so one walk over
    /// the header splits them into the columns to keep and the columns to drop.
    const auto header_columns = mapHeaderColumnsToInputs(dag.getInputs(), input_header);

    plan.input_columns.resize(header_columns.size());
    size_t passthrough_index = 0;
    size_t next_unneeded_passthrough = 0;
    for (size_t position = 0; position < header_columns.size(); ++position)
    {
        if (!header_columns.passesThrough(position))
            continue;

        if (next_unneeded_passthrough < unneeded_passthrough_indices.size()
            && unneeded_passthrough_indices[next_unneeded_passthrough] == passthrough_index)
        {
            ++next_unneeded_passthrough;
            plan.input_columns[position] = InputColumnUsage::PassesThroughDropped;
        }
        else
            plan.input_columns[position] = InputColumnUsage::PassesThroughNeeded;

        ++passthrough_index;
    }

    if (next_unneeded_passthrough != unneeded_passthrough_indices.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unneeded output position {} is out of range for pass-through inputs",
            unneeded_passthrough_indices[next_unneeded_passthrough]);

    /// Nobody reads the filter column any more, so it can be removed from the header.
    if (!plan.remove_filter_column && std::ranges::binary_search(unneeded_dag_indices, filter_col_pre_erase_pos))
        plan.remove_filter_column = true;

    /// The filter column is dropped, so its `materialize` wrapper is no longer observable, and
    /// `applyToOutputs` folds it away. That changes what survives the pruning, so the rest of
    /// this looks at the DAG as it is going to be. This includes filters that were already marked for
    /// removal by `ReadFromMergeTree`. The fold only looks through `materialize` and aliases, so without a
    /// `materialize` in the predicate there is nothing to fold.
    std::optional<ActionsDAG> folded_dag;
    if (plan.remove_filter_column)
    {
        const auto predicate_nodes = findReachableNodes({old_outputs[filter_col_pre_erase_pos]});
        const bool has_materialize = std::ranges::any_of(predicate_nodes, [](const auto * node)
        {
            return node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "materialize";
        });

        if (has_materialize)
        {
            folded_dag = dag.clone();
            folded_dag->foldFilterPredicateThroughMaterialize(filter_col_pre_erase_pos);
            /// The fold only ever replaces the filter output by a constant.
            plan.fold_filter_predicate = folded_dag->getOutputs()[filter_col_pre_erase_pos]->type == ActionsDAG::ActionType::COLUMN
                && old_outputs[filter_col_pre_erase_pos]->type != ActionsDAG::ActionType::COLUMN;
            if (!plan.fold_filter_predicate)
                folded_dag.reset();
        }
    }
    const auto & analyzed_dag = folded_dag ? *folded_dag : dag;
    const auto & analyzed_outputs = analyzed_dag.getOutputs();

    /// The filter column is needed to filter, whether or not anyone reads it.
    std::erase(unneeded_dag_indices, filter_col_pre_erase_pos);
    plan.unneeded_dag_positions = std::move(unneeded_dag_indices);

    plan.filter_output_position = filter_col_pre_erase_pos;
    plan.changes_output_header = !plan.unneeded_dag_positions.empty() || remove_filter_column != plan.remove_filter_column;

    /// What removeUnusedActions would keep once the outputs are pruned. It folds constants before it
    /// collects the nodes to keep, and folding clears the children of a folded node, so stop at such a
    /// node to see the same set. ARRAY_JOIN is always a root there.
    auto roots = plan.neededDAGOutputs(analyzed_outputs);
    for (const auto & node : analyzed_dag.getNodes())
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            roots.push_back(&node);

    const auto is_folded_constant = [](const ActionsDAG::Node * node) { return node->column && !node->children.empty(); };
    const auto surviving_nodes = findReachableNodes(roots, is_folded_constant);

    plan.removes_any_action = surviving_nodes.size() < analyzed_dag.getNodes().size();

    /// Every input reads a header position of its own, so the column it reads is needed exactly when the
    /// input survives. A clone keeps the inputs in their order. While inputs may not be removed, every
    /// input is a root, and so survives.
    const auto & inputs = analyzed_dag.getInputs();
    for (size_t position = 0; position < header_columns.size(); ++position)
        if (!header_columns.passesThrough(position))
            plan.input_columns[position] = surviving_nodes.contains(inputs[header_columns.read_by[position]])
                ? InputColumnUsage::ReadNeeded
                : InputColumnUsage::ReadDropped;

    return plan;
}

ActionsDAG::NodeRawConstPtrs FilterStep::UnneededColumnsPlan::neededDAGOutputs(const ActionsDAG::NodeRawConstPtrs & outputs) const
{
    ActionsDAG::NodeRawConstPtrs needed;
    needed.reserve(outputs.size() - unneeded_dag_positions.size());

    size_t next_unneeded = 0;
    for (size_t position = 0; position < outputs.size(); ++position)
    {
        if (next_unneeded < unneeded_dag_positions.size() && unneeded_dag_positions[next_unneeded] == position)
            ++next_unneeded;
        else
            needed.push_back(outputs[position]);
    }

    return needed;
}

FilterDAGOutputPruningResult FilterStep::UnneededColumnsPlan::toResult() const
{
    const bool drops_a_passthrough = std::ranges::contains(input_columns, InputColumnUsage::PassesThroughDropped);
    const bool drops_an_input = std::ranges::contains(input_columns, InputColumnUsage::ReadDropped);

    FilterDAGOutputPruningResult result;
    result.input_positions_changed = drops_an_input || drops_a_passthrough;
    result.changed = changes_output_header || fold_filter_predicate || removes_any_action || result.input_positions_changed;

    for (size_t position = 0; position < input_columns.size(); ++position)
        if (input_columns[position] == InputColumnUsage::ReadNeeded || input_columns[position] == InputColumnUsage::PassesThroughNeeded)
            result.required_input_positions.push_back(position);

    return result;
}

void FilterStep::UnneededColumnsPlan::applyToOutputs(ActionsDAG & dag, bool & remove_filter_column_) const
{
    if (fold_filter_predicate)
        dag.foldFilterPredicateThroughMaterialize(filter_output_position);

    dag.getOutputs() = neededDAGOutputs(dag.getOutputs());
    remove_filter_column_ = remove_filter_column;
}

FilterDAGOutputPruningResult FilterStep::pruneDAGOutputsByPosition(
    ActionsDAG & dag,
    const String & filter_column_name,
    bool & remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & required_output_positions)
{
    /// The output header holds the DAG outputs, then the columns of the input header no input reads, without the
    /// filter column where it is removed. Every input reads a column of the header of its own.
    const size_t output_count = dag.getOutputs().size() + input_header.columns() - dag.getInputs().size()
        - (remove_filter_column ? 1 : 0);
    chassert(output_count == FilterTransform::transformHeader(input_header, &dag, filter_column_name, remove_filter_column).columns());

    const auto plan = analyzeUnneededColumns(
        dag, filter_column_name, remove_filter_column, input_header, complementPositions(output_count, required_output_positions));

    plan.applyToOutputs(dag, remove_filter_column);
    [[maybe_unused]] const bool removed_any_action = dag.removeUnusedActions();
    chassert(removed_any_action == plan.removes_any_action);

    return plan.toResult();
}

static bool isTrivialSubtree(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.at(0);

    return node->type != ActionsDAG::ActionType::FUNCTION && node->type != ActionsDAG::ActionType::ARRAY_JOIN;
}

struct ActionsAndName
{
    ActionsDAG dag;
    std::string name;
};

static ActionsAndName splitSingleAndFilter(ActionsDAG & dag, const ActionsDAG::Node * filter_node)
{
    /// avoid_duplicate_inputs: the split promotes the atom into an input of the remainder DAG, copying its
    /// name verbatim. Duplicate names are legal inside a DAG but break the `Block` invariant, so a colliding
    /// input has to be renamed (`split` adds an `ALIAS` so the original name stays resolvable).
    auto split_result = dag.split({filter_node}, true, true);
    dag = std::move(split_result.second);

    const auto * split_filter_node = split_result.split_nodes_mapping[filter_node];
    split_result.first.getOutputs().emplace(split_result.first.getOutputs().begin(), split_filter_node);
    auto name = split_filter_node->result_name;
    return ActionsAndName{std::move(split_result.first), std::move(name)};
}

/// Try to split the left most AND atom to a separate DAG.
static std::optional<ActionsAndName> trySplitSingleAndFilter(ActionsDAG & dag, const std::string & filter_name)
{
    const auto * filter = &dag.findInOutputs(filter_name);
    while (filter->type == ActionsDAG::ActionType::ALIAS)
        filter = filter->children.at(0);

    if (filter->type != ActionsDAG::ActionType::FUNCTION || filter->function_base->getName() != "and")
        return {};

    const ActionsDAG::Node * condition_to_split = nullptr;
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(filter);
    while (!nodes.empty())
    {
        const auto * node = nodes.top();
        nodes.pop();

        if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "and")
        {
            /// The order is important. We should take the left-most atom, so put conditions on stack in reverse order.
            for (const auto * child : node->children | std::ranges::views::reverse)
                nodes.push(child);

            continue;
        }

        if (isTrivialSubtree(node))
            continue;

        /// Do not split subtree if it's the last non-trivial one.
        /// So, split the first found condition only when there is a another one found.
        if (condition_to_split)
            return splitSingleAndFilter(dag, condition_to_split);

        condition_to_split = node;
    }

    return {};
}

static std::vector<ActionsAndName> splitAndChainIntoMultipleFilters(ActionsDAG & dag, const std::string & filter_name)
{
    std::vector<ActionsAndName> res;

    while (auto condition = trySplitSingleAndFilter(dag, filter_name))
        res.push_back(std::move(*condition));

    return res;
}

FilterStep::FilterStep(
    const SharedHeader & input_header_,
    ActionsDAG actions_dag_,
    String filter_column_name_,
    bool remove_filter_column_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(FilterTransform::transformHeader(
            *input_header_,
            &actions_dag_,
            filter_column_name_,
            remove_filter_column_)),
        getTraits())
    , actions_dag(std::move(actions_dag_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
    /// The filter column is dropped from the output, so only the predicate's value is observable and it
    /// is safe to fold a `materialize`-wrapped constant away (#78166). Doing it here covers every way a
    /// dropped-filter step comes to be - in particular the fresh steps `tryPushDownFilter` creates, which
    /// neither `tryMergeExpressions` nor `pruneDAGOutputsByPosition` ever sees.
    if (remove_filter_column)
        actions_dag.foldFilterPredicateThroughMaterialize(filter_column_name);

    actions_dag.removeAliasesForFilter(filter_column_name);
    /// Removing aliases may result in unneeded ALIAS node in DAG.
    /// This should not be an issue by itself,
    /// but it might trigger an issue with duplicated names in Block after plan optimizations.
    actions_dag.removeUnusedActions(false, false);
}

void FilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    std::vector<ActionsAndName> and_atoms;

    /// Splitting AND filter condition to steps under the setting, which is enabled with merge_filters optimization.
    /// This is needed to support short-circuit properly.
    if (settings.enable_multiple_filters_transforms_for_and_chain && !actions_dag.hasStatefulFunctions())
        and_atoms = splitAndChainIntoMultipleFilters(actions_dag, filter_column_name);

    /// All streams of the pipe have the same header, so compute the transformed header once
    /// instead of in every FilterTransform instance: the computation is linear in the size
    /// of the DAG, and there is one transform per stream.
    for (auto & and_atom : and_atoms)
    {
        auto expression = std::make_shared<ExpressionActions>(std::move(and_atom.dag), settings.getActionsSettings());
        auto transformed_header = std::make_shared<const Block>(expression->getActionsDAG().updateHeader(pipeline.getHeader()));
        pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
        {
            bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;

            /// Each split atom gets the same query condition cache key.
            return std::make_shared<FilterTransform>(header, transformed_header, expression, and_atom.name, true, on_totals, nullptr, condition);
        });
    }

    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag), settings.getActionsSettings());

    auto transformed_header = std::make_shared<const Block>(expression->getActionsDAG().updateHeader(pipeline.getHeader()));
    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<FilterTransform>(header, transformed_header, expression, filter_column_name, remove_filter_column, on_totals, nullptr, condition);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), *output_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), settings.getActionsSettings());

        auto converted_header = std::make_shared<const Block>(ExpressionTransform::transformHeader(pipeline.getHeader(), convert_actions->getActionsDAG()));
        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<ExpressionTransform>(header, converted_header, convert_actions, dataflow_cache_updater); });
    }
    else
    {
        if (dataflow_cache_updater)
        {
            pipeline.addSimpleTransform([&](const SharedHeader & header)
                                        { return std::make_shared<RuntimeDataflowStatisticsCollector>(header, dataflow_cache_updater); });
        }
    }
}

void FilterStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    auto cloned_dag = actions_dag.clone();

    std::vector<ActionsAndName> and_atoms;
    if (!settings.pretty && !actions_dag.hasStatefulFunctions())
        and_atoms = splitAndChainIntoMultipleFilters(cloned_dag, filter_column_name);

    for (auto & and_atom : and_atoms)
    {
        settings.out << prefix << "AND column: " << and_atom.name << '\n';
        if (!settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(std::move(and_atom.dag));
            expression->describeActions(settings.out, prefix);
        }
    }

    /// A condition made only of join runtime filters renders as an empty pretty expression plus an
    /// annotation, the same `Runtime filters:` line a read step shows. Print the annotation on its own
    /// rather than an empty `Filter column:`.
    const auto annotation = settings.pretty ? QueryPlanFormat::getColumnAnnotation(filter_column_name, settings) : std::string_view{};
    const String pretty_column
        = settings.pretty ? QueryPlanFormat::formatColumnPretty(filter_column_name, settings.pretty_names) : filter_column_name;

    if (!pretty_column.empty() || annotation.empty())
    {
        settings.out << prefix << "Filter column: " << pretty_column;

        if (!settings.pretty && remove_filter_column)
            settings.out << " (removed)";
        settings.out << '\n';
    }

    if (!annotation.empty())
        settings.out << prefix << annotation << '\n';

    auto expression = std::make_shared<ExpressionActions>(std::move(cloned_dag));
    if (!settings.compact)
        expression->describeActions(settings.out, prefix);
}

void FilterStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto cloned_dag = actions_dag.clone();

    std::vector<ActionsAndName> and_atoms;
    if (!actions_dag.hasStatefulFunctions())
        and_atoms = splitAndChainIntoMultipleFilters(cloned_dag, filter_column_name);

    for (auto & and_atom : and_atoms)
    {
        auto expression = std::make_shared<ExpressionActions>(std::move(and_atom.dag));
        map.add("AND column", and_atom.name);
        map.add("Expression", expression->toTree());
    }

    map.add("Filter Column", filter_column_name);
    map.add("Removes Filter", remove_filter_column);

    auto expression = std::make_shared<ExpressionActions>(actions_dag.clone());
    map.add("Expression", expression->toTree());
}

void FilterStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(FilterTransform::transformHeader(*input_headers.front(), &actions_dag, filter_column_name, remove_filter_column));

    if (!getDataStreamTraits().preserves_sorting)
        return;
}

void FilterStep::setConditionForQueryConditionCache(UInt64 condition_hash_, const String & condition_)
{
    condition = {condition_hash_, condition_};
}

bool FilterStep::canUseType(const DataTypePtr & filter_type)
{
    return FilterTransform::canUseType(filter_type);
}


void FilterStep::serialize(Serialization & ctx) const
{
    UInt8 flags = 0;
    if (remove_filter_column)
        flags |= 1;
    writeIntBinary(flags, ctx.out);

    writeStringBinary(filter_column_name, ctx.out);

    actions_dag.serialize(ctx.out, ctx.registry);
}

QueryPlanStepPtr FilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "FilterStep must have one input stream");

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);

    bool remove_filter_column = bool(flags & 1);

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);

    return std::make_unique<FilterStep>(ctx.input_headers.front(), std::move(actions_dag), std::move(filter_column_name), remove_filter_column);
}

bool FilterStep::canRemoveUnusedColumns() const
{
    return true;
}

FilterStep::UnneededColumnsPlan FilterStep::analyzeUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in FilterStep");

    chassert(
        actions_dag.getInputs().size() <= getInputHeaders().at(0)->columns()
        && "There cannot be more DAG inputs than columns in the input header");

    return analyzeUnneededColumns(actions_dag, filter_column_name, remove_filter_column, *input_headers.front(), unneeded_output_positions);
}

FilterStep::UnneededInputPositions FilterStep::getUnneededColumns(const std::vector<size_t> & unneeded_output_positions) const
{
    const auto plan = analyzeUnneededColumns(unneeded_output_positions);

    std::vector<size_t> positions;
    for (size_t position = 0; position < plan.input_columns.size(); ++position)
        if (plan.input_columns[position] == InputColumnUsage::ReadDropped || plan.input_columns[position] == InputColumnUsage::PassesThroughDropped)
            positions.push_back(position);

    return {std::move(positions)};
}

FilterStep::RemoveUnusedColumnsResult
FilterStep::removeUnusedColumns(const std::vector<size_t> & unneeded_output_positions, const std::vector<PrunedInput> & inputs)
{
    const auto plan = analyzeUnneededColumns(unneeded_output_positions);
    const auto & pruned = inputs.at(0);
    const auto input_header = input_headers.front();

    plan.applyToOutputs(actions_dag, remove_filter_column);

    RemoveUnusedColumnsResult result;
    result.dropped_output_positions = unneeded_output_positions;

    const bool dag_changed = alignInputsWithPrunedChild(actions_dag, plan.input_columns, *input_header, pruned);
    result.step_changed = plan.changes_output_header || plan.fold_filter_predicate || dag_changed
        || !blocksHaveEqualStructure(*input_header, *pruned.header);

    if (result.step_changed)
        updateInputHeader(pruned.header, 0);

    return result;
}


QueryPlanStepPtr FilterStep::clone() const
{
    return std::make_unique<FilterStep>(*this);
}

void registerFilterStep(QueryPlanStepRegistry & registry);
void registerFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Filter", FilterStep::deserialize);
}

}
