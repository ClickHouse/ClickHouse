#include <Processors/QueryPlan/FilterStep.h>

#include <algorithm>
#include <limits>
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

FilterDAGOutputPruningResult pruneFilterDAGOutputsByPosition(
    ActionsDAG & dag,
    const String & filter_column_name,
    bool & remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & required_output_positions,
    bool remove_inputs)
{
    FilterDAGOutputPruningResult result;

    const bool was_remove_filter_column = remove_filter_column;
    const auto & old_outputs = dag.getOutputs();
    const size_t old_dag_outputs_size = old_outputs.size();
    const auto actions_dag_input_count_before = dag.getInputs().size();

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

    /// Map positions from the final (post-erase) header to the pre-erase header.
    /// remove_filter_column is captured by value because the mapping depends on the original value of the flag, not on
    /// whether the filter column is still present at the time of mapping.
    auto map_to_pre_erase_pos = [filter_col_pre_erase_pos, was_remove_filter_column](size_t pos) -> size_t
    {
        if (!was_remove_filter_column)
            return pos;
        return pos >= filter_col_pre_erase_pos ? pos + 1 : pos;
    };

    /// Map positions from post-erase to pre-erase layout, then split into DAG vs pass-through.
    std::vector<size_t> pre_erase_positions;
    pre_erase_positions.reserve(required_output_positions.size());
    for (size_t pos : required_output_positions)
        pre_erase_positions.push_back(map_to_pre_erase_pos(pos));

    auto [required_dag_indices, required_passthrough_indices] = dag.splitOutputPositions(pre_erase_positions);

    /// Build the list of pass-through input columns.
    const auto passthrough_input_header_positions = dag.matchInputPositionsToHeader(input_header).passthrough;

    std::set<size_t> required_passthrough_input_header_positions;
    for (size_t passthrough_index : required_passthrough_indices)
    {
        if (passthrough_index >= passthrough_input_header_positions.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Required output position {} is out of range for pass-through inputs", passthrough_index);
        required_passthrough_input_header_positions.insert(passthrough_input_header_positions[passthrough_index]);
    }

    const auto has_to_remove_any_pass_through
        = passthrough_input_header_positions.size() > required_passthrough_input_header_positions.size();

    std::set<size_t> required_dag_index_set(required_dag_indices.begin(), required_dag_indices.end());

    /// Check if the filter column is required by the caller. If not, we can remove it.
    if (!remove_filter_column && !required_dag_index_set.contains(filter_col_pre_erase_pos))
        remove_filter_column = true;

    required_dag_index_set.insert(filter_col_pre_erase_pos);

    /// Keep only the required DAG output nodes, plus always keep the filter column.
    ActionsDAG::NodeRawConstPtrs new_dag_outputs;
    new_dag_outputs.reserve(required_dag_index_set.size());

    for (size_t i = 0; i < old_dag_outputs_size; ++i)
    {
        if (required_dag_index_set.contains(i))
            new_dag_outputs.push_back(old_outputs[i]);
    }

    auto & dag_outputs = dag.getOutputs();
    if (new_dag_outputs.size() != dag_outputs.size())
        result.changed = true;
    dag_outputs = std::move(new_dag_outputs);

    if (was_remove_filter_column != remove_filter_column)
        result.changed = true;

    if (dag.removeUnusedActions(remove_inputs))
        result.changed = true;

    if (!remove_inputs && has_to_remove_any_pass_through)
    {
        for (size_t passthrough_input_header_position : passthrough_input_header_positions)
        {
            if (!required_passthrough_input_header_positions.contains(passthrough_input_header_position))
            {
                const auto & column = input_header.getByPosition(passthrough_input_header_position);
                dag.addInput(column);
            }
        }
        result.changed = true;
    }

    if (remove_inputs)
    {
        auto required_input_positions = dag.matchInputPositionsToHeader(input_header).matched;
        required_input_positions.insert(
            required_input_positions.end(),
            required_passthrough_input_header_positions.begin(),
            required_passthrough_input_header_positions.end());

        std::sort(required_input_positions.begin(), required_input_positions.end());
        result.required_input_positions = std::move(required_input_positions);
        result.input_positions_changed = dag.getInputs().size() != actions_dag_input_count_before || has_to_remove_any_pass_through;

        if (result.input_positions_changed)
            result.changed = true;
    }

    return result;
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
    auto split_result = dag.split({filter_node}, true);
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

    for (auto & and_atom : and_atoms)
    {
        auto expression = std::make_shared<ExpressionActions>(std::move(and_atom.dag), settings.getActionsSettings());
        pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
        {
            bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
            return std::make_shared<FilterTransform>(header, expression, and_atom.name, true, on_totals);
        });
    }

    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag), settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        return std::make_shared<FilterTransform>(header, expression, filter_column_name, remove_filter_column, on_totals, nullptr, condition);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), *output_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_actions_dag), settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const SharedHeader & header)
                                    { return std::make_shared<ExpressionTransform>(header, convert_actions, dataflow_cache_updater); });
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

    settings.out << prefix << "Filter column: "
                 << (settings.pretty ? QueryPlanFormat::formatColumnPretty(filter_column_name, settings.pretty_names) : filter_column_name);

    if (!settings.pretty && remove_filter_column)
        settings.out << " (removed)";
    settings.out << '\n';

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

    ActionsDAG actions_dag = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context);

    return std::make_unique<FilterStep>(ctx.input_headers.front(), std::move(actions_dag), std::move(filter_column_name), remove_filter_column);
}

bool FilterStep::canRemoveUnusedColumns() const
{
    return true;
}

FilterStep::RemoveUnusedColumnsResult FilterStep::removeUnusedColumns(const std::vector<size_t> & required_output_positions, bool remove_inputs)
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in FilterStep");

    /// When extra columns were absorbed from a child step that cannot reduce its output,
    /// prevent input removal to avoid re-creating the mismatch on subsequent optimization passes.
    if (prevent_input_removal)
        remove_inputs = false;

    chassert(
        actions_dag.getInputs().size() <= getInputHeaders().at(0)->columns()
        && "There cannot be more DAG inputs than columns in the input header");

    const auto & input_header = input_headers.front();
    auto pruning_result = pruneFilterDAGOutputsByPosition(
        actions_dag, filter_column_name, remove_filter_column, *input_header, required_output_positions, remove_inputs);

    if (!pruning_result.changed && output_header->columns() == required_output_positions.size())
        return {};

    if (actions_dag.getInputs().size() > getInputHeaders().at(0)->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There cannot be more inputs in the DAG than columns in the input header");

    if (pruning_result.input_positions_changed)
    {
        Block new_input_header{};
        for (size_t pos : pruning_result.required_input_positions)
            new_input_header.insert(input_header->getByPosition(pos));

        SharedHeader new_shared_input_header = std::make_shared<const Block>(std::move(new_input_header));
        updateInputHeader(std::move(new_shared_input_header), 0);
        return {true, {std::move(pruning_result.required_input_positions)}, required_output_positions};
    }

    updateOutputHeader();

    return {true, {}, required_output_positions};
}

bool FilterStep::canRemoveColumnsFromOutput() const
{
    if (output_header == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output header is not set in FilterStep");

    if (!remove_filter_column && output_header->columns() == 1)
        return false;

    return canRemoveUnusedColumns();
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
