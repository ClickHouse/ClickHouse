#include <Processors/QueryPlan/FilterStep.h>

#include <algorithm>
#include <ranges>
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

std::vector<ActionsAndName> splitAndChainIntoMultipleFilters(ActionsDAG & dag, const std::string & filter_name)
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
        return std::make_shared<FilterTransform>(header, expression, filter_column_name, remove_filter_column, on_totals, nullptr, condition, count_mergetree_output_rows);
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

    UInt8 flags;
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
    const size_t num_dag_outputs = actions_dag.getOutputs().size();
    const auto actions_dag_input_count_before = actions_dag.getInputs().size();

    /// The pre-erase output header (from updateHeader) is:
    /// [DAG output 0, ..., DAG output N-1, pass-through input 0, ...]
    /// When remove_filter_column is true, FilterTransform::transformHeader erases the first column
    /// named filter_column_name from this block, shifting subsequent positions by -1.
    /// Map the caller's positions (into the final output header) back to the pre-erase layout.

    /// Find the filter column's position in the pre-erase header.
    size_t filter_col_pre_erase_pos = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < num_dag_outputs; ++i)
    {
        if (actions_dag.getOutputs()[i]->result_name == filter_column_name)
        {
            filter_col_pre_erase_pos = i;
            break;
        }
    }
    chassert(filter_col_pre_erase_pos != std::numeric_limits<size_t>::max() && "Filter column not found in DAG outputs");

    /// Map positions from the final (post-erase) header to the pre-erase header.
    /// remove_filter_column is captured by value because the mapping depends on the original value of the flag, not on
    /// whether the filter column is still present at the time of mapping.
    auto map_to_pre_erase_pos = [filter_col_pre_erase_pos, is_filter_column_removed = remove_filter_column](size_t pos) -> size_t
    {
        if (!is_filter_column_removed)
            return pos;
        return pos >= filter_col_pre_erase_pos ? pos + 1 : pos;
    };

    /// Map positions from post-erase to pre-erase layout, then split into DAG vs pass-through.
    std::vector<size_t> pre_erase_positions;
    pre_erase_positions.reserve(required_output_positions.size());
    for (size_t pos : required_output_positions)
        pre_erase_positions.push_back(map_to_pre_erase_pos(pos));

    auto [required_dag_indices, required_passthrough_indices]
        = actions_dag.splitOutputPositions(pre_erase_positions);


    if (!remove_filter_column)
    {
        /// Check if the filter column is required by the caller. If not, we can remove it.
        bool filter_column_required_by_caller =
            std::find(required_dag_indices.begin(), required_dag_indices.end(), filter_col_pre_erase_pos)
            != required_dag_indices.end();
        if (!filter_column_required_by_caller)
            remove_filter_column = true;
    }

    /// Build the list of pass-through input columns.
    auto passthrough_input_header_positions = actions_dag.matchInputPositionsToHeader(*input_header).passthrough;

    std::set<size_t> required_passthrough_input_header_positions;
    for (size_t pt_idx : required_passthrough_indices)
    {
        if (pt_idx >= passthrough_input_header_positions.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Required output position {} is out of range for pass-through inputs", pt_idx);
        required_passthrough_input_header_positions.insert(passthrough_input_header_positions[pt_idx]);
    }

    const auto num_passthrough = passthrough_input_header_positions.size();
    const auto has_to_remove_any_pass_through = num_passthrough > required_passthrough_indices.size();

    /// Keep only the required DAG output nodes, plus always keep the filter column.
    auto & dag_outputs = actions_dag.getOutputs();
    ActionsDAG::NodeRawConstPtrs new_dag_outputs;
    new_dag_outputs.reserve(required_dag_indices.size() + 1);

    std::set<size_t> required_dag_index_set(required_dag_indices.begin(), required_dag_indices.end());

    /// Always keep the filter column in the DAG outputs.
    required_dag_index_set.insert(filter_col_pre_erase_pos);

    for (size_t i = 0; i < num_dag_outputs; ++i)
    {
        if (required_dag_index_set.contains(i))
            new_dag_outputs.push_back(dag_outputs[i]);
    }
    dag_outputs = std::move(new_dag_outputs);

    auto updated_actions = actions_dag.removeUnusedActions(remove_inputs);

    /// If we cannot remove inputs but need to remove pass-through outputs,
    /// convert unrequired pass-through inputs into DAG inputs.
    const auto has_to_add_input_to_actions = !remove_inputs && has_to_remove_any_pass_through;
    if (has_to_add_input_to_actions)
    {
        for (size_t pt_pos : passthrough_input_header_positions)
        {
            if (!required_passthrough_input_header_positions.contains(pt_pos))
            {
                const auto & col = input_header->getByPosition(pt_pos);
                actions_dag.addInput(col);
            }
        }
        updated_actions = true;
    }

    if (!updated_actions && output_header->columns() == required_output_positions.size())
        return {};

    if (actions_dag.getInputs().size() > getInputHeaders().at(0)->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There cannot be more inputs in the DAG than columns in the input header");

    const auto actions_dag_has_less_inputs = actions_dag.getInputs().size() < actions_dag_input_count_before;
    const auto update_inputs = remove_inputs && (actions_dag_has_less_inputs || has_to_remove_any_pass_through);

    if (update_inputs)
    {
        /// Build the set of required input header positions.
        auto matched_positions = actions_dag.matchInputPositionsToHeader(*input_header).matched;
        std::vector<size_t> required_input_positions(matched_positions.begin(), matched_positions.end());
        required_input_positions.insert(
            required_input_positions.end(),
            required_passthrough_input_header_positions.begin(),
            required_passthrough_input_header_positions.end());

        /// Build the result vector and update the input header.
        std::sort(required_input_positions.begin(), required_input_positions.end());

        Block new_input_header{};
        for (size_t pos : required_input_positions)
            new_input_header.insert(input_header->getByPosition(pos));

        SharedHeader new_shared_input_header = std::make_shared<const Block>(std::move(new_input_header));
        updateInputHeader(std::move(new_shared_input_header), 0);
        return {true, {std::move(required_input_positions)}, required_output_positions};
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

void registerFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Filter", FilterStep::deserialize);
}

}
