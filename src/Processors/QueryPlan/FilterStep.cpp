#include <Processors/QueryPlan/FilterStep.h>
#include <boost/functional/hash.hpp>

#include <limits>
#include <optional>
#include <ranges>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <IO/Operators.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>
#include <fmt/ranges.h>

namespace DB
{

namespace Setting
{
    extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;
    extern const SettingsBool short_circuit_function_evaluation_reorder_arguments;
}

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
    std::vector<size_t> pre_erase_positions;
    pre_erase_positions.reserve(unneeded_output_positions.size());
    for (size_t pos : unneeded_output_positions)
        pre_erase_positions.push_back(remove_filter_column && pos >= filter_col_pre_erase_pos ? pos + 1 : pos);

    /// The positions ascend, so the unneeded DAG outputs are a prefix of them, and the rest are pass-through columns.
    const size_t first_unneeded_passthrough
        = std::ranges::lower_bound(pre_erase_positions, old_dag_outputs_size) - pre_erase_positions.begin();
    std::vector<size_t> unneeded_dag_indices(pre_erase_positions.begin(), pre_erase_positions.begin() + first_unneeded_passthrough);

    /// Nobody reads the filter column any more, so it can be removed from the header.
    if (!plan.remove_filter_column && std::ranges::binary_search(unneeded_dag_indices, filter_col_pre_erase_pos))
        plan.remove_filter_column = true;

    /// The filter column is needed to filter, whether or not anyone reads it.
    std::erase(unneeded_dag_indices, filter_col_pre_erase_pos);
    plan.unneeded_dag_positions = std::move(unneeded_dag_indices);

    plan.changes_output_header = !plan.unneeded_dag_positions.empty() || remove_filter_column != plan.remove_filter_column;

    /// What removeUnusedActions would keep once the outputs are pruned. It folds constants before it
    /// collects the nodes to keep, and folding clears the children of a folded node, so stop at such a
    /// node to see the same set. ARRAY_JOIN is always a root there.
    auto roots = plan.neededDAGOutputs(old_outputs);
    for (const auto & node : dag.getNodes())
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            roots.push_back(&node);

    const auto is_folded_constant = [](const ActionsDAG::Node * node) { return node->column && !node->children.empty(); };
    const auto surviving_nodes = findReachableNodes(roots, is_folded_constant);

    /// One entry per column of the input header: the input reading it, or nothing when it passes by.
    const auto header_columns = mapHeaderColumnsToInputs(dag.getInputs(), input_header);
    plan.input_columns.resize(header_columns.size());

    /// The pass-through columns follow the DAG outputs in the pre-erase header, in their order, so one walk over
    /// the input header pairs them up with the unneeded positions.
    size_t passthrough_position = old_dag_outputs_size;
    size_t next_unneeded_passthrough = first_unneeded_passthrough;

    /// A dropped column the child keeps is consumed by a new input, and inputs are paired with header columns by name,
    /// in order. So a pass-through column after a needed one of the same name cannot be consumed, and is kept.
    std::unordered_set<std::string_view> needed_passthrough_names;
    std::vector<size_t> kept_unneeded_positions;
    for (size_t position = 0; position < header_columns.size(); ++position)
    {
        if (!header_columns.passesThrough(position))
        {
            /// Every input reads a header position of its own, so the column it reads is needed exactly when
            /// the input survives.
            const auto * input = dag.getInputs()[header_columns.read_by[position]];
            plan.input_columns[position] = surviving_nodes.contains(input) ? InputColumnUsage::ReadNeeded : InputColumnUsage::ReadDropped;
            continue;
        }

        bool is_unneeded = next_unneeded_passthrough < pre_erase_positions.size()
            && pre_erase_positions[next_unneeded_passthrough] == passthrough_position;
        if (is_unneeded)
            ++next_unneeded_passthrough;

        /// The filter column comes before the pass-through columns, so the caller counts them one lower when it is erased.
        const auto & name = input_header.getByPosition(position).name;
        if (is_unneeded && needed_passthrough_names.contains(name))
        {
            is_unneeded = false;
            kept_unneeded_positions.push_back(remove_filter_column ? passthrough_position - 1 : passthrough_position);
        }

        if (!is_unneeded)
            needed_passthrough_names.insert(name);

        plan.input_columns[position] = is_unneeded ? InputColumnUsage::PassesThroughDropped : InputColumnUsage::PassesThroughNeeded;
        ++passthrough_position;
    }

    if (next_unneeded_passthrough != pre_erase_positions.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unneeded output position {} is out of range for pass-through inputs",
            pre_erase_positions[next_unneeded_passthrough]);

    plan.dropped_output_positions = unneeded_output_positions;
    std::erase_if(plan.dropped_output_positions, [&](size_t position) { return std::ranges::binary_search(kept_unneeded_positions, position); });

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

std::vector<size_t> FilterStep::UnneededColumnsPlan::unneededInputPositions() const
{
    std::vector<size_t> positions;
    for (size_t position = 0; position < input_columns.size(); ++position)
        if (input_columns[position] == InputColumnUsage::ReadDropped || input_columns[position] == InputColumnUsage::PassesThroughDropped)
            positions.push_back(position);

    return positions;
}

FilterDAGOutputPruningResult FilterStep::UnneededColumnsPlan::toResult(bool removed_any_action) const
{
    FilterDAGOutputPruningResult result;
    result.unneeded_input_positions = unneededInputPositions();
    result.changed = changes_output_header || removed_any_action || !result.unneeded_input_positions.empty();
    return result;
}

void FilterStep::UnneededColumnsPlan::applyToOutputs(ActionsDAG & dag, bool & remove_filter_column_) const
{
    dag.getOutputs() = neededDAGOutputs(dag.getOutputs());
    remove_filter_column_ = remove_filter_column;
}

FilterDAGOutputPruningResult FilterStep::pruneDAGOutputsByPosition(
    ActionsDAG & dag,
    const String & filter_column_name,
    bool & remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & unneeded_output_positions)
{
    const auto plan = analyzeUnneededColumns(dag, filter_column_name, remove_filter_column, input_header, unneeded_output_positions);

    plan.applyToOutputs(dag, remove_filter_column);
    const bool removed_any_action = dag.removeUnusedActions();
    return plan.toResult(removed_any_action);
}

std::vector<size_t> FilterStep::getUnneededInputPositions(
    const ActionsDAG & dag,
    const String & filter_column_name,
    bool remove_filter_column,
    const Block & input_header,
    const std::vector<size_t> & unneeded_output_positions)
{
    return analyzeUnneededColumns(dag, filter_column_name, remove_filter_column, input_header, unneeded_output_positions)
        .unneededInputPositions();
}

std::vector<std::optional<size_t>> FilterStep::mapOutputsToInputs(
    const ActionsDAG & dag, const String & filter_column_name, bool remove_filter_column, const Block & input_header)
{
    const auto & inputs = dag.getInputs();
    const auto header_columns = mapHeaderColumnsToInputs(inputs, input_header);

    std::unordered_map<const ActionsDAG::Node *, size_t> header_position_of_input;
    for (size_t position = 0; position < header_columns.size(); ++position)
        if (!header_columns.passesThrough(position))
            header_position_of_input.emplace(inputs[header_columns.read_by[position]], position);

    /// The output header is the outputs of the DAG, without the first filter column if it is removed, then the columns
    /// the filter passes through, in their order.
    std::vector<std::optional<size_t>> result;
    bool filter_column_erased = !remove_filter_column;
    for (const auto * output : dag.getOutputs())
    {
        if (!filter_column_erased && output->result_name == filter_column_name)
        {
            filter_column_erased = true;
            continue;
        }

        const auto it = header_position_of_input.find(output);
        result.push_back(it == header_position_of_input.end() ? std::nullopt : std::optional<size_t>(it->second));
    }

    for (size_t position = 0; position < header_columns.size(); ++position)
        if (header_columns.passesThrough(position))
            result.push_back(position);

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

/// Can the atom be evaluated before the atoms that precede it in the AND chain? It is evaluated on more rows then,
/// so it must be cheap (no functions that are worth to execute lazily) and all its functions must be unable to throw,
/// be stateless, have no observable side effects and be deterministic in the scope of the query. See setting `short_circuit_function_evaluation_reorder_arguments`.
static bool canBeMovedToTheFrontOfAndChain(const ActionsDAG::Node * node)
{
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(node);
    while (!nodes.empty())
    {
        node = nodes.top();
        nodes.pop();

        if (node->type == ActionsDAG::ActionType::ARRAY_JOIN || node->type == ActionsDAG::ActionType::PLACEHOLDER)
            return false;

        if (node->type == ActionsDAG::ActionType::FUNCTION && !node->column)
        {
            DataTypesWithConstInfo arguments;
            arguments.reserve(node->children.size());
            for (const auto * child : node->children)
                arguments.push_back({child->result_type, child->column != nullptr});

            const auto & function = *node->function_base;
            if (function.isStateful()
                || function.hasObservableSideEffects()
                || !function.isDeterministicInScopeOfQuery()
                || function.isSuitableForShortCircuitArgumentsExecution(arguments)
                || function.canThrow(arguments))
                return false;
        }

        for (const auto * child : node->children)
            nodes.push(child);
    }
    return true;
}

/// Is the atom heavy, that is, does it contain functions that are worth to execute lazily (like `JSONExtractString`)?
/// Only heavy atoms are worth to evaluate on fewer rows, so a cheap atom is moved only in front of heavy ones.
static bool isHeavyAndChainAtom(const ActionsDAG::Node * node)
{
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(node);
    while (!nodes.empty())
    {
        node = nodes.top();
        nodes.pop();

        if (node->type == ActionsDAG::ActionType::FUNCTION && !node->column)
        {
            DataTypesWithConstInfo arguments;
            arguments.reserve(node->children.size());
            for (const auto * child : node->children)
                arguments.push_back({child->result_type, child->column != nullptr});

            if (node->function_base->isSuitableForShortCircuitArgumentsExecution(arguments))
                return true;
        }

        for (const auto * child : node->children)
            nodes.push(child);
    }
    return false;
}

/// Does the atom contain stateful functions, functions with observable side effects (like `sleep`),
/// or functions that are not deterministic in the scope of the query (like `rand` or `rowNumberInBlock`)?
/// Such an atom must not be evaluated on fewer rows than in the original order, so no atom can be moved in front of it.
static bool hasOrderSensitiveFunctions(const ActionsDAG::Node * node)
{
    std::stack<const ActionsDAG::Node *> nodes;
    nodes.push(node);
    while (!nodes.empty())
    {
        node = nodes.top();
        nodes.pop();

        if (node->type == ActionsDAG::ActionType::FUNCTION
            && (node->function_base->isStateful()
                || node->function_base->hasObservableSideEffects()
                || !node->function_base->isDeterministicInScopeOfQuery()))
            return true;

        for (const auto * child : node->children)
            nodes.push(child);
    }
    return false;
}

/// Try to split the left most AND atom to a separate DAG.
/// With `reorder`, the left most atom that can be moved to the front of the chain is preferred.
static std::optional<ActionsAndName> trySplitSingleAndFilter(ActionsDAG & dag, const std::string & filter_name, bool reorder)
{
    const auto * filter = &dag.findInOutputs(filter_name);
    while (filter->type == ActionsDAG::ActionType::ALIAS)
        filter = filter->children.at(0);

    if (filter->type != ActionsDAG::ActionType::FUNCTION || filter->function_base->getName() != "and")
        return {};

    /// Non-trivial atoms from left to right.
    std::vector<const ActionsDAG::Node *> atoms;
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

        if (!isTrivialSubtree(node))
            atoms.push_back(node);
    }

    /// Do not split subtree if it's the last non-trivial one.
    if (atoms.size() < 2)
        return {};

    /// A heavy atom is evaluated only on the rows that passed the atoms split before it. If a cheap atom follows it,
    /// split the cheap one first. The atoms that are not moved keep their relative order.
    /// A cheap atom is never moved in front of another cheap atom: it does not save anything, while the written order
    /// may put the more selective condition first, as in `notEmpty(s) AND s < 'x'`.
    /// This is decided once per pipeline, not per block, so the profile event `ShortCircuitArgumentsReordered` is not incremented here.
    if (reorder)
    {
        for (const auto * atom : atoms)
        {
            if (canBeMovedToTheFrontOfAndChain(atom))
                return splitSingleAndFilter(dag, atom);
            if (!isHeavyAndChainAtom(atom) || hasOrderSensitiveFunctions(atom))
                break;
        }
    }

    return splitSingleAndFilter(dag, atoms.front());
}

static std::vector<ActionsAndName> splitAndChainIntoMultipleFilters(ActionsDAG & dag, const std::string & filter_name, bool reorder)
{
    std::vector<ActionsAndName> res;

    while (auto condition = trySplitSingleAndFilter(dag, filter_name, reorder))
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
    /// Fold a `materialize`-wrapped constant predicate away (#78166), such as the one `tryMergeExpressions` drags
    /// in from the branches of a `UNION`. Only the value of the predicate decides which rows pass, so the filter
    /// reads a constant; a kept filter column stays an output as it was, and removing unused columns drops it once
    /// nobody reads it. Doing it here covers every way a filter step comes to be, in particular the fresh steps
    /// `tryPushDownFilter` creates. The output header does not change: a kept column stays, and the constant is
    /// removed.
    actions_dag.foldFilterPredicateThroughMaterialize(filter_column_name, remove_filter_column, *input_header_);

    actions_dag.removeAliasesForFilter(filter_column_name);
    /// Removing aliases may result in unneeded ALIAS node in DAG.
    /// This should not be an issue by itself,
    /// but it might trigger an issue with duplicated names in Block after plan optimizations.
    actions_dag.removeUnusedActions(false, false);
}

void FilterStep::setPrefilteredConditions(UInt64 token, std::unordered_set<UInt64> condition_hashes)
{
    prefiltered_conditions = PrefilteredConditions{.token = token, .condition_hashes = std::move(condition_hashes)};
}

/// The atoms of the AND chain of `node` from left to right, the order of short-circuit evaluation, and its `and` nodes.
static void collectConjunctionAtoms(
    const ActionsDAG::Node * node, ActionsDAG::NodeRawConstPtrs & atoms, std::unordered_set<const ActionsDAG::Node *> & and_nodes)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "and")
    {
        and_nodes.insert(node);
        for (const auto * child : node->children)
            collectConjunctionAtoms(child, atoms, and_nodes);
        return;
    }
    atoms.push_back(node);
}

/// A filter split in two: the leading atoms of its AND chain that the rows of the prefiltered chunks already passed,
/// and the rest. Applied one after the other, they filter the same rows and give the same columns as the filter.
struct PrefilteredConditionsSplit
{
    /// Filters by the conjunction of the atoms, and passes the input columns through.
    ActionsDAG conditions;
    /// The same columns as `conditions`, without evaluating them: for the prefiltered chunks.
    ActionsDAG skipped_conditions;
    /// The actions of the filter, with the conjunction of the other atoms as the filter column.
    ActionsDAG rest;
};

static constexpr auto prefiltered_conditions_column_name = "__prefiltered_conditions";

/// Conjunction of `atoms` in `dag` as a new node: `and` also converts a single atom to the truth value of a conjunction.
static const ActionsDAG::Node & addConjunction(ActionsDAG & dag, ActionsDAG::NodeRawConstPtrs atoms)
{
    auto true_type = std::make_shared<DataTypeUInt8>();
    const auto & true_node = dag.addColumn(true_type->createColumnConst(1, Field(UInt64(1))), true_type, "__prefiltered_true");
    if (atoms.empty())
        return true_node;
    if (atoms.size() == 1)
        atoms.push_back(&true_node);
    FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    return dag.addFunction(func_builder_and, std::move(atoms), {});
}

/// Whether the blocks have the same columns, with the same constness.
static bool haveSameStructureAndConstness(const Block & lhs, const Block & rhs)
{
    if (!blocksHaveEqualStructure(lhs, rhs))
        return false;
    auto is_const = [](const ColumnWithTypeAndName & column) { return column.column && isColumnConst(*column.column); };
    for (size_t i = 0; i < lhs.columns(); ++i)
        if (is_const(lhs.getByPosition(i)) != is_const(rhs.getByPosition(i)))
            return false;
    return true;
}

/// Splits off the leading atoms of the AND chain of the filter with `condition_hashes`. Only if the atoms are used only by
/// the AND chain: otherwise the rest still needs their values. Not for stateful functions: the rest would see fewer rows.
static std::optional<PrefilteredConditionsSplit> splitPrefilteredConditions(
    const ActionsDAG & dag,
    const String & filter_column_name,
    bool remove_filter_column,
    const std::unordered_set<UInt64> & condition_hashes,
    const Block & input_header)
{
    const auto & outputs = dag.getOutputs();
    if (dag.hasStatefulFunctions()
        || std::ranges::count_if(outputs, [&](const auto * node) { return node->result_name == filter_column_name; }) != 1)
        return std::nullopt;

    const auto * filter_node = &dag.findInOutputs(filter_column_name);
    ActionsDAG::NodeRawConstPtrs atoms;
    std::unordered_set<const ActionsDAG::Node *> and_nodes;
    collectConjunctionAtoms(filter_node, atoms, and_nodes);

    size_t num_prefiltered = 0;
    while (num_prefiltered < atoms.size() && atoms[num_prefiltered]->type == ActionsDAG::ActionType::FUNCTION
           && condition_hashes.contains(atoms[num_prefiltered]->getHash()))
        ++num_prefiltered;
    if (num_prefiltered == 0)
        return std::nullopt;

    const ActionsDAG::NodeRawConstPtrs prefiltered_atoms(atoms.begin(), atoms.begin() + num_prefiltered);
    const std::unordered_set<const ActionsDAG::Node *> prefiltered_set(prefiltered_atoms.begin(), prefiltered_atoms.end());
    for (const auto * output : outputs)
        if (prefiltered_set.contains(output) && !(output == filter_node && remove_filter_column))
            return std::nullopt;
    for (const auto & node : dag.getNodes())
        if (!and_nodes.contains(&node))
            for (const auto * child : node.children)
                if (prefiltered_set.contains(child))
                    return std::nullopt;

    /// The conditions, with all input columns passed through in the order of the header.
    ActionsDAG conditions(input_header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs condition_nodes;
    conditions.mergeNodes(ActionsDAG::cloneSubDAG(prefiltered_atoms, /*remove_aliases=*/ false), &condition_nodes);
    conditions.getOutputs().insert(
        conditions.getOutputs().begin(), &conditions.addAlias(addConjunction(conditions, condition_nodes), prefiltered_conditions_column_name));
    conditions.removeUnusedActions(false, false);

    ActionsDAG skipped_conditions(input_header.getColumnsWithTypeAndName());
    skipped_conditions.getOutputs().insert(
        skipped_conditions.getOutputs().begin(), &skipped_conditions.addAlias(addConjunction(skipped_conditions, {}), prefiltered_conditions_column_name));

    /// The filter column keeps its name and its position among the outputs: `FilterTransform` finds it by position.
    ActionsDAG::NodeMapping copy_map;
    ActionsDAG rest = dag.clone(copy_map);
    ActionsDAG::NodeRawConstPtrs rest_atoms;
    for (size_t i = num_prefiltered; i < atoms.size(); ++i)
        rest_atoms.push_back(copy_map.at(atoms[i]));
    auto & rest_outputs = rest.getOutputs();
    auto rest_filter_it = std::ranges::find(rest_outputs, copy_map.at(filter_node));
    *rest_filter_it = &rest.addAlias(addConjunction(rest, std::move(rest_atoms)), filter_column_name);
    rest.removeUnusedActions(false, false);

    /// The two filters one after the other must give the same columns as the filter.
    const Block conditions_header = FilterTransform::transformHeader(input_header, &conditions, prefiltered_conditions_column_name, true);
    if (!haveSameStructureAndConstness(conditions_header, input_header)
        || !haveSameStructureAndConstness(
            FilterTransform::transformHeader(input_header, &skipped_conditions, prefiltered_conditions_column_name, true), input_header)
        || !haveSameStructureAndConstness(
            FilterTransform::transformHeader(conditions_header, &rest, filter_column_name, remove_filter_column),
            FilterTransform::transformHeader(input_header, &dag, filter_column_name, remove_filter_column)))
        return std::nullopt;

    return PrefilteredConditionsSplit{std::move(conditions), std::move(skipped_conditions), std::move(rest)};
}

void FilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    std::vector<ActionsAndName> and_atoms;

    /// The chunks of the rows that already passed the leading conditions skip them (see `setPrefilteredConditions`).
    if (prefiltered_conditions)
    {
        if (auto split = splitPrefilteredConditions(
                actions_dag, filter_column_name, remove_filter_column, prefiltered_conditions->condition_hashes, pipeline.getHeader()))
        {
            auto expression = std::make_shared<ExpressionActions>(std::move(split->conditions), settings.getActionsSettings());
            auto skipped_expression = std::make_shared<ExpressionActions>(std::move(split->skipped_conditions), settings.getActionsSettings());
            auto transformed_header = std::make_shared<const Block>(expression->getActionsDAG().updateHeader(pipeline.getHeader()));
            pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
            {
                bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
                auto transform = std::make_shared<FilterTransform>(
                    header, transformed_header, expression, prefiltered_conditions_column_name, true, on_totals, nullptr, condition);
                transform->setPrefilteredExpression(skipped_expression, prefiltered_conditions->token);
                return transform;
            });
            actions_dag = std::move(split->rest);
        }
    }

    /// Splitting AND filter condition to steps under the setting, which is enabled with merge_filters optimization.
    /// This is needed to support short-circuit properly.
    /// The atoms are reordered only with short-circuit evaluation, like the arguments of `and`.
    if (settings.enable_multiple_filters_transforms_for_and_chain && !actions_dag.hasStatefulFunctions())
    {
        const auto & actions_settings = settings.getActionsSettings();
        bool reorder = actions_settings.short_circuit_function_evaluation != ShortCircuitFunctionEvaluation::DISABLE
            && actions_settings.short_circuit_function_evaluation_reorder_arguments;
        and_atoms = splitAndChainIntoMultipleFilters(actions_dag, filter_column_name, reorder);
    }

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

/// Whether the AND chain atoms are reordered (`short_circuit_function_evaluation_reorder_arguments` with short-circuit evaluation enabled), to show the AND chain split as it is executed.
/// The pipeline is not built for `EXPLAIN`, so the setting is taken from the query context. Without a query context
/// (the plan is described outside of a query), the default value of the setting is used.
static bool reorderAndChainForDescription()
{
    auto reorder = [](const Settings & query_settings)
    {
        return query_settings[Setting::short_circuit_function_evaluation] != ShortCircuitFunctionEvaluation::DISABLE
            && query_settings[Setting::short_circuit_function_evaluation_reorder_arguments];
    };
    if (auto query_context = CurrentThread::tryGetQueryContext())
        return reorder(query_context->getSettingsRef());
    return reorder(Settings());
}

void FilterStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    auto cloned_dag = actions_dag.clone();

    std::vector<ActionsAndName> and_atoms;
    if (!settings.pretty && !actions_dag.hasStatefulFunctions())
        and_atoms = splitAndChainIntoMultipleFilters(cloned_dag, filter_column_name, reorderAndChainForDescription());

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
        and_atoms = splitAndChainIntoMultipleFilters(cloned_dag, filter_column_name, reorderAndChainForDescription());

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

void FilterStep::saltConditionForQueryConditionCache(UInt64 salt)
{
    if (!condition)
        return;

    /// `size_t` (not `UInt64`) so `boost::hash_combine` binds on every platform, same as in `updateQueryConditionCache`.
    size_t condition_hash = condition->first;
    boost::hash_combine(condition_hash, salt);
    condition->first = condition_hash;
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

    /// A cache key must not depend on the name this plan build composed for the filter column
    /// (`greater(__table1.x, 5_UInt8)` here, `greater(__table2.x, 5_UInt8)` in the plan enclosing a
    /// shipped fragment). What the predicate computes says the same thing build-independently, and
    /// unlike an output index it does not move when the two builds order their outputs differently.
    if (ctx.for_cache_key)
        writeIntBinary(actions_dag.getOutputIdentity(filter_column_name, ctx.input_header), ctx.out);
    else
        writeStringBinary(filter_column_name, ctx.out);

    actions_dag.serialize(ctx.out, ctx.registry, ctx.input_header);
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
    return {analyzeUnneededColumns(unneeded_output_positions).unneededInputPositions()};
}

FilterStep::RemoveUnusedColumnsResult
FilterStep::removeUnusedColumns(const std::vector<size_t> & unneeded_output_positions, const std::vector<PrunedInput> & inputs)
{
    const auto plan = analyzeUnneededColumns(unneeded_output_positions);
    const auto & pruned = inputs.at(0);
    const auto input_header = input_headers.front();

    plan.applyToOutputs(actions_dag, remove_filter_column);

    RemoveUnusedColumnsResult result;
    result.dropped_output_positions = plan.dropped_output_positions;

    const bool dag_changed = alignInputsWithPrunedChild(actions_dag, plan.input_columns, *input_header, pruned);
    result.step_changed = plan.changes_output_header || dag_changed
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
