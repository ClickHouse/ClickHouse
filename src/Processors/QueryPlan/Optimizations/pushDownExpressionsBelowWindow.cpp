#include <Core/Block.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace DB::QueryPlanOptimizations
{

namespace
{

using Node = ActionsDAG::Node;
using NodeSet = std::unordered_set<const Node *>;

const Node * resolveAliases(const Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();

    return node;
}

/// A lower bound of the number of bytes a value of the column takes, for the types the optimization supports.
/// As in `pushDownVolumeReducingFunctions`, composite types are not supported: a path of a composite type can share
/// its payload with other expressions under a different nested column name.
std::optional<size_t> getMinimumSizeOfValue(const DataTypePtr & type)
{
    if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(type.get()))
        return fixed_string->getN();

    /// The offset of the value.
    if (typeid_cast<const DataTypeString *>(type.get()))
        return sizeof(UInt64);

    return {};
}

void collectSubtree(const Node * root, NodeSet & nodes)
{
    std::vector<const Node *> stack{root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (nodes.insert(node).second)
            stack.insert(stack.end(), node->children.begin(), node->children.end());
    }
}

/// The nodes that may be executed lazily, only for some rows: the arguments of short-circuit functions (`if`,
/// `multiIf`, `and`, `or`, ...) except the ones that are always executed, and everything they depend on.
/// Computing such a node in another step would execute it for all rows, which can throw (`toUInt64(s)` in
/// `if(count() OVER () = 0, toUInt64(s), 0)`) or be slow.
NodeSet collectMaybeLazilyExecutedNodes(const ActionsDAG & actions)
{
    NodeSet result;
    for (const auto & node : actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
            continue;

        IFunctionBase::ShortCircuitSettings short_circuit_settings;
        if (!node.function_base->isShortCircuit(short_circuit_settings, node.children.size()))
            continue;

        for (size_t i = 0; i < node.children.size(); ++i)
        {
            /// For commutative functions (`and`, `or`) any argument may be executed lazily.
            if (short_circuit_settings.arguments_are_commutative || short_circuit_settings.force_enable_lazy_execution
                || !short_circuit_settings.arguments_with_disabled_lazy_execution.contains(i))
                collectSubtree(node.children[i], result);
        }
    }

    return result;
}

/// Whether the expression can be computed in another step, before the window functions: every function is
/// evaluated row by row (not stateful like `runningDifference`, not depending on blocks like `blockSize`, without
/// side effects like `sleepEachRow`), and the only column it reads is a single input. The window and sorting steps
/// keep every row and only change their order and split into blocks, so such an expression has the same value
/// on either side of them.
struct ExpressionInfo
{
    bool can_be_moved = false;
    /// The single input of the expression, if any.
    const Node * input = nullptr;
};

const ExpressionInfo & getExpressionInfo(const Node * node, std::unordered_map<const Node *, ExpressionInfo> & cache)
{
    if (auto it = cache.find(node); it != cache.end())
        return it->second;

    ExpressionInfo info;
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
            info = {.can_be_moved = true, .input = node};
            break;
        case ActionsDAG::ActionType::COLUMN:
            /// A prepared set is built by the steps the planner adds for the step that uses it.
            info.can_be_moved = !WhichDataType(node->result_type).isSet();
            break;
        case ActionsDAG::ActionType::ALIAS:
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (node->type == ActionsDAG::ActionType::FUNCTION
                && (!node->function_base || node->function_base->isStateful() || !node->function_base->isDeterministic()
                    || !node->function_base->isDeterministicInScopeOfQuery() || node->function_base->hasObservableSideEffects()))
                break;

            info.can_be_moved = true;
            for (const auto * child : node->children)
            {
                const auto & child_info = getExpressionInfo(child, cache);
                if (!child_info.can_be_moved || (child_info.input && info.input && child_info.input != info.input))
                {
                    info = {};
                    break;
                }

                if (child_info.input)
                    info.input = child_info.input;
            }
            break;
        }
        default:
            break;
    }

    return cache.emplace(node, info).first->second;
}

/// Whether the column `column_name` of the output of `node` does not share memory with another column of it.
/// An expression can return its argument as is (an identity `CAST`, `toNullable`, an alias), so two columns can
/// share memory only if they are computed from a common column. The steps that are not known to keep
/// the columns separate are not accepted.
bool hasOwnMemory(const QueryPlan::Node & node, const String & column_name, size_t depth = 0)
{
    static constexpr size_t max_depth = 100;
    if (depth > max_depth)
        return false;

    const auto * step = node.step.get();

    const ActionsDAG * actions = nullptr;
    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
        actions = &expression->getExpression();
    else if (const auto * filter = typeid_cast<const FilterStep *>(step))
        actions = &filter->getExpression();

    if (actions)
    {
        const Node * column_output = nullptr;
        for (const auto * output : actions->getOutputs())
        {
            if (output->result_name != column_name)
                continue;

            if (column_output)
                return false;
            column_output = output;
        }

        if (!column_output)
            return false;

        NodeSet column_nodes;
        collectSubtree(resolveAliases(column_output), column_nodes);
        std::erase_if(column_nodes, [](const Node * column_node) { return column_node->type == ActionsDAG::ActionType::COLUMN; });

        for (const auto * output : actions->getOutputs())
        {
            if (output == column_output)
                continue;

            NodeSet output_nodes;
            collectSubtree(output, output_nodes);
            for (const auto * output_node : output_nodes)
                if (column_nodes.contains(output_node))
                    return false;
        }

        if (node.children.size() != 1)
            return false;

        for (const auto * column_node : column_nodes)
            if (column_node->type == ActionsDAG::ActionType::INPUT && !hasOwnMemory(*node.children.front(), column_node->result_name, depth + 1))
                return false;

        return true;
    }

    if (typeid_cast<const ISourceStep *>(step) || typeid_cast<const AggregatingStep *>(step) || typeid_cast<const MergingAggregatedStep *>(step))
        return true;

    if (typeid_cast<const UnionStep *>(step))
    {
        for (const auto * child : node.children)
            if (!hasOwnMemory(*child, column_name, depth + 1))
                return false;
        return true;
    }

    /// Steps like `Limit` and `Offset` pass the columns through.
    const auto & input_headers = step->getInputHeaders();
    if (node.children.size() == 1 && input_headers.size() == 1 && blocksHaveEqualStructure(*input_headers.front(), *step->getOutputHeader()))
        return hasOwnMemory(*node.children.front(), column_name, depth + 1);

    return false;
}

/// Whether a step above may stop reading the result before all rows are read, so that the projection after
/// the window functions is computed only for some rows: `LIMIT`, `LIMIT ... WITH TIES`, `LIMIT AFTER / UNTIL`,
/// `ORDER BY ... LIMIT` (`liftUpFunctions` computes the expressions after such a sorting).
/// An aggregation reads all rows, so the steps above it do not matter.
bool mayStopBeforeReadingAllRows(const Stack & stack)
{
    for (auto it = stack.rbegin() + 1; it != stack.rend(); ++it)
    {
        const auto * step = it->node->step.get();
        if (typeid_cast<const AggregatingStep *>(step) || typeid_cast<const MergingAggregatedStep *>(step))
            return false;

        if (typeid_cast<const LimitStep *>(step) || typeid_cast<const LimitRangeStep *>(step))
            return true;

        if (const auto * sorting = typeid_cast<const SortingStep *>(step); sorting && sorting->getLimit())
            return true;
    }

    return false;
}

void addColumnNames(const SortDescription & description, NameSet & names)
{
    for (const auto & column : description)
        names.insert(column.column_name);
}

}

void tryPushDownExpressionsBelowWindow(const Stack & stack, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto * parent_node = stack.back().node;
    auto * expression_step = typeid_cast<ExpressionStep *>(parent_node->step.get());
    if (!expression_step || parent_node->children.size() != 1)
        return;

    auto * window_node = parent_node->children.front();
    if (!typeid_cast<const WindowStep *>(window_node->step.get()))
        return;

    auto & parent_actions = expression_step->getExpression();
    if (parent_actions.hasArrayJoin())
        return;

    /// The rewrite resolves columns by name, which is ambiguous with duplicated names.
    NameSet input_names;
    for (const auto * input : parent_actions.getInputs())
        if (!input_names.insert(input->result_name).second)
            return;

    if (mayStopBeforeReadingAllRows(stack))
        return;

    /// The chain of window steps and of the sorting steps for them. The expressions are computed below it,
    /// so that none of them buffers the columns the expressions read.
    std::vector<QueryPlan::Node *> chain;
    NameSet columns_read_by_chain;
    for (auto * node = window_node; node->children.size() == 1; node = node->children.front())
    {
        if (const auto * window = typeid_cast<const WindowStep *>(node->step.get()))
        {
            for (const auto & window_function : window->getWindowFunctions())
                columns_read_by_chain.insert(window_function.argument_names.begin(), window_function.argument_names.end());

            const auto & window_description = window->getWindowDescription();
            addColumnNames(window_description.full_sort_description, columns_read_by_chain);
            for (const auto * window_actions : {&window_description.partition_by_actions, &window_description.order_by_actions})
                for (const auto & actions : *window_actions)
                    if (actions)
                        for (const auto & name : actions->getRequiredColumnsNames())
                            columns_read_by_chain.insert(name);
        }
        else if (const auto * sorting = typeid_cast<const SortingStep *>(node->step.get()); sorting && !sorting->hasPartitions() && !sorting->getLimit())
        {
            addColumnNames(sorting->getSortDescription(), columns_read_by_chain);
        }
        else
        {
            break;
        }

        chain.push_back(node);
    }

    if (chain.empty() || chain.back()->children.size() != 1)
        return;

    QueryPlan::Node * bottom_node = chain.back()->children.front();
    const Block & bottom_header = *bottom_node->step->getOutputHeader();
    const Block & window_header = *window_node->step->getOutputHeader();

    std::unordered_map<std::string_view, size_t> bottom_column_counts;
    for (const auto & column : bottom_header)
        ++bottom_column_counts[column.name];

    /// The largest expressions that can be computed below the chain, grouped by the column they read.
    const auto maybe_lazily_executed_nodes = collectMaybeLazilyExecutedNodes(parent_actions);
    std::unordered_map<const Node *, ExpressionInfo> expression_info_cache;
    std::unordered_map<const Node *, const Node *> input_of_candidate;
    for (const auto & node : parent_actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || maybe_lazily_executed_nodes.contains(&node))
            continue;

        const auto & info = getExpressionInfo(&node, expression_info_cache);
        if (!info.can_be_moved || !info.input)
            continue;

        auto minimum_size_of_input = getMinimumSizeOfValue(info.input->result_type);
        if (!minimum_size_of_input || !node.result_type->haveMaximumSizeOfValue()
            || node.result_type->getMaximumSizeOfValueInMemory() > *minimum_size_of_input)
            continue;

        input_of_candidate.emplace(&node, info.input);
    }

    if (input_of_candidate.empty())
        return;

    NodeSet nodes_below_candidates;
    for (const auto & [candidate, _] : input_of_candidate)
        for (const auto * child : candidate->children)
            collectSubtree(child, nodes_below_candidates);

    std::unordered_map<const Node *, std::vector<const Node *>> candidates_by_input;
    for (const auto & [candidate, input] : input_of_candidate)
        if (!nodes_below_candidates.contains(candidate))
            candidates_by_input[input].push_back(candidate);

    NodeSet outputs;
    for (const auto * output : parent_actions.getOutputs())
        outputs.insert(output);

    NodeSet split_nodes;
    NameSet replaced_columns;
    NameSet result_names;
    for (const auto & [input, candidates] : candidates_by_input)
    {
        const auto & column_name = input->result_name;
        if (columns_read_by_chain.contains(column_name) || bottom_column_counts[column_name] != 1)
            continue;

        const auto * bottom_column = bottom_header.findByName(column_name);
        if (!bottom_column || !bottom_column->type->equals(*input->result_type))
            continue;

        /// The column is freed only if nothing but the candidates reads it. An internal node of a candidate
        /// is not allowed to be read from outside either, otherwise it would be computed below the chain and
        /// carried through it. Constants are not carried, so they can be shared.
        NodeSet candidate_nodes;
        for (const auto * candidate : candidates)
            collectSubtree(candidate, candidate_nodes);
        std::erase_if(candidate_nodes, [](const Node * node) { return node->type == ActionsDAG::ActionType::COLUMN; });

        NodeSet candidate_roots(candidates.begin(), candidates.end());
        bool is_read_by_others = false;
        for (const auto & node : parent_actions.getNodes())
        {
            if (candidate_nodes.contains(&node))
            {
                if (!candidate_roots.contains(&node) && outputs.contains(&node))
                    is_read_by_others = true;
                continue;
            }

            for (const auto * child : node.children)
                if (candidate_nodes.contains(child) && !candidate_roots.contains(child))
                    is_read_by_others = true;
        }

        if (is_read_by_others)
            continue;

        /// The results must not be larger than the smallest possible value of the column.
        size_t size_of_results = 0;
        bool has_name_conflicts = false;
        for (const auto * candidate : candidates)
        {
            size_of_results += candidate->result_type->getMaximumSizeOfValueInMemory();
            has_name_conflicts |= window_header.has(candidate->result_name) || !result_names.insert(candidate->result_name).second;
        }

        if (has_name_conflicts || size_of_results > *getMinimumSizeOfValue(input->result_type))
            continue;

        if (!hasOwnMemory(*bottom_node, column_name))
            continue;

        split_nodes.insert(candidates.begin(), candidates.end());
        replaced_columns.insert(column_name);
    }

    if (split_nodes.empty())
        return;

    /// The first part computes the expressions from the replaced columns, the second part reads their results.
    auto split_result = parent_actions.split(split_nodes);

    ActionsDAG pushed_actions = std::move(split_result.first);
    pushed_actions.appendInputsForUnusedColumns(bottom_header);
    for (const auto * input : pushed_actions.getInputs())
        if (!replaced_columns.contains(input->result_name))
            pushed_actions.getOutputs().push_back(input);

    parent_actions = std::move(split_result.second);

    auto & pushed_node = nodes.emplace_back();
    pushed_node.children = {bottom_node};
    chain.back()->children = {&pushed_node};
    pushed_node.step = std::make_unique<ExpressionStep>(bottom_node->step->getOutputHeader(), std::move(pushed_actions));
    pushed_node.step->setStepDescription("Projection before window functions", settings.max_step_description_length);

    for (auto it = chain.rbegin(); it != chain.rend(); ++it)
        (*it)->step->updateInputHeader((*it)->children.front()->step->getOutputHeader());
    parent_node->step->updateInputHeader(window_node->step->getOutputHeader());

    /// Merging into a `Filter` below would compute the expressions for the rows it removes.
    if (typeid_cast<const ExpressionStep *>(bottom_node->step.get()))
        tryMergeExpressions(&pushed_node, nodes, settings);
}

}
