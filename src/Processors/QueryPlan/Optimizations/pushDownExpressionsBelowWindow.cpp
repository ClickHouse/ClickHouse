#include <Core/Block.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

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

/// A lower bound of the number of bytes a value of the column takes, for the types the optimization supports.
/// As in `pushDownVolumeReducingFunctions`, composite types are not supported: a path of a composite type can share
/// its payload with other expressions under a different nested column name.
/// Whether a column of the type takes `getMaximumSizeOfValueInMemory` bytes per value at most. It is not so for
/// many types with a maximum size of value: `LowCardinality` keeps indexes and a dictionary, `Variant` keeps
/// discriminators and offsets.
bool isStoredInFixedSize(const DataTypePtr & type)
{
    auto nested_type = removeNullable(type);
    /// `LowCardinality(UInt64)` is represented by a number as its dictionary type.
    if (nested_type->lowCardinality())
        return false;

    return nested_type->isValueRepresentedByNumber() || isFixedString(nested_type);
}

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

/// Whether a function with the result of type `result_type` may return its argument of type `argument_type`
/// (or a part of it) as is, without copying, like an identity `CAST`, `toNullable`, `assumeNotNull` or `tupleElement`.
bool mayReturnArgumentAsIs(const DataTypePtr & argument_type, const DataTypePtr & result_type)
{
    auto argument = removeLowCardinalityAndNullable(argument_type);
    if (argument->equals(*removeLowCardinalityAndNullable(result_type)))
        return true;

    /// A part of a composite value, like an element of a tuple or the keys of a map.
    return !argument->isValueRepresentedByNumber() && !isStringOrFixedString(argument);
}

/// The nodes whose results the result of `node` may be: itself, and the arguments that the functions on the way
/// may return as is. Two columns can share memory only if they have a common such node.
void collectStorageNodes(const Node * node, NodeSet & storage_nodes)
{
    std::vector<const Node *> stack{node};
    while (!stack.empty())
    {
        const auto * current = stack.back();
        stack.pop_back();

        /// Constants are not carried through the steps.
        if (current->type == ActionsDAG::ActionType::COLUMN)
            continue;

        if (!storage_nodes.insert(current).second)
            continue;

        for (const auto * child : current->children)
            if (current->type == ActionsDAG::ActionType::ALIAS || mayReturnArgumentAsIs(child->result_type, current->result_type))
                stack.push_back(child);
    }
}

bool hasIdentifier(const ASTPtr & ast, const NameSet & names)
{
    if (const auto * identifier = ast->as<ASTIdentifier>(); identifier && names.contains(identifier->name()))
        return true;

    for (const auto & child : ast->children)
        if (hasIdentifier(child, names))
            return true;

    return false;
}

/// Whether the column `column_name` of a `MergeTree` read does not share memory with another column of it.
/// A column missing in a part is filled with its default expression, which can return another column as is
/// (`t String DEFAULT s`), also through other columns (`v String DEFAULT u`, `u String DEFAULT s`), and
/// the `PREWHERE` and row policy actions can compute columns from the read ones.
bool mergeTreeColumnHasOwnMemory(const ReadFromMergeTree & read, const String & column_name)
{
    const auto & columns = read.getStorageMetadata()->getColumns();
    if (columns.getDefault(column_name))
        return false;

    /// The mutations and patches applied on the fly can make a column the same as another one (`UPDATE t = s`).
    if (const auto & mutations_snapshot = read.getMutationsSnapshot())
    {
        const auto columns_changed_on_fly = mutations_snapshot->getColumnsChangedOnFly();
        for (const auto & column : *read.getOutputHeader())
            if (columns_changed_on_fly.contains(column.name))
                return false;
    }

    /// The columns whose default expressions read the column, directly or through other columns.
    NameSet dependent_columns{column_name};
    for (bool added = true; added;)
    {
        added = false;
        for (const auto & column : columns)
        {
            if (!dependent_columns.contains(column.name) && column.default_desc.expression
                && hasIdentifier(column.default_desc.expression, dependent_columns))
            {
                dependent_columns.insert(column.name);
                added = true;
            }
        }
    }

    for (const auto & column : *read.getOutputHeader())
        if (column.name != column_name && dependent_columns.contains(column.name))
            return false;

    std::vector<const ActionsDAG *> read_actions;
    for (const auto & row_level_filter : {read.getRowLevelFilter(), read.getDeferredRowLevelFilter()})
        if (row_level_filter)
            read_actions.push_back(&row_level_filter->actions);
    for (const auto & prewhere_info : {read.getPrewhereInfo(), read.getDeferredPrewhereInfo()})
        if (prewhere_info)
            read_actions.push_back(&prewhere_info->prewhere_actions);

    for (const auto * actions : read_actions)
    {
        for (const auto * output : actions->getOutputs())
        {
            /// A column computed by the actions is not supported.
            if (output->result_name == column_name)
            {
                if (output->type != ActionsDAG::ActionType::INPUT)
                    return false;
                continue;
            }

            NodeSet output_storage_nodes;
            collectStorageNodes(output, output_storage_nodes);
            for (const auto * output_storage_node : output_storage_nodes)
                if (output_storage_node->type == ActionsDAG::ActionType::INPUT && output_storage_node->result_name == column_name)
                    return false;
        }
    }

    return true;
}

/// Whether the column `column_name` of the output of `node` does not share memory with another column of it.
/// The steps that are not known to keep the columns separate are not accepted.
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

        NodeSet column_storage_nodes;
        collectStorageNodes(column_output, column_storage_nodes);

        for (const auto * output : actions->getOutputs())
        {
            if (output == column_output)
                continue;

            NodeSet output_storage_nodes;
            collectStorageNodes(output, output_storage_nodes);
            for (const auto * output_storage_node : output_storage_nodes)
                if (column_storage_nodes.contains(output_storage_node))
                    return false;
        }

        if (node.children.size() != 1)
            return false;

        /// An input column may share memory with another column of the step below.
        for (const auto * storage_node : column_storage_nodes)
            if (storage_node->type == ActionsDAG::ActionType::INPUT
                && !hasOwnMemory(*node.children.front(), storage_node->result_name, depth + 1))
                return false;

        return true;
    }

    if (const auto * read_from_merge_tree = typeid_cast<const ReadFromMergeTree *>(step))
        return mergeTreeColumnHasOwnMemory(*read_from_merge_tree, column_name);

    /// The reads that create new full columns. Others may return columns that stay in memory anyway, like a `Memory`
    /// table, a materialized CTE or a prepared source, fill the missing columns with default expressions, like `File`,
    /// or return sparse columns, like the reads from other servers, so freeing them may not reduce the memory usage.
    if (typeid_cast<const ReadFromSystemNumbersStep *>(step))
        return true;

    if (typeid_cast<const AggregatingStep *>(step) || typeid_cast<const MergingAggregatedStep *>(step))
        return true;

    if (typeid_cast<const UnionStep *>(step))
    {
        for (const auto * child : node.children)
            if (!hasOwnMemory(*child, column_name, depth + 1))
                return false;
        return true;
    }

    /// A window step adds its results to the columns it passes through.
    if (const auto * window = typeid_cast<const WindowStep *>(step))
    {
        for (const auto & window_function : window->getWindowFunctions())
            if (window_function.column_name == column_name)
                return true;
        return node.children.size() == 1 && hasOwnMemory(*node.children.front(), column_name, depth + 1);
    }

    /// Steps like `Limit` and `Offset` pass the columns through.
    const auto & input_headers = step->getInputHeaders();
    if (node.children.size() == 1 && input_headers.size() == 1 && blocksHaveEqualStructure(*input_headers.front(), *step->getOutputHeader()))
        return hasOwnMemory(*node.children.front(), column_name, depth + 1);

    return false;
}

/// Whether the column `column_name` of the output of `node` may be a sparse or a lazily replicated column, which takes
/// much less than the offset of a `String` per row, while the result of an expression of it can be a full column.
/// A part can store a column in a sparse serialization, and functions keep their results sparse if they map the default
/// value to the default value (`toFixedString(s, 8)`), so all the inputs of the expressions are followed to the reads.
/// An `arrayJoin` can replicate all the columns of the block lazily, not only the ones computed from it.
bool mayBeSparseOrReplicated(const QueryPlan::Node & node, const String & column_name, size_t depth = 0)
{
    static constexpr size_t max_depth = 100;
    if (depth > max_depth)
        return true;

    const auto * step = node.step.get();

    const ActionsDAG * actions = nullptr;
    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
        actions = &expression->getExpression();
    else if (const auto * filter = typeid_cast<const FilterStep *>(step))
        actions = &filter->getExpression();

    if (actions)
    {
        if (actions->hasArrayJoin())
            return true;

        const auto * column_output = actions->tryFindInOutputs(column_name);
        if (!column_output || node.children.size() != 1)
            return true;

        NodeSet column_nodes;
        collectSubtree(column_output, column_nodes);
        for (const auto * column_node : column_nodes)
            if (column_node->type == ActionsDAG::ActionType::INPUT && mayBeSparseOrReplicated(*node.children.front(), column_node->result_name, depth + 1))
                return true;

        return false;
    }

    if (const auto * read = typeid_cast<const ReadFromMergeTree *>(step))
    {
        /// Subcolumns and virtual columns have no serialization info of their own,
        /// and a pending rename makes the parts store the column under another name.
        if (!read->getStorageMetadata()->getColumns().hasPhysical(column_name))
            return true;

        if (const auto & mutations_snapshot = read->getMutationsSnapshot(); mutations_snapshot && mutations_snapshot->hasMetadataMutations())
            return true;

        for (const auto & part : read->getParts())
        {
            const auto & serialization_infos = part.data_part->getSerializationInfos();
            auto it = serialization_infos.find(column_name);
            if (it == serialization_infos.end() || it->second->hasCustomSerialization())
                return true;
        }
        return false;
    }

    if (typeid_cast<const ReadFromSystemNumbersStep *>(step) || typeid_cast<const AggregatingStep *>(step)
        || typeid_cast<const MergingAggregatedStep *>(step))
        return false;

    if (typeid_cast<const UnionStep *>(step))
    {
        for (const auto * child : node.children)
            if (mayBeSparseOrReplicated(*child, column_name, depth + 1))
                return true;
        return false;
    }

    if (const auto * window = typeid_cast<const WindowStep *>(step))
    {
        for (const auto & window_function : window->getWindowFunctions())
            if (window_function.column_name == column_name)
                return false;
        return node.children.size() != 1 || mayBeSparseOrReplicated(*node.children.front(), column_name, depth + 1);
    }

    const auto & input_headers = step->getInputHeaders();
    if (node.children.size() == 1 && input_headers.size() == 1 && blocksHaveEqualStructure(*input_headers.front(), *step->getOutputHeader()))
        return mayBeSparseOrReplicated(*node.children.front(), column_name, depth + 1);

    return true;
}

bool hasConstantFilter(const FilterStep & filter)
{
    const auto * filter_node = filter.getExpression().tryFindInOutputs(filter.getFilterColumnName());
    return !filter_node || filter_node->column;
}

bool breaksOnOverflow(const SizeLimits & limits)
{
    return limits.overflow_mode == OverflowMode::BREAK && (limits.max_rows || limits.max_bytes);
}

bool breaksOnOverflow(const Aggregator::Params & params)
{
    return params.group_by_overflow_mode == OverflowMode::BREAK && params.max_rows_to_group_by;
}

/// Whether the steps above read all rows, so that the projection after the window functions is computed for all rows.
/// Many steps can stop reading early: `LIMIT` (also `LIMIT BY` with constant keys, `LIMIT AFTER / UNTIL`,
/// `DISTINCT ... LIMIT`, `ORDER BY ... LIMIT`, after which `liftUpFunctions` computes the expressions), `INTERSECT`
/// or a `JOIN` with an empty side, a `Filter` with a constant `false` condition, a step with `overflow_mode = 'break'`
/// (also set only for a subquery). So only the steps known to read all rows are accepted. An aggregation reads
/// all rows, so the steps above it do not matter.
bool stepsAboveReadAllRows(const Stack & stack)
{
    for (auto it = stack.rbegin() + 1; it != stack.rend(); ++it)
    {
        const auto * step = it->node->step.get();
        if (const auto * aggregating = typeid_cast<const AggregatingStep *>(step))
            return !breaksOnOverflow(aggregating->getParams());

        if (const auto * merging_aggregated = typeid_cast<const MergingAggregatedStep *>(step))
            return !breaksOnOverflow(merging_aggregated->getParams());

        if (const auto * filter = typeid_cast<const FilterStep *>(step))
        {
            if (hasConstantFilter(*filter))
                return false;
            continue;
        }

        if (const auto * sorting = typeid_cast<const SortingStep *>(step))
        {
            if (sorting->getLimit() || breaksOnOverflow(sorting->getSettings().size_limits))
                return false;
            continue;
        }

        if (const auto * distinct = typeid_cast<const DistinctStep *>(step))
        {
            if (distinct->getLimitHint() || breaksOnOverflow(distinct->getSettings().set_size_limits))
                return false;
            continue;
        }

        /// The set-building steps are not accepted: the limits of a set are not known in the plan.
        if (typeid_cast<const ExpressionStep *>(step) || typeid_cast<const WindowStep *>(step) || typeid_cast<const UnionStep *>(step)
            || typeid_cast<const ArrayJoinStep *>(step) || typeid_cast<const OffsetStep *>(step) || typeid_cast<const ExtremesStep *>(step)
            || typeid_cast<const FillingStep *>(step) || typeid_cast<const CreatingSetsStep *>(step)
            || typeid_cast<const DelayedCreatingSetsStep *>(step))
            continue;

        return false;
    }

    return true;
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
    if (parent_node->children.size() != 1)
        return;

    /// A `Filter` keeps the rows for which the condition is true after the window functions as well, so the
    /// expressions it computes are computed for the same rows below them. With `QUALIFY`, the projection is
    /// a separate step above the `Filter` and is not changed.
    ActionsDAG * parent_actions_ptr = nullptr;
    const FilterStep * parent_filter = nullptr;
    if (auto * expression_step = typeid_cast<ExpressionStep *>(parent_node->step.get()))
    {
        parent_actions_ptr = &expression_step->getExpression();
    }
    else if (auto * filter_step = typeid_cast<FilterStep *>(parent_node->step.get()); filter_step && !hasConstantFilter(*filter_step))
    {
        parent_actions_ptr = &filter_step->getExpression();
        parent_filter = filter_step;
    }

    if (!parent_actions_ptr)
        return;

    auto * window_node = parent_node->children.front();
    if (!typeid_cast<const WindowStep *>(window_node->step.get()))
        return;

    auto & parent_actions = *parent_actions_ptr;
    if (parent_actions.hasArrayJoin())
        return;

    /// The rewrite resolves columns by name, which is ambiguous with duplicated names.
    NameSet input_names;
    for (const auto * input : parent_actions.getInputs())
        if (!input_names.insert(input->result_name).second)
            return;

    if (!stepsAboveReadAllRows(stack))
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
        else if (const auto * sorting = typeid_cast<const SortingStep *>(node->step.get()); sorting && !sorting->getLimit())
        {
            addColumnNames(sorting->getSortDescription(), columns_read_by_chain);
            addColumnNames(sorting->getPartitionByDescription(), columns_read_by_chain);
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

    /// The expressions that can be computed below the chain, grouped by the column they read.
    auto maybe_lazily_executed_nodes = collectMaybeLazilyExecutedNodes(parent_actions);

    /// With short-circuit evaluation disabled, a `Filter` still applies the arguments of an `and` condition one after
    /// another, so an argument is not executed for the rows removed by the previous ones (`s != 'x' AND toUInt64(s) > 0`).
    /// Computed in another step, the `and` would execute all its arguments for all rows, like in `tryFuseFilterIntoArrayJoin`.
    if (parent_filter && settings.short_circuit_function_evaluation_disabled)
    {
        const auto * filter_node = &parent_actions.findInOutputs(parent_filter->getFilterColumnName());
        while (filter_node->type == ActionsDAG::ActionType::ALIAS)
            filter_node = filter_node->children.front();

        if (filter_node->type == ActionsDAG::ActionType::FUNCTION && filter_node->function_base
            && filter_node->function_base->getName() == "and")
            maybe_lazily_executed_nodes.insert(filter_node);
    }

    std::unordered_map<const Node *, ExpressionInfo> expression_info_cache;
    std::unordered_map<const Node *, std::vector<const Node *>> candidates_by_input;
    for (const auto & node : parent_actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || maybe_lazily_executed_nodes.contains(&node))
            continue;

        const auto & info = getExpressionInfo(&node, expression_info_cache);
        if (!info.can_be_moved || !info.input)
            continue;

        auto minimum_size_of_input = getMinimumSizeOfValue(info.input->result_type);
        if (!minimum_size_of_input || !isStoredInFixedSize(node.result_type)
            || node.result_type->getMaximumSizeOfValueInMemory() > *minimum_size_of_input)
            continue;

        candidates_by_input[info.input].push_back(&node);
    }

    if (candidates_by_input.empty())
        return;

    NodeSet outputs;
    for (const auto * output : parent_actions.getOutputs())
        outputs.insert(output);

    NameSet result_names;

    /// Whether computing `candidates` below the chain replaces the column `input` there: nothing but them reads it,
    /// and their results are not larger than its smallest value.
    const auto can_replace_column = [&](const Node * input, const std::vector<const Node *> & candidates)
    {
        /// An internal node of a candidate is not allowed to be read from outside, otherwise it would be computed
        /// below the chain and carried through it. Constants are not carried, so they can be shared.
        NodeSet candidate_nodes;
        for (const auto * candidate : candidates)
            collectSubtree(candidate, candidate_nodes);
        std::erase_if(candidate_nodes, [](const Node * node) { return node->type == ActionsDAG::ActionType::COLUMN; });

        const NodeSet candidate_roots(candidates.begin(), candidates.end());
        for (const auto & node : parent_actions.getNodes())
        {
            if (candidate_nodes.contains(&node))
            {
                if (!candidate_roots.contains(&node) && outputs.contains(&node))
                    return false;
                continue;
            }

            for (const auto * child : node.children)
                if (candidate_nodes.contains(child) && !candidate_roots.contains(child))
                    return false;
        }

        size_t size_of_results = 0;
        NameSet names;
        for (const auto * candidate : candidates)
        {
            size_of_results += candidate->result_type->getMaximumSizeOfValueInMemory();
            if (window_header.has(candidate->result_name) || result_names.contains(candidate->result_name)
                || !names.insert(candidate->result_name).second)
                return false;
        }

        return size_of_results <= *getMinimumSizeOfValue(input->result_type);
    };

    NodeSet split_nodes;
    NameSet replaced_columns;
    for (const auto & [input, candidates] : candidates_by_input)
    {
        const auto & column_name = input->result_name;
        if (columns_read_by_chain.contains(column_name) || bottom_column_counts[column_name] != 1)
            continue;

        const auto * bottom_column = bottom_header.findByName(column_name);
        if (!bottom_column || !bottom_column->type->equals(*input->result_type))
            continue;

        /// The highest candidates have the smallest results in most cases, like `toUInt8(length(s))`. But if an
        /// intermediate result is read by something else, like `v` in `v > 0` (both a projection column and
        /// the condition of the `Filter`), only the lowest candidates can replace the column.
        const NodeSet candidate_set(candidates.begin(), candidates.end());
        NodeSet nodes_below_candidates;
        for (const auto * candidate : candidates)
            for (const auto * child : candidate->children)
                collectSubtree(child, nodes_below_candidates);

        std::vector<const Node *> highest_candidates;
        std::vector<const Node *> lowest_candidates;
        for (const auto * candidate : candidates)
        {
            if (!nodes_below_candidates.contains(candidate))
                highest_candidates.push_back(candidate);

            NodeSet subtree;
            for (const auto * child : candidate->children)
                collectSubtree(child, subtree);
            if (std::none_of(subtree.begin(), subtree.end(), [&](const Node * node) { return candidate_set.contains(node); }))
                lowest_candidates.push_back(candidate);
        }

        const std::vector<const Node *> * replacing_candidates = nullptr;
        if (can_replace_column(input, highest_candidates))
            replacing_candidates = &highest_candidates;
        else if (can_replace_column(input, lowest_candidates))
            replacing_candidates = &lowest_candidates;

        if (!replacing_candidates || !hasOwnMemory(*bottom_node, column_name) || mayBeSparseOrReplicated(*bottom_node, column_name))
            continue;

        for (const auto * candidate : *replacing_candidates)
            result_names.insert(candidate->result_name);
        split_nodes.insert(replacing_candidates->begin(), replacing_candidates->end());
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

    /// Merging into a `Filter` or into an `arrayJoin` below would compute the expressions for the rows they remove:
    /// in one step an `arrayJoin` is executed after the actions that do not depend on it.
    if (const auto * bottom_expression = typeid_cast<const ExpressionStep *>(bottom_node->step.get());
        bottom_expression && !bottom_expression->getExpression().hasArrayJoin())
        tryMergeExpressions(&pushed_node, nodes, settings);
}

}
