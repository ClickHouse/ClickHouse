#include <Processors/QueryPlan/RuntimeFilterOrder.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/Utils.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/logical.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <numeric>
#include <ranges>

namespace DB
{

namespace
{

constexpr Int64 min_rows_for_selectivity = 32768;
constexpr Int64 min_large_block_rows_for_probe_time = 4 * min_rows_for_selectivity;
constexpr Float64 probe_time_margin = 0.95;
constexpr Float64 rejected_rows_margin = 1.1;

Float64 rejectedFraction(const RuntimeFilterStepMeasurement & step)
{
    return 1.0 - *step.selectivity;
}

Float64 rejectedRowsAndBlocks(const RuntimeFilterStepMeasurement & step)
{
    return rejectedFraction(step) + step.rows_in_rejected_blocks_ratio;
}

/// A step without `c` has run on small blocks only, which a filter rejects whole far more often: against a step with `c`, its
/// rejected blocks don't count.
Float64 rejectedRowsAgainst(const RuntimeFilterStepMeasurement & step, const RuntimeFilterStepMeasurement & other)
{
    if (!step.probe_nanoseconds_per_row && other.probe_nanoseconds_per_row)
        return rejectedFraction(step);
    return rejectedRowsAndBlocks(step);
}

Float64 probeTimePerRejectedRow(const RuntimeFilterStepMeasurement & step)
{
    const Float64 rejected_rows = rejectedRowsAndBlocks(step);
    if (rejected_rows <= 0)
        return std::numeric_limits<Float64>::infinity();
    return *step.probe_nanoseconds_per_row / rejected_rows;
}

/// Relative to the rows passed too, so a step rejecting 97% still yields to one rejecting 99.98%.
bool rejectsClearlyMore(const RuntimeFilterStepMeasurement & mover, const RuntimeFilterStepMeasurement & other)
{
    const Float64 mover_rejected = rejectedRowsAgainst(mover, other);
    const Float64 other_rejected = rejectedRowsAgainst(other, mover);
    auto passed = [](Float64 rejected_rows) { return std::max(1.0 - rejected_rows, 0.0); };
    return mover_rejected > rejected_rows_margin * other_rejected || rejected_rows_margin * passed(mover_rejected) < passed(other_rejected);
}

bool movesAhead(const RuntimeFilterStepMeasurement & mover, const RuntimeFilterStepMeasurement & ahead)
{
    if (!mover.selectivity || !ahead.selectivity)
        return false;
    if (mover.probe_nanoseconds_per_row && ahead.probe_nanoseconds_per_row)
        return probeTimePerRejectedRow(mover) < probe_time_margin * probeTimePerRejectedRow(ahead);
    if (ahead.probe_nanoseconds_per_row && ahead.cheap_to_probe && !mover.cheap_to_probe)
        return false;
    return rejectsClearlyMore(mover, ahead);
}

RuntimeFilterStepMeasurement measureRuntimeFilter(const RuntimeFilter & filter)
{
    const auto & stats = filter.getStats();
    auto load = [](const std::atomic<Int64> & counter) { return counter.load(std::memory_order_relaxed); };
    auto ratio = [&](const std::atomic<Int64> & part, Int64 rows)
    { return rows > 0 ? static_cast<Float64>(load(part)) / static_cast<Float64>(rows) : 0.0; };

    RuntimeFilterStepMeasurement measurement;
    measurement.cheap_to_probe = filter.cheapToProbe();

    const Int64 checked = load(stats.rows_checked);
    if (filter.isKeySetDropped())
        measurement.selectivity = 1.0;
    else if (checked >= min_rows_for_selectivity)
        measurement.selectivity = ratio(stats.rows_passed, checked);

    const Int64 large_block_rows = load(stats.large_block_rows);
    if (large_block_rows >= min_large_block_rows_for_probe_time)
    {
        measurement.probe_nanoseconds_per_row = ratio(stats.large_block_probe_nanoseconds, large_block_rows);
        measurement.rows_in_rejected_blocks_ratio = ratio(stats.large_block_rows_in_rejected_blocks, large_block_rows);
    }
    else
        measurement.rows_in_rejected_blocks_ratio = ratio(stats.rows_in_rejected_blocks, checked);
    return measurement;
}

bool isFunction(const ActionsDAG::Node * node, std::string_view name)
{
    return node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == name;
}

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// A key that only looks up stored values, so it can't throw: a column, a safe cast of one, or a tuple of those.
bool addKeyColumns(const ActionsDAG::Node * node, NameSet & key_columns)
{
    node = skipAliases(node);
    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        key_columns.insert(node->result_name);
        return true;
    }
    if ((isFunction(node, "_CAST") || isFunction(node, "CAST")) && canBeSafelyCast(node->children.front()->result_type, node->result_type))
        return addKeyColumns(node->children.front(), key_columns);
    return isFunction(node, "tuple")
        && std::ranges::all_of(node->children, [&](const auto * child) { return addKeyColumns(child, key_columns); });
}

/// `__applyFilter(filter_id, key)`, alone or in the `or(..., isNull(key))` an ANTI join adds.
std::optional<String> tryGetRuntimeFilterId(const ActionsDAG::Node * node, NameSet & key_columns)
{
    node = skipAliases(node);
    if (isFunction(node, "or"))
    {
        const ActionsDAG::Node * filter = nullptr;
        for (const auto * child : node->children)
        {
            if (isFunction(child, "__applyFilter") && !filter)
                filter = child;
            else if (!isFunction(child, "isNull") || !addKeyColumns(child->children.front(), key_columns))
                return std::nullopt;
        }
        /// A set that holds NULL rejects the NULL keys the `or` passes, so the filter measures other rows than the conjunct.
        if (!filter || filter->children.size() != 2 || !canDropNullKeys(filter->children.back()->result_type))
            return std::nullopt;
        node = filter;
    }
    if (!isFunction(node, "__applyFilter") || node->children.size() != 2 || !node->children.front()->column
        || !addKeyColumns(node->children.back(), key_columns))
        return std::nullopt;
    const auto * filter_id = checkAndGetColumnConst<ColumnString>(node->children.front()->column.get());
    if (!filter_id)
        return std::nullopt;
    return filter_id->getValue<String>();
}

/// The filters of a step made of runtime filters only. Several when the PREWHERE split groups adjacent conditions that read the same
/// columns, as two joins on one probe column.
RuntimeFilterKeys getRuntimeFilters(const PrewhereExprStep & step)
{
    if (!step.is_synthesized_dynamic_filter || !step.actions)
        return {};
    const auto & dag = step.actions->getActionsDAG();
    const auto * filter = dag.tryFindInOutputs(step.filter_column_name);
    if (!filter)
        return {};
    ActionsDAG::NodeRawConstPtrs atoms;
    flattenConjunction(skipAliases(filter), atoms);
    RuntimeFilterKeys filters;
    for (const auto * atom : atoms)
    {
        /// The constant `true` of the last step's `and(<filters>, true)`.
        if (atom->type == ActionsDAG::ActionType::COLUMN)
            continue;
        auto filter_id = tryGetRuntimeFilterId(atom, filters.key_columns);
        if (!filter_id)
            return {};
        filters.filter_ids.push_back(std::move(*filter_id));
    }
    return filters;
}

bool isMovableStep(const PrewhereExprStep & step, bool last_step_needs_filter)
{
    /// Any of the reorderable steps can end up last and keeps its `need_filter`; `remove_filter_column == false` means a later step
    /// reads the filter column.
    return !step.query_condition_cache_attribution_boundary && step.remove_filter_column && step.need_filter == last_step_needs_filter;
}

ReorderableRuntimeFilterSteps findReorderableSteps(size_t size, const std::function<RuntimeFilterKeys(size_t)> & get_movable_filters)
{
    ReorderableRuntimeFilterSteps steps;
    for (size_t i = size; i > 0; --i)
    {
        auto filters = get_movable_filters(i - 1);
        if (!filters.filter_ids.empty())
        {
            steps.first_step = i - 1;
            steps.filter_ids_per_step.push_back(std::move(filters.filter_ids));
            steps.key_columns_per_step.push_back(std::move(filters.key_columns));
        }
        else if (!steps.filter_ids_per_step.empty())
            break;
    }
    if (steps.filter_ids_per_step.size() < 2)
        return {};
    std::ranges::reverse(steps.filter_ids_per_step);
    std::ranges::reverse(steps.key_columns_per_step);
    return steps;
}

}

/// Selectivities multiply as if independent; a block one filter rejects whole the step rejects whole.
RuntimeFilterStepMeasurement measureRuntimeFilterStep(const VectorWithMemoryTracking<RuntimeFilterConstPtr> & filters)
{
    RuntimeFilterStepMeasurement step{.selectivity = 1.0, .probe_nanoseconds_per_row = 0.0, .cheap_to_probe = true};
    for (const auto & filter : filters)
    {
        if (!filter)
            return {};

        const auto measurement = measureRuntimeFilter(*filter);
        if (step.selectivity && measurement.selectivity)
            *step.selectivity *= *measurement.selectivity;
        else
            step.selectivity.reset();
        if (step.probe_nanoseconds_per_row && measurement.probe_nanoseconds_per_row)
            *step.probe_nanoseconds_per_row += *measurement.probe_nanoseconds_per_row;
        else
            step.probe_nanoseconds_per_row.reset();
        step.rows_in_rejected_blocks_ratio = std::max(step.rows_in_rejected_blocks_ratio, measurement.rows_in_rejected_blocks_ratio);
        step.cheap_to_probe = step.cheap_to_probe && measurement.cheap_to_probe;
    }
    return step;
}

PrewhereStepOrder orderRuntimeFilterSteps(const VectorWithMemoryTracking<RuntimeFilterStepMeasurement> & steps)
{
    PrewhereStepOrder order(steps.size());
    std::iota(order.begin(), order.end(), 0);
    /// The comparisons mix measures, so they need not be transitive: bound the passes as for a bubble sort.
    for (size_t pass = 0; pass < order.size(); ++pass)
    {
        bool swapped = false;
        for (size_t i = 1; i < order.size(); ++i)
        {
            if (movesAhead(steps[order[i]], steps[order[i - 1]]))
            {
                std::swap(order[i - 1], order[i]);
                swapped = true;
            }
        }
        if (!swapped)
            break;
    }
    return order;
}

PrewhereStepOrder
orderRuntimeFilterStepsByMeasurement(const IRuntimeFilterLookup & lookup, const VectorWithMemoryTracking<Names> & filter_ids_per_step)
{
    VectorWithMemoryTracking<RuntimeFilterStepMeasurement> measurements;
    measurements.reserve(filter_ids_per_step.size());
    for (const auto & filter_ids : filter_ids_per_step)
    {
        VectorWithMemoryTracking<RuntimeFilterConstPtr> filters;
        for (const auto & filter_id : filter_ids)
            filters.push_back(lookup.find(filter_id));
        measurements.push_back(measureRuntimeFilterStep(filters));
    }
    auto order = orderRuntimeFilterSteps(measurements);
    if (std::ranges::is_sorted(order))
        return {};
    return order;
}

std::optional<ActionsDAG> reorderRuntimeFilterConjuncts(
    const ActionsDAG & dag, const String & filter_column_name, const IRuntimeFilterLookup & lookup)
{
    ActionsDAG::NodeRawConstPtrs conjuncts;
    flattenConjunction(&dag.findInOutputs(filter_column_name), conjuncts);

    const auto steps = findReorderableSteps(conjuncts.size(), [&](size_t i)
    {
        RuntimeFilterKeys filters;
        if (auto filter_id = tryGetRuntimeFilterId(conjuncts[i], filters.key_columns))
            filters.filter_ids.push_back(std::move(*filter_id));
        return filters;
    });
    const size_t begin = steps.first_step;
    const auto & filter_ids_per_step = steps.filter_ids_per_step;
    if (filter_ids_per_step.empty())
        return std::nullopt;

    const auto order = orderRuntimeFilterStepsByMeasurement(lookup, filter_ids_per_step);
    if (order.empty())
        return std::nullopt;

    const ActionsDAG::NodeRawConstPtrs planned(conjuncts.begin() + begin, conjuncts.begin() + begin + order.size());
    std::ranges::transform(order, conjuncts.begin() + begin, [&](size_t step) { return planned[step]; });
    auto step_filter_ids = [&](size_t step) { return fmt::format("[{}]", fmt::join(filter_ids_per_step[step], ", ")); };
    LOG_TRACE(
        getLogger("RuntimeFilterOrder"),
        "Runtime filter PREWHERE steps run in the order {}",
        fmt::join(order | std::views::transform(step_filter_ids), ", "));

    ActionsDAG::NodeMapping old_to_new;
    ActionsDAG result = dag.clone(old_to_new);
    for (auto & conjunct : conjuncts)
        conjunct = old_to_new.at(conjunct);
    const auto & new_root = result.addFunction(createInternalFunctionAndOverloadResolver(), std::move(conjuncts), filter_column_name);
    result.addOrReplaceInOutputs(new_root);
    result.removeUnusedActions(/*allow_remove_inputs=*/ false);
    return result;
}

ReorderableRuntimeFilterSteps findReorderableRuntimeFilterSteps(const PrewhereExprInfo & prewhere_actions)
{
    return findReorderableSteps(prewhere_actions.steps.size(), [&](size_t i) -> RuntimeFilterKeys
    {
        const auto & step = *prewhere_actions.steps[i];
        if (!isMovableStep(step, prewhere_actions.steps.back()->need_filter))
            return {};
        return getRuntimeFilters(step);
    });
}

}
