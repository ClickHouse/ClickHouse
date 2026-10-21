#include <Core/SortDescription.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/optimizeReadInOrder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// True when a `Sort + Limit` above `input_node` must not be added: a per-replica top-n is not the global one, and
/// with `defer_to_read_in_order` the second pass of `optimizeReadInOrder` can already read in the requested order.
bool shouldSkipTopKAboveMergeTreeInput(
    const QueryPlan::Node & input_node,
    const SortingStep & sort_step,
    const SortDescription & description,
    size_t limit,
    bool defer_to_read_in_order)
{
    const auto * reading = findMergeTreeRead(&input_node);
    if (!reading)
        return false;

    if (reading->isParallelReadingFromReplicas())
        return true;

    if (!defer_to_read_in_order)
        return false;

    /// as in `tryTopKThroughJoin`: defer only if pass 2 can read in order, a reverse read may be impossible (`FINAL`)
    SortingStep probe_sort_step(input_node.step->getOutputHeader(), description, limit, sort_step.getSettings());
    const auto order_info = getInputOrderIfReadInOrderIsUseful(probe_sort_step, reading->getStorageMetadata()->getSortingKey(), input_node);
    return order_info && (order_info->direction == 1 || reading->canReadInReverseOrder());
}

/// Add `notEmpty(c1) OR ... OR notEmpty(cn)` over the joined columns above `input_node`, the input of `array_join`:
/// an inner `ARRAY JOIN` drops a row only when all its arrays are empty. A constant condition needs no filter,
/// because then the `ARRAY JOIN` emits rows for all input rows or for none.
void addArrayJoinEmptinessFilter(const ArrayJoinStep & array_join, QueryPlan::Node *& input_node, QueryPlan::Nodes & nodes)
{
    ActionsDAG dag(input_node->step->getOutputHeader()->getColumnsWithTypeAndName());
    auto not_empty_function = FunctionFactory::instance().get("notEmpty", nullptr);

    ActionsDAG::NodeRawConstPtrs not_empty;
    for (const auto & name : array_join.getColumns())
        not_empty.push_back(&dag.addFunction(not_empty_function, {&dag.findInOutputs(name)}, {}));

    const auto * condition = not_empty.front();
    if (not_empty.size() > 1)
        condition = &dag.addFunction(FunctionFactory::instance().get("or", nullptr), std::move(not_empty), {});

    if (condition->column)
        return;

    dag.getOutputs().push_back(condition);
    String filter_column_name = condition->result_name;

    auto & filter_node = nodes.emplace_back();
    filter_node.children.push_back(input_node);
    filter_node.step = std::make_unique<FilterStep>(
        input_node->step->getOutputHeader(),
        std::move(dag),
        std::move(filter_column_name),
        /*remove_filter_column_=*/ true);
    filter_node.step->setStepDescription("Non-empty arrays for ARRAY JOIN");
    input_node = &filter_node;
}

}

/// Move `Sorting(K, limit = n)` below an `ARRAY JOIN` when `K` does not read a joined column. The outer `Limit(n)`
/// stays above the `ARRAY JOIN`.
///
/// Each output row of the `ARRAY JOIN` has the `K` of its input row. If each kept input row gives at least one
/// output row, the top-n output rows come from the top-n input rows, so the result does not change. `LEFT ARRAY JOIN`
/// gives at least one row for each input row. For an inner `ARRAY JOIN`, a filter below the moved sort removes the
/// rows whose arrays are all empty. A fused element filter can remove all elements of a non-empty array, and an
/// aligned `ARRAY JOIN` of several arrays must throw `SIZES_OF_ARRAYS_DONT_MATCH` for rows that the moved sort
/// would remove, so the pass does not apply to them.
///
/// Pattern: `Limit -> Sorting -> Expression* -> ArrayJoin`, where each `Expression` passes the sort keys through
/// (see `peelPassThroughExpressions`).
size_t tryTopKThroughArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// LIMIT WITH TIES needs to know how many rows have the threshold value, so we cannot
    /// restrict the input to n rows.
    if (limit_step->withTies())
        return 0;

    /// Skip when `always_read_till_end` is set (e.g. `WITH TOTALS`, `exact_rows_before_limit`).
    /// Both require the upstream to keep processing past the limit, which the moved `Sorting`'s
    /// own `limit` would cut short below the `ARRAY JOIN`.
    if (limit_step->alwaysReadTillEnd())
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * sort_node = parent_node->children.front();
    auto * sort_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sort_step)
        return 0;

    /// Only Full sort is meaningful here. FinishSorting/MergingSorted mean the input is already
    /// (partially) sorted, in which case the pipeline already stops early and there is nothing
    /// to gain. A partitioned sort produces a per-partition order, so a plain row-count limit
    /// below it would not correspond to the top-n of any partition.
    if (sort_step->getType() != SortingStep::Type::Full || sort_step->hasPartitions())
        return 0;

    if (sort_step->hasLimitByHint())
        return 0;

    if (sort_node->children.size() != 1)
        return 0;

    SortDescription description = sort_step->getSortDescription();
    QueryPlan::Node * first_node_below_sort = sort_node->children.front();
    QueryPlan::Node * array_join_node = first_node_below_sort;
    if (!peelPassThroughExpressions(array_join_node, description))
        return 0;

    auto * array_join_step = typeid_cast<ArrayJoinStep *>(array_join_node->step.get());
    if (!array_join_step || array_join_step->hasElementFilter() || array_join_node->children.size() != 1)
        return 0;

    const auto & array_join_columns = array_join_step->getColumns();
    /// the moved sort drops rows before the step checks that aligned arrays have equal sizes
    if (array_join_columns.size() > 1 && !array_join_step->isUnaligned())
        return 0;

    /// a joined column keeps its name across the step, so the input header alone does not tell that a key is joined
    const auto & array_join_input_header = array_join_step->getInputHeaders().front();
    for (const auto & sort_column : description)
        if (std::ranges::contains(array_join_columns, sort_column.column_name) || !array_join_input_header->has(sort_column.column_name))
            return 0;

    const size_t n = limit_step->getLimitForSorting();
    if (n == 0)
        return 0;

    QueryPlan::Node * array_join_input_node = array_join_node->children.front();

    /// Defer to parallel-replica coordination / `optimizeReadInOrder` when either would make
    /// a bounded `Sorting` below the `ARRAY JOIN` wrong or redundant. This is the steady state
    /// for `LEFT ARRAY JOIN ... ORDER BY <primary key>`, which already reads `InOrder`.
    if (shouldSkipTopKAboveMergeTreeInput(
            *array_join_input_node, *sort_step, description, n, settings.read_in_order))
        return 0;

    if (!array_join_step->isLeft())
        addArrayJoinEmptinessFilter(*array_join_step, array_join_input_node, nodes);

    auto moved_sort_step = std::make_unique<SortingStep>(
        array_join_input_node->step->getOutputHeader(),
        description,
        n,
        sort_step->getSettings());
    moved_sort_step->setStepDescription(*sort_step);

    /// Rewire
    ///
    ///   Limit -> Sort -> Expression* -> ArrayJoin -> Input
    ///
    /// into
    ///
    ///   Limit -> Expression* -> ArrayJoin -> Sort -> [Filter] -> Input.
    ///
    /// Reusing `sort_node` avoids allocating and abandoning a plan node. The expression chain
    /// keeps its original links and remains above the `ARRAY JOIN`.
    sort_node->step = std::move(moved_sort_step);
    sort_node->children[0] = array_join_input_node;
    array_join_node->children[0] = sort_node;
    array_join_step->updateInputHeader(sort_node->step->getOutputHeader());
    parent_node->children[0] = first_node_below_sort;

    /// the depth of the subtree to optimize again: `ArrayJoin -> Sorting -> [Filter]`, and one `Expression` above
    return 4;
}

}
