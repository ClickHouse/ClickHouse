#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// Move `Sorting(K, limit = n)` below an `ARRAY JOIN` when its keys do not depend on expanded
/// columns. The outer `Limit(n)` remains above the `ARRAY JOIN` to truncate expanded rows.
///
/// Soundness sketch
/// ----------------
/// Consider `Limit(n) <- Sort(K) <- ArrayJoin(c)` where `K` does not reference `c`. Every output
/// row of the `ARRAY JOIN` inherits its `K` value from the input row it was expanded from, so the
/// top-n output rows by `K` are drawn from the input rows with the n smallest (or largest) `K`
/// values - that is, the top-n input rows by `K`. Sorting the input with the same limit before
/// expansion therefore cannot change the final result.
///
/// This relies on every input row producing at least one output row. `LEFT ARRAY JOIN` satisfies
/// it by construction (`emptyArrayToSingle` in `ArrayJoinResultIterator`'s constructor gives every
/// empty array one default element). An inner `ARRAY JOIN` drops rows whose arrays are all empty,
/// so for it we first insert an emptiness guard on the join's input, then place the sort above that
/// guard (`ArrayJoin -> Sort -> Filter`): the sort picks the top-n among rows that survive the
/// `ARRAY JOIN`, and each of those expands into at least one row. A fused element filter
/// (`query_plan_fuse_filter_into_array_join`) can still drop every element of a non-empty array, so
/// this optimization bails out when `hasElementFilter()` is set.
///
/// Only a single `ArrayJoinStep` is handled. Walking a chain would drop the analyzer's
/// rename-to-identifier expressions under deeper joins (`NOT_FOUND_COLUMN_IN_BLOCK`) and would
/// mis-build emptiness guards when an upper join reads the array produced by a lower one
/// (`ARRAY JOIN nested AS inner ARRAY JOIN inner AS elem`).
///
/// Pattern matched: `LimitStep -> SortingStep -> [ExpressionStep] -> ArrayJoinStep`.
/// The optional `ExpressionStep`s are allowed only when every sort key passes through them
/// unchanged (see `peelPassThroughExpressions`).
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
    const NameSet array_join_column_names(array_join_columns.begin(), array_join_columns.end());
    const auto & array_join_input_header = array_join_step->getInputHeaders().front();

    /// Every sort key must be carried through the `ARRAY JOIN` unchanged. A joined column
    /// keeps its name across the step and only changes its type, so checking that the name is
    /// present in the input header is not enough.
    for (const auto & sort_column : description)
    {
        if (array_join_column_names.contains(sort_column.column_name))
            return 0;
        if (!array_join_input_header->has(sort_column.column_name))
            return 0;
    }

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

    /// An inner ARRAY JOIN drops input rows whose arrays are all empty. Filter them out on the
    /// join's immediate input (where the joined columns are present under `getColumns()` names),
    /// then place the sort above that guard: `ArrayJoin -> Sort -> Filter -> Input`.
    if (!array_join_step->isLeft())
    {
        if (!addArrayJoinEmptinessFilter(*array_join_step, array_join_input_node, nodes))
            return 0;
    }

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

    /// How deep to re-run first-pass optimizations under the `Limit`. Enough for the common
    /// shape `ArrayJoin -> Sorting -> [emptiness Filter]` (and one pass-through `Expression`
    /// above the join); deeper expression chains are left for a later full pass.
    return 4;
}

}
