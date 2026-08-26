#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/optimizeReadInOrder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

#include <vector>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Walk down a single-child chain looking for a `ReadFromMergeTree` step. Used for the
/// parallel-replicas guard below.
const ReadFromMergeTree * findMergeTreeRead(const QueryPlan::Node * node)
{
    while (node)
    {
        if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
            return reading;
        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

}

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
/// so for it we first insert an emptiness guard below the new sort: the sort then picks the top-n
/// among the rows that survive the `ARRAY JOIN`, and each of those expands into at least one row.
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
    /// Both require the upstream to keep processing past the limit, which the inserted `Limit`
    /// would prevent.
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

    std::vector<std::pair<QueryPlan::Node *, ArrayJoinStep *>> array_joins;
    QueryPlan::Node * insertion_parent_node = nullptr;
    while (true)
    {
        if (auto * array_join_step = typeid_cast<ArrayJoinStep *>(array_join_node->step.get()))
        {
            if (array_join_node->children.size() != 1)
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

            array_joins.emplace_back(array_join_node, array_join_step);
            insertion_parent_node = array_join_node;
            array_join_node = array_join_node->children.front();
            continue;
        }

        if (typeid_cast<ExpressionStep *>(array_join_node->step.get()))
        {
            auto * expression_node = array_join_node;
            if (!peelPassThroughExpressions(array_join_node, description, 1))
                return 0;
            insertion_parent_node = expression_node;
            continue;
        }

        break;
    }

    if (array_joins.empty() || !insertion_parent_node)
        return 0;

    const size_t n = limit_step->getLimitForSorting();
    if (n == 0)
        return 0;

    /// Reuse the cap that already gates `tryOptimizeTopK`. If the user disabled large-N TopK
    /// optimization there, do not work around it here.
    if (settings.max_limit_for_top_k_optimization && n > settings.max_limit_for_top_k_optimization)
        return 0;

    QueryPlan::Node * array_join_input_node = array_join_node;

    /// Do not insert a `Sort + Limit` when the input is read with parallel replicas. The inserted
    /// `Sort` would let `optimizeReadInOrder` turn the scan into `WithOrder` mode, conflicting
    /// with the coordination mode the other replicas pick ("Replica decided to read in Default
    /// mode, not in WithOrder").
    if (const auto * reading = findMergeTreeRead(array_join_input_node))
    {
        if (reading->isParallelReadingFromReplicas())
            return 0;
    }

    /// Defer to `optimizeReadInOrder` (second pass) when the input can stream rows in the
    /// requested order straight from the storage's sorting key. That path scans only the rows the
    /// limit keeps, without materializing a sort, so it is strictly better than what we would do
    /// here. This is the steady state for `LEFT ARRAY JOIN ... ORDER BY <primary key>`, which
    /// already reads `InOrder` today.
    if (settings.read_in_order)
    {
        SortingStep probe_sort_step(
            array_join_input_node->step->getOutputHeader(),
            description,
            n,
            sort_step->getSettings());

        if (wouldReadInOrderBeUseful(
                probe_sort_step,
                *array_join_input_node,
                settings.read_in_order_through_join,
                settings.read_in_order_through_spilling_join))
            return 0;
    }

    /// An inner ARRAY JOIN drops input rows whose arrays are all empty. Filter them out below the
    /// moved sort so that each of the n rows it keeps expands into at least one output row.
    QueryPlan::Node * sorting_input_node = array_join_input_node;
    for (const auto & [_, array_join_step] : array_joins)
    {
        if (array_join_step->isLeft())
            continue;

        if (!addArrayJoinEmptinessFilter(*array_join_step, sorting_input_node, nodes))
            return 0;
    }

    auto moved_sort_step = std::make_unique<SortingStep>(
        sorting_input_node->step->getOutputHeader(),
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
    sort_node->children[0] = sorting_input_node;
    insertion_parent_node->children[0] = sort_node;
    insertion_parent_node->step->updateInputHeader(sort_node->step->getOutputHeader());
    parent_node->children[0] = first_node_below_sort;

    /// Re-run optimizations on the moved sort and guard.
    return 4 + array_joins.size();
}

}
