#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Interpreters/ActionsDAG.h>

#include <limits>
#include <optional>

namespace DB::QueryPlanOptimizations
{

/// Map a column name down through the Expression steps between the Sorting and the read, which the analyzer
/// uses to qualify names (`k` -> `__table1.k`). Only renames are followed: anything computed, including a
/// monotonic function of the key, has no counterpart among the read's own columns and gives up.
static std::optional<String> resolveThroughRenames(const String & name, const std::vector<const ActionsDAG *> & dags)
{
    String current = name;
    for (auto it = dags.rbegin(); it != dags.rend(); ++it)
    {
        const auto * node = (*it)->tryFindInOutputs(current);
        if (!node)
            return {};

        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.front();

        if (node->type != ActionsDAG::ActionType::INPUT)
            return {};

        current = node->result_name;
    }
    return current;
}

/// Pattern: Limit(offset>0)|Offset -> [Expression|Sorting|Limit(offset==0)]* -> ReadFromMergeTree (forward
/// read-in-order). Drop the leading granules consumed by the offset during reading and reduce the offset by
/// the rows skipped. Any other step in between (e.g. a Filter that removes rows) makes the walk bail out.
void optimizeSkipOffsetForReadInOrder(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * reading = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!reading)
        return;

    /// Only forward read-in-order can have leading granules cleanly skipped.
    if (const auto & input_order_info = reading->getInputOrder(); !input_order_info || input_order_info->direction != 1)
        return;

    /// A function that is not deterministic within the query derives a row's result from the rows preceding it
    /// in its stream (`rowNumberInAllBlocks`, `neighbor`, `runningDifference`) or from the block the row arrives
    /// in (`blockSize`, `nowInBlock`), and skipping granules changes both those rows and the block boundaries
    /// they are batched into. That is visible even above the offset, where the trimmed read shifts what the
    /// offset step passes on, so bail out on such a function anywhere on the path to the root.
    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        auto * step = iter->node->step.get();

        if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
        {
            if (dagContainsNonDeterministicFunction(expression_step->getExpression()))
                return;
        }
        else if (auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            if (dagContainsNonDeterministicFunction(filter_step->getExpression()))
                return;
        }
    }

    /// The order the offset counts rows in, resolved into the read's own column names.
    SortDescription offset_order;
    std::vector<const ActionsDAG *> dags_below_sorting;

    auto apply = [&](size_t offset, auto && set_offset)
    {
        if (offset_order.empty())
            return;
        if (const size_t skipped_rows = reading->skipRowsForOffset(offset, offset_order))
            set_offset(offset - skipped_rows);
    };

    /// The tightest LIMIT walked past on the way up, if any. Such a limit cuts the prefix the offset above it
    /// sees, so trimming granules below it can promote rows the limit would have dropped.
    std::optional<size_t> intermediate_limit;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        auto * step = iter->node->step.get();

        if (auto * limit_step = typeid_cast<LimitStep *>(step))
        {
            if (limit_step->withTies())
                return;

            /// `always_read_till_end` (e.g. exact `rows_before_limit_at_least`) reads sources to completion so
            /// LimitTransform can count every row reaching it; skipped granules would never be counted, so the
            /// count would underreport by exactly the skipped rows.
            if (limit_step->alwaysReadTillEnd())
                return;

            const size_t offset = limit_step->getOffset();

            /// A preliminary LIMIT (no offset) only truncates the tail; walk past it to the real offset.
            if (offset == 0)
            {
                const size_t limit = limit_step->getLimit();
                intermediate_limit = intermediate_limit ? std::min(*intermediate_limit, limit) : limit;
                continue;
            }

            /// Skipping is only invisible to the limits below if they still pass through the whole prefix
            /// this offset consumes plus everything it keeps.
            const size_t limit = limit_step->getLimit();
            const bool overflows = limit > std::numeric_limits<size_t>::max() - offset;
            if (intermediate_limit && (overflows || *intermediate_limit < offset + limit))
                return;

            apply(offset, [&](size_t new_offset) { limit_step->setOffset(new_offset); });
            return;
        }

        if (auto * offset_step = typeid_cast<OffsetStep *>(step))
        {
            /// A pure OFFSET keeps every row after it, so no finite intermediate limit can cover it.
            if (intermediate_limit)
                return;

            apply(offset_step->getOffset(), [&](size_t new_offset) { offset_step->setOffset(new_offset); });
            return;
        }

        /// Sorting preserves the leading rows. An Expression does too, unless it contains an arrayJoin, which
        /// expands rows and makes the offset count post-expansion rows rather than source rows.
        if (auto * sorting_step = typeid_cast<SortingStep *>(step))
        {
            /// A second Sorting re-orders the rows the offset sees, so the leading granules of the read are
            /// no longer the ones it consumes.
            if (!offset_order.empty())
                return;

            for (const auto & column : sorting_step->getSortDescription())
            {
                auto resolved = resolveThroughRenames(column.column_name, dags_below_sorting);
                if (!resolved)
                    return;
                offset_order.emplace_back(*resolved, column.direction, column.nulls_direction);
            }
            continue;
        }

        if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
        {
            if (expression_step->getExpression().hasArrayJoin())
                return;
            if (offset_order.empty())
                dags_below_sorting.push_back(&expression_step->getExpression());
            continue;
        }

        return;
    }
}

}
