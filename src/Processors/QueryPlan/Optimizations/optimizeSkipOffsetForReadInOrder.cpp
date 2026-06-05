#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

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

    auto apply = [&](size_t offset, auto && set_offset)
    {
        if (const size_t skipped_rows = reading->skipRowsForOffset(offset))
            set_offset(offset - skipped_rows);
    };

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        auto * step = iter->node->step.get();

        if (auto * limit_step = typeid_cast<LimitStep *>(step))
        {
            if (limit_step->withTies())
                return;

            /// A preliminary LIMIT (no offset) only truncates the tail; walk past it to the real offset.
            if (limit_step->getOffset() == 0)
                continue;

            apply(limit_step->getOffset(), [&](size_t new_offset) { limit_step->setOffset(new_offset); });
            return;
        }

        if (auto * offset_step = typeid_cast<OffsetStep *>(step))
        {
            apply(offset_step->getOffset(), [&](size_t new_offset) { offset_step->setOffset(new_offset); });
            return;
        }

        /// Sorting and pure expressions preserve the leading rows; anything else may change the row set.
        if (typeid_cast<SortingStep *>(step) || typeid_cast<ExpressionStep *>(step))
            continue;

        return;
    }
}

}
