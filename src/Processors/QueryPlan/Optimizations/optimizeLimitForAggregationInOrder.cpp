#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>

namespace DB::QueryPlanOptimizations
{

/// Returns true if the step is transparent for this optimization:
/// it must preserve the number of rows (to avoid applying the limit
/// before row-dropping operators like FilterStep, LimitByStep, OffsetStep).
/// ExpressionStep is allowed explicitly: it preserves row count and doesn't
/// reorder rows, even though its generic preserves_sorting trait is false
/// (because it cannot prove sort preservation for arbitrary expressions).
/// The sort column identifiers are the same between SortingStep and
/// AggregatingStep, so ExpressionStep doesn't interfere with prefix matching.
static bool isTransparentStep(IQueryPlanStep * step)
{
    if (typeid_cast<ExpressionStep *>(step))
        return true;

    /// WindowStep preserves row count and may preserve sorting,
    /// but we don't support crossing it for now.
    if (typeid_cast<WindowStep *>(step))
        return false;

    auto * transforming = dynamic_cast<ITransformingStep *>(step);
    if (!transforming)
        return false;

    return transforming->getDataStreamTraits().preserves_sorting
        && transforming->getTransformTraits().preserves_number_of_rows;
}

/// When the plan contains LimitStep → SortingStep → AggregatingStep(in-order),
/// and the aggregation output is already sorted in the ORDER BY order,
/// we can push the limit into the aggregation step. This enables early termination:
/// the aggregation stops after producing enough groups, avoiding reading the entire table.
void optimizeLimitForAggregationInOrder(QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
        if (!limit_step)
            continue;

        if (limit_step->withTies())
            continue;

        if (limit_step->alwaysReadTillEnd())
            continue;

        size_t limit = limit_step->getLimitForSorting();
        if (limit == 0)
            continue;

        /// Find SortingStep below LimitStep, skipping transparent steps.
        auto * current = node;
        SortingStep * sorting_step = nullptr;

        while (current->children.size() == 1)
        {
            current = current->children[0];
            sorting_step = typeid_cast<SortingStep *>(current->step.get());
            if (sorting_step)
                break;

            if (!isTransparentStep(current->step.get()))
                break;
        }

        if (!sorting_step)
            continue;

        /// Find AggregatingStep below SortingStep, skipping transparent steps.
        if (current->children.size() != 1)
            continue;
        current = current->children[0];

        while (current)
        {
            auto * aggregating_step = typeid_cast<AggregatingStep *>(current->step.get());
            if (aggregating_step)
            {
                if (!aggregating_step->inOrder())
                    break;

                if (!aggregating_step->getFinal())
                    break;

                /// WITH TOTALS uses overflow_row.
                if (aggregating_step->getParams().overflow_row)
                    break;

                if (aggregating_step->isGroupingSets())
                    break;

                /// Aggregation-in-order produces output sorted by group_by_sort_description.
                /// Check that the SortingStep's sort description is a prefix of it.
                const auto & sort_desc = sorting_step->getSortDescription();
                const auto & agg_sort_desc = aggregating_step->getGroupBySortDescription();

                if (sort_desc.empty() || sort_desc.size() > agg_sort_desc.size())
                    break;

                bool prefix_match = agg_sort_desc.hasPrefix(sort_desc);

                if (prefix_match)
                {
                    /// Use the smallest limit if multiple LimitSteps point to the same AggregatingStep.
                    size_t current_hint = aggregating_step->getLimitHint();
                    if (current_hint == 0 || limit < current_hint)
                        aggregating_step->setLimitHint(limit);
                }

                break;
            }

            /// Stop at TotalsHavingStep — HAVING may filter groups.
            if (typeid_cast<TotalsHavingStep *>(current->step.get()))
                break;

            if (!isTransparentStep(current->step.get()))
                break;

            if (current->children.size() != 1)
                break;

            current = current->children[0];
        }
    }
}

}
