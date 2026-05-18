#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>

namespace DB::QueryPlanOptimizations
{

/// Returns true if the step preserves both row count and row order, so it is
/// safe to cross when matching LimitStep → SortingStep → AggregatingStep.
/// For ExpressionStep this is necessary but not sufficient on the
/// SortingStep → AggregatingStep leg — see expressionPreservesSortColumns.
static bool isTransparentStep(IQueryPlanStep * step)
{
    /// ExpressionStep does not advertise preserves_sorting (it cannot prove it
    /// for arbitrary expressions). Row count must be checked: ARRAY JOIN changes it.
    if (auto * expression = typeid_cast<ExpressionStep *>(step))
        return expression->getTransformTraits().preserves_number_of_rows;

    /// ExtremesStep observes the stream before LIMIT is applied; pushing the
    /// limit past it would feed it a truncated prefix and produce wrong extremes.
    /// WindowStep preserves rows but is not supported by this optimization.
    if (typeid_cast<ExtremesStep *>(step) || typeid_cast<WindowStep *>(step))
        return false;

    auto * transforming = dynamic_cast<ITransformingStep *>(step);
    if (!transforming)
        return false;

    return transforming->getDataStreamTraits().preserves_sorting
        && transforming->getTransformTraits().preserves_number_of_rows;
}

/// Returns true if every column referenced in sort_desc is the same value on
/// both sides of the ExpressionStep — i.e. each output with that name is a
/// pass-through of an input with the same name (possibly via alias chains).
/// A FUNCTION output, or an alias of a different input, would silently rewrite
/// the sort-key value while keeping the identifier, which would let LIMIT be
/// pushed past an order-changing transformation. Example: `SELECT -k AS k`
/// makes the output `k` a FUNCTION node, not a pass-through of the input `k`.
static bool expressionPreservesSortColumns(const ExpressionStep & expression, const SortDescription & sort_desc)
{
    const auto & dag = expression.getExpression();
    for (const auto & desc : sort_desc)
    {
        const auto * node = dag.tryFindInOutputs(desc.column_name);
        if (!node)
            return false;
        while (node->type == ActionsDAG::ActionType::ALIAS)
        {
            chassert(node->children.size() == 1);
            node = node->children.front();
        }
        if (node->type != ActionsDAG::ActionType::INPUT || node->result_name != desc.column_name)
            return false;
    }
    return true;
}

/// For LimitStep → SortingStep → AggregatingStep(in-order) where the sort
/// description is a prefix of the aggregation's group-by sort description,
/// push the limit into the aggregation step to enable early termination.
void optimizeLimitForAggregationInOrder(QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (frame.next_child < frame.node->children.size())
        {
            stack.push_back({.node = frame.node->children[frame.next_child++]});
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
        if (!limit_step || limit_step->withTies() || limit_step->alwaysReadTillEnd())
            continue;

        size_t limit = limit_step->getLimitForSorting();
        if (!limit)
            continue;

        /// Find SortingStep below LimitStep, skipping transparent steps.
        auto * current = node;
        SortingStep * sorting_step = nullptr;
        while (current->children.size() == 1)
        {
            current = current->children[0];
            sorting_step = typeid_cast<SortingStep *>(current->step.get());
            if (sorting_step || !isTransparentStep(current->step.get()))
                break;
        }
        if (!sorting_step || current->children.size() != 1)
            continue;
        current = current->children[0];

        /// Find AggregatingStep below SortingStep, skipping transparent steps.
        /// Stop at TotalsHavingStep — HAVING may filter groups.
        while (current)
        {
            auto * aggregating_step = typeid_cast<AggregatingStep *>(current->step.get());
            if (aggregating_step)
            {
                /// overflow_row is set by WITH TOTALS.
                if (!aggregating_step->inOrder()
                    || !aggregating_step->getFinal()
                    || aggregating_step->getParams().overflow_row
                    || aggregating_step->isGroupingSets())
                    break;

                const auto & sort_desc = sorting_step->getSortDescription();
                const auto & agg_sort_desc = aggregating_step->getGroupBySortDescription();
                if (sort_desc.empty() || !agg_sort_desc.hasPrefix(sort_desc))
                    break;

                /// Use the smallest limit if multiple LimitSteps point to the same AggregatingStep.
                size_t current_hint = aggregating_step->getLimitHint();
                if (!current_hint || limit < current_hint)
                    aggregating_step->setLimitHint(limit);
                break;
            }

            if (typeid_cast<TotalsHavingStep *>(current->step.get())
                || !isTransparentStep(current->step.get())
                || current->children.size() != 1)
                break;

            if (auto * expression = typeid_cast<ExpressionStep *>(current->step.get()))
            {
                if (!expressionPreservesSortColumns(*expression, sorting_step->getSortDescription()))
                    break;
            }

            current = current->children[0];
        }
    }
}

}
