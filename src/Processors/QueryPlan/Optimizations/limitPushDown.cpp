#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// If plan looks like Limit -> Sorting, update limit for Sorting
static bool tryUpdateLimitForSortingSteps(QueryPlan::Node * node, size_t limit)
{
    if (limit == 0)
        return false;

    QueryPlanStepPtr & step = node->step;
    QueryPlan::Node * child = nullptr;
    bool updated = false;

    if (auto * sorting = typeid_cast<SortingStep *>(step.get()))
    {
        /// TODO: remove LimitStep here.
        sorting->updateLimit(limit);
        updated = true;
        child = node->children.front();
    }

    /// In case we have several sorting steps.
    /// Try update limit for them also if possible.
    if (child)
        tryUpdateLimitForSortingSteps(child, limit);

    return updated;
}

size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * limit = typeid_cast<LimitStep *>(parent.get());

    if (!limit)
        return 0;

    /// Skip LIMIT WITH TIES by now.
    if (limit->withTies())
        return 0;

    /// Push LIMIT into each branch of UNION ALL.
    /// Each branch only needs to produce at most (limit + offset) rows,
    /// because the final LIMIT on top will handle the rest.
    /// Skip when `always_read_till_end` is set (e.g. `exact_rows_before_limit` or `WITH TOTALS`),
    /// because per-branch limits would terminate sources early and break `rows_before_limit_at_least`.
    if (typeid_cast<UnionStep *>(child.get()))
    {
        if (limit->alwaysReadTillEnd())
            return 0;

        const size_t limit_for_branches = limit->getLimitForSorting();
        if (limit_for_branches == 0)
            return 0;

        /// Check if all branches already have a Limit to avoid repeated application.
        bool all_have_limit = true;
        for (const auto * child_of_union : child_node->children)
        {
            if (!typeid_cast<const LimitStep *>(child_of_union->step.get()))
            {
                all_have_limit = false;
                break;
            }
        }
        if (all_have_limit)
            return 0;

        for (auto *& child_of_union : child_node->children)
        {
            auto & limit_node = nodes.emplace_back();
            limit_node.children.push_back(child_of_union);
            child_of_union = &limit_node;

            limit_node.step = std::make_unique<LimitStep>(
                limit_node.children.front()->step->getOutputHeader(),
                limit_for_branches, 0);
        }

        /// The outer Limit stays on top; we just added inner Limits to each branch.
        return 0;
    }

    const auto * transforming = dynamic_cast<const ITransformingStep *>(child.get());

    /// Skip everything which is not transform.
    if (!transforming)
        return 0;

    /// Special cases for sorting steps.
    if (tryUpdateLimitForSortingSteps(child_node, limit->getLimitForSorting()))
        return 0;

    if (auto * distinct = typeid_cast<DistinctStep *>(child.get()))
    {
        distinct->updateLimitHint(limit->getLimitForSorting());
        return 0;
    }

    if (typeid_cast<const SortingStep *>(child.get()))
        return 0;

    /// Special case for TotalsHaving. Totals may be incorrect if we push down limit.
    if (typeid_cast<const TotalsHavingStep *>(child.get()))
        return 0;

    /// Disable for WindowStep.
    /// TODO: we can push down limit in some cases if increase the limit value.
    if (typeid_cast<const WindowStep *>(child.get()))
        return 0;

    /// Now we should decide if pushing down limit possible for this step.

    const auto & transform_traits = transforming->getTransformTraits();
    // const auto & data_stream_traits = transforming->getDataStreamTraits();

    /// Cannot push down if child changes the number of rows.
    if (!transform_traits.preserves_number_of_rows)
        return 0;

    /// Input stream for Limit have changed.
    limit->updateInputHeader(transforming->getInputHeaders().front());

    parent.swap(child);
    return 2;
}

}
