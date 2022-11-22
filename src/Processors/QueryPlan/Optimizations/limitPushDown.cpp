#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
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

size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
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

    const auto * transforming = dynamic_cast<const ITransformingStep *>(child.get());

    /// Skip everything which is not transform.
    if (!transforming)
        return 0;

    /// Special cases for sorting steps.
    if (tryUpdateLimitForSortingSteps(child_node, limit->getLimitForSorting()))
        return 0;

    /// Special case for TotalsHaving. Totals may be incorrect if we push down limit.
    if (typeid_cast<const TotalsHavingStep *>(child.get()))
        return 0;

    /// Now we should decide if pushing down limit possible for this step.

    const auto & transform_traits = transforming->getTransformTraits();
    const auto & data_stream_traits = transforming->getDataStreamTraits();

    /// Cannot push down if child changes the number of rows.
    if (!transform_traits.preserves_number_of_rows)
        return 0;

    /// Cannot push down if data was sorted exactly by child stream.
    if (!child->getOutputStream().sort_description.empty() && !data_stream_traits.preserves_sorting)
        return 0;

    /// Now we push down limit only if it doesn't change any stream properties.
    /// TODO: some of them may be changed and, probably, not important for following streams. We may add such info.
    if (!limit->getOutputStream().hasEqualPropertiesWith(transforming->getOutputStream()))
        return 0;

    /// Input stream for Limit have changed.
    limit->updateInputStream(transforming->getInputStreams().front());

    parent.swap(child);
    return 2;
}

}
