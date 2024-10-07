#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/UnionStep.h>

namespace DB::QueryPlanOptimizations
{

/// We are trying to find a part of plan like
///
///          - ReadFromRemote (x N)
///  - Union - ReadFromParallelRemoteReplicasStep (x M)
///          - Aggregating/MergingAggregated
///
/// and enable memory bound merging for remote steps if it was enabled for local aggregation.
void enableMemoryBoundMerging(QueryPlan::Node & node)
{
    auto * root_mergine_aggeregated = typeid_cast<MergingAggregatedStep *>(node.step.get());
    if (!root_mergine_aggeregated)
        return;

    const auto & union_node = *node.children.front();
    auto * union_step = typeid_cast<UnionStep *>(union_node.step.get());
    if (!union_step)
        return;

    std::vector<ReadFromRemote *> reading_steps;
    std::vector<ReadFromParallelRemoteReplicasStep *> async_reading_steps;
    IQueryPlanStep * local_plan = nullptr;

    reading_steps.reserve((union_node.children.size()));
    async_reading_steps.reserve((union_node.children.size()));

    for (const auto & child : union_node.children)
    {
        auto * child_node = child->step.get();
        if (auto * reading_step = typeid_cast<ReadFromRemote *>(child_node))
            reading_steps.push_back(reading_step);
        else if (auto * async_reading_step = typeid_cast<ReadFromParallelRemoteReplicasStep *>(child_node))
            async_reading_steps.push_back(async_reading_step);
        else if (local_plan)
            /// Usually there is a single local plan.
            /// TODO: we can support many local plans and calculate common sort description prefix. Do we need it?
            return;
        else
            local_plan = child_node;
    }

    /// We determine output stream sort properties by a local plan (local because otherwise table could be unknown).
    /// If no local shard exist for this cluster, no sort properties will be provided, c'est la vie.
    if (local_plan == nullptr || (reading_steps.empty() && async_reading_steps.empty()))
        return;

    SortDescription sort_description;
    bool enforce_aggregation_in_order = false;

    if (auto * aggregating_step = typeid_cast<AggregatingStep *>(local_plan))
    {
        if (aggregating_step->memoryBoundMergingWillBeUsed())
        {
            sort_description = aggregating_step->getSortDescription();
            enforce_aggregation_in_order = true;
        }
    }
    else if (auto * mergine_aggeregated = typeid_cast<MergingAggregatedStep *>(local_plan))
    {
        if (mergine_aggeregated->memoryBoundMergingWillBeUsed())
        {
            sort_description = mergine_aggeregated->getGroupBySortDescription();
        }
    }

    if (sort_description.empty())
        return;

    for (auto & reading : reading_steps)
    {
        reading->enableMemoryBoundMerging();
        if (enforce_aggregation_in_order)
            reading->enforceAggregationInOrder();
    }

    for (auto & reading : async_reading_steps)
    {
        reading->enableMemoryBoundMerging();
        if (enforce_aggregation_in_order)
            reading->enforceAggregationInOrder();
    }

    root_mergine_aggeregated->applyOrder(sort_description);
}

}
