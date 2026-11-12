#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/typeid_cast.h>

#include <unordered_map>

namespace DB::QueryPlanOptimizations
{

namespace
{

enum class StreamOrderUse : uint8_t
{
    /// The step keeps its input streams apart and in order, so its own consumer decides.
    PassedOn,
    Ignored,
    Required,
};

StreamOrderUse getStreamOrderUse(const IQueryPlanStep & step)
{
    if (typeid_cast<const ExpressionStep *>(&step) || typeid_cast<const FilterStep *>(&step) || typeid_cast<const UnionStep *>(&step))
        return StreamOrderUse::PassedOn;

    if (const auto * merging_aggregated = typeid_cast<const MergingAggregatedStep *>(&step))
    {
        /// Without keys the result is never two-level, so the memory efficient merge waits for every input.
        if (!merging_aggregated->memoryBoundMergingWillBeUsed()
            && (!merging_aggregated->isMemoryEfficient() || merging_aggregated->getParams().keys.empty()))
            return StreamOrderUse::Ignored;
        return StreamOrderUse::Required;
    }

    if (const auto * aggregating = typeid_cast<const AggregatingStep *>(&step))
        return aggregating->inOrder() ? StreamOrderUse::Required : StreamOrderUse::Ignored;

    if (const auto * sorting = typeid_cast<const SortingStep *>(&step))
        return sorting->getType() == SortingStep::Type::Full ? StreamOrderUse::Ignored : StreamOrderUse::Required;

    return StreamOrderUse::Required;
}

}

void allowReadingShardsFromSeveralReplicas(QueryPlan::Node & root)
{
    std::unordered_map<ReadFromRemote *, bool> allowed;
    Stack stack;

    traverseQueryPlan(stack, root, [&](QueryPlan::Node & node)
    {
        auto * reading = typeid_cast<ReadFromRemote *>(node.step.get());
        if (!reading)
            return;

        /// The consumer of the plan root is outside of the plan.
        bool allow = false;
        for (size_t i = stack.size() - 1; i > 0; --i)
        {
            const auto use = getStreamOrderUse(*stack[i - 1].node->step);
            if (use == StreamOrderUse::PassedOn)
                continue;
            allow = use == StreamOrderUse::Ignored;
            break;
        }

        auto [it, inserted] = allowed.emplace(reading, allow);
        if (!inserted)
            it->second = it->second && allow;
    });

    for (const auto & [reading, allow] : allowed)
        reading->allowReadingShardFromSeveralReplicas(allow);
}

}
