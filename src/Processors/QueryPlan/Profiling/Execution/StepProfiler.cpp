#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace
{

/// One clock per stage of every step the snapshot found, so the walk of the plan happens once.
StepWallClocks collectWallClocksForPlanSteps(const PlanAnalysisSnapshot & plan_snapshot)
{
    StepWallClocks clocks;

    for (const auto & [step_id, analyzer] : plan_snapshot.analyzers)
        for (size_t group : analyzer.groups)
            clocks.try_emplace(std::make_pair(step_id, group), std::make_unique<StepWallClock>());

    return clocks;
}

}

StepProfiler::StepProfiler(const QueryPlan & plan, bool collect_work_intervals_, bool only_built_child_plans)
    : collect_work_intervals(collect_work_intervals_)
    , plan_snapshot(PlanAnalysisSnapshot::capture(plan, only_built_child_plans))
    , clocks(collectWallClocksForPlanSteps(plan_snapshot))
{
}

StepWallClock * StepProfiler::findClockForStep(const String & step_id, size_t group) const
{
    auto it = clocks.find({step_id, group});
    return it != clocks.end() ? it->second.get() : nullptr;
}

void StepProfiler::markExecutionFinished()
{
    execution_time_ns.store(clock_gettime_ns() - execution_start_ns, std::memory_order_release);
}

bool StepProfiler::needCollectWorkIntervals() const
{
    return collect_work_intervals;
}

void StepProfiler::addWorkIntervals(WorkIntervals intervals)
{
    if (intervals.empty())
        return;

    std::lock_guard lock(mutex);
    intervals_per_thread.push_back(std::move(intervals));
}

WorkIntervalsPerThread StepProfiler::extractWorkIntervals(UInt64 rebase_origin_ns)
{
    std::lock_guard lock(mutex);

    for (auto & thread_intervals : intervals_per_thread)
        for (auto & interval : thread_intervals)
            interval.start_of_interval_ns -= rebase_origin_ns;

    return std::move(intervals_per_thread);
}

}
