#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/IProcessor.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

namespace
{

/// One clock per stage of every step the snapshot found.
StepWallClocks collectWallClocksForPlanSteps(const PlanAnalysisSnapshot & plan_snapshot)
{
    StepWallClocks clocks;

    for (const auto & [step_id, analyzer] : plan_snapshot.analyzers)
        for (const auto & group : analyzer.groups)
            clocks.try_emplace(std::make_pair(step_id, group.id), std::make_unique<StepWallClock>());

    return clocks;
}

}

StepProfiler::StepProfiler(const PlanWalk & walk, bool collect_work_intervals_)
    : collect_work_intervals(collect_work_intervals_)
    , plan_snapshot(PlanAnalysisSnapshot::capture(walk))
    , clocks(collectWallClocksForPlanSteps(plan_snapshot))
{
}

StepWallClock * StepProfiler::findClockForStep(const String & step_id, size_t group) const
{
    auto it = clocks.find({step_id, group});
    return it != clocks.end() ? it->second.get() : nullptr;
}

void StepProfiler::markExecutionStarted()
{
    execution_start_ns.store(clock_gettime_ns(), std::memory_order_release);
}

void StepProfiler::markExecutionFinished()
{
    /// Zero until the executor starts, and a pipeline may be finalized without ever having run.
    const UInt64 started_at = execution_start_ns.load(std::memory_order_acquire);
    if (started_at == 0)
        return;

    execution_time_ns.store(clock_gettime_ns() - started_at, std::memory_order_release);
}

bool StepProfiler::needCollectWorkIntervals() const
{
    return collect_work_intervals;
}

StepFailure StepFailure::of(const IProcessor & processor)
{
    return {.step_id = processor.getStepUniqID(), .step_group = processor.getQueryPlanStepGroup(), .processor_name = processor.getName()};
}

void StepProfiler::addPassedOnException(std::exception_ptr exception, const IProcessor & processor)
{
    auto entry = std::make_pair(std::move(exception), StepFailure::of(processor));
    std::lock_guard lock(mutex);
    passed_on_exceptions.push_back(std::move(entry));
}

void StepProfiler::recordFailure(const std::exception_ptr & cause, const IProcessor * thrower)
{
    std::lock_guard lock(mutex);
    if (failure)
        return;

    /// The same exception object travels through the ports and is rethrown as is, so it is found by
    /// identity. Only the processor it started from registers it: the ones that forward it do not.
    for (const auto & [exception, origin] : passed_on_exceptions)
    {
        if (exception == cause)
        {
            failure = origin;
            return;
        }
    }

    if (thrower)
        failure = StepFailure::of(*thrower);
}

std::optional<StepFailure> StepProfiler::getFailure() const
{
    std::lock_guard lock(mutex);
    return failure;
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
