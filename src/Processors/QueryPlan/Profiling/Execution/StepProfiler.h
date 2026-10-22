#pragma once

#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>

#include <Common/Stopwatch.h>
#include <base/types.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <utility>

namespace DB
{

class IQueryPlanStep;
class QueryPlan;

/// Keyed by the step's uniq id rather than its address: the clocks are read while the query runs
/// and once it has finished, and by then the plan the steps belonged to may be long gone. The
/// processors carry the same id, so they can find their clock without the step.
using PlanStepGroup = std::pair<String, size_t>;
using StepWallClocks = std::map<PlanStepGroup, std::unique_ptr<StepWallClock>>;

/// Everything the statistics of a query are produced from: the clocks the processors stamp while
/// it runs, and the analyzer each step handed out for shaping what they measured.
///
/// Both come from one walk of the plan, taken while the plan is alive, and both outlive it. The
/// pipeline holds this, so a step's timings and its analyzer stay reachable from the same place
/// for as long as the statistics can still be asked for.
class StepProfiler
{
public:
    /// `only_built_child_plans` keeps the walk from asking a step to build child plans it does not
    /// already hold, which would make profiling change the work the query does. `EXPLAIN` wants
    /// those plans and so passes false.
    StepProfiler(const QueryPlan & plan, bool collect_work_intervals_, bool only_built_child_plans = false);

    StepWallClock * findClockForStep(const String & step_id, size_t group) const;

    const PlanAnalysisSnapshot & getPlanSnapshot() const { return plan_snapshot; }

    /// Drops the analyzers once the statistics have been produced. They hold the `IJoin`s, and with
    /// them their hash tables, so this is not merely tidying up. The clocks and the timings stay,
    /// because a caller may still read them.
    void releaseAnalyzers();

    /// Stamped by the executor when it finishes, so the duration excludes everything the
    /// query-finish path does afterwards -- serializing and sending the result, in particular.
    void markExecutionFinished();

    /// How long the query executed. Zero until the executor has finished.
    UInt64 getExecutionTimeNs() const { return execution_time_ns.load(std::memory_order_acquire); }
    UInt64 getExecutionStartNs() const { return execution_start_ns; }

    bool needCollectWorkIntervals() const;
    void addWorkIntervals(WorkIntervals intervals);
    WorkIntervalsPerThread extractWorkIntervals(UInt64 rebase_origin_ns);

private:
    const bool collect_work_intervals;

    /// Declared before `clocks`: the clocks are keyed by the stages the analyzers report.
    PlanAnalysisSnapshot plan_snapshot;
    const StepWallClocks clocks;

    const UInt64 execution_start_ns = clock_gettime_ns();
    std::atomic<UInt64> execution_time_ns = 0;

    std::mutex mutex;
    WorkIntervalsPerThread intervals_per_thread;
};

using StepProfilerPtr = std::shared_ptr<StepProfiler>;

}
