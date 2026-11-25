#pragma once

#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>

#include <Common/Stopwatch.h>
#include <base/types.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace DB
{


/// Keyed by the step's uniq id: the clocks are read while the query runs and once it has finished,
/// and by then the plan the steps belonged to may be long gone. The processors carry the same id,
/// so they can find their clock without the step.
using PlanStepGroup = std::pair<String, size_t>;
using StepWallClocks = MapWithMemoryTracking<PlanStepGroup, std::unique_ptr<StepWallClock>>;

/// The processor whose work threw the exception that failed the query, as values: the processor
/// is gone by the time the failure is logged.
class IProcessor;

struct StepFailure
{
    /// Uniq id of the plan step the processor belongs to; empty for a processor without one.
    String step_id;
    /// The stage of the step the processor belongs to, `GroupId` in the statistics.
    size_t step_group = 0;
    String processor_name;

    static StepFailure of(const IProcessor & processor);
};

/// Everything the statistics of a query are produced from: the clocks the processors stamp while
/// it runs, and the analyzer each step handed out for shaping what they measured.
///
/// Both are read from a walk of the plan taken while it is alive, and both outlive it. The
/// pipeline holds this, so a step's timings and its analyzer stay reachable from the same place
/// for as long as the statistics can still be asked for.
class StepProfiler
{
public:
    StepProfiler(const PlanWalk & walk, bool collect_work_intervals_);

    [[nodiscard]] StepWallClock * findClockForStep(const String & step_id, size_t group) const;

    [[nodiscard]] const PlanAnalysisSnapshot & getPlanSnapshot() const { return plan_snapshot; }

    /// Stamped by the executor, so the duration is the execution alone: the pipeline is already
    /// built when it starts, and the result is sent after it ends.
    void markExecutionStarted();
    void markExecutionFinished();

    /// Set by the executor once none of its threads runs anymore, whether it finished or threw: from
    /// then on, what the processors measured is final and can be read.
    void markExecutionStopped() { execution_stopped.store(true, std::memory_order_release); }
    [[nodiscard]] bool hasExecutionStopped() const { return execution_stopped.load(std::memory_order_acquire); }

    /// How long the query executed. Zero until the executor has finished.
    [[nodiscard]] UInt64 getExecutionTimeNs() const { return execution_time_ns.load(std::memory_order_acquire); }
    [[nodiscard]] UInt64 getExecutionStartNs() const { return execution_start_ns.load(std::memory_order_acquire); }

    [[nodiscard]] bool needCollectWorkIntervals() const;
    void addWorkIntervals(WorkIntervals intervals);
    WorkIntervalsPerThread extractWorkIntervals(UInt64 rebase_origin_ns);

    /// Remembers which processor passed `exception` on through its output port instead of throwing
    /// it, so that the failure can be traced back to it once a processor downstream rethrows it.
    void addPassedOnException(std::exception_ptr exception, const IProcessor & processor);

    /// Recorded by the executor for the exception that cancels the execution. Only the first
    /// failure is kept: whatever the other processors throw after it is a consequence. The failure
    /// is the processor that passed `cause` on, if one did, and otherwise `thrower`, the processor
    /// whose work threw it, if that is known.
    void recordFailure(const std::exception_ptr & cause, const IProcessor * thrower);
    [[nodiscard]] std::optional<StepFailure> getFailure() const;

private:
    const bool collect_work_intervals;

    /// Declared before `clocks`: the clocks are keyed by the stages the analyzers report.
    const PlanAnalysisSnapshot plan_snapshot;
    const StepWallClocks clocks;

    std::atomic<UInt64> execution_start_ns = 0;
    std::atomic<UInt64> execution_time_ns = 0;
    std::atomic<bool> execution_stopped = false;

    mutable std::mutex mutex;
    WorkIntervalsPerThread intervals_per_thread;
    VectorWithMemoryTracking<std::pair<std::exception_ptr, StepFailure>> passed_on_exceptions;
    std::optional<StepFailure> failure;
};

using StepProfilerPtr = std::shared_ptr<StepProfiler>;

}
