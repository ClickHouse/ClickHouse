#pragma once

#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>

#include <map>
#include <memory>
#include <mutex>
#include <utility>

namespace DB
{

class IQueryPlanStep;
class QueryPlan;

using PlanStepGroup = std::pair<const IQueryPlanStep *, size_t>;
using StepWallClocks = std::map<PlanStepGroup, std::unique_ptr<StepWallClock>>;

/// Statistics needed for EXPLAIN ANALYZE.
class StepProfiler
{
public:
    /// `only_built_child_plans` keeps the walk from asking a step to build child plans it does not
    /// already hold, which would make attaching the clocks change the work the query does. `EXPLAIN`
    /// wants those plans and so passes false.
    StepProfiler(const QueryPlan & plan, bool collect_work_intervals_, bool only_built_child_plans = false);

    StepWallClock * findClockForStep(const IQueryPlanStep * step, size_t group) const;

    bool needCollectWorkIntervals() const;
    void addWorkIntervals(WorkIntervals intervals);
    WorkIntervalsPerThread extractWorkIntervals(UInt64 execution_start_ns);

private:
    const bool collect_work_intervals;
    const StepWallClocks clocks;

    std::mutex mutex;
    WorkIntervalsPerThread intervals_per_thread;
};

using StepProfilerPtr = std::shared_ptr<StepProfiler>;

}
