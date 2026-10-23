#pragma once

#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct WorkInterval
{
    UInt64 start_of_interval_ns;
    UInt64 duration_of_interval_ns;

    /// `IQueryPlanStep::getStepIndex` of the step the processor belongs to, rather than the step
    /// itself: the intervals are read once the query has run, by which time the plan may be gone.
    /// `PlanStepIndex::NONE` for the processors that belong to no step.
    UInt64 step_id;
};

using WorkIntervals = VectorWithMemoryTracking<WorkInterval>;

using WorkIntervalsPerThread = VectorWithMemoryTracking<WorkIntervals>;

}
