#pragma once

#include <base/types.h>
#include <vector>

namespace DB
{

struct WorkInterval
{
    UInt64 start_of_interval_ns;
    UInt64 duration_of_interval_ns;

    /// `IQueryPlanStep::getUniqID` of the step the processor belongs to, rather than the step
    /// itself: the intervals are read once the query has run, by which time the plan may be gone.
    /// Empty for the processors that belong to no step.
    String step_id;
};

using WorkIntervals = std::vector<WorkInterval>;

using WorkIntervalsPerThread = std::vector<WorkIntervals>;

}
