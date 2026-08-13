#pragma once

#include <base/types.h>
#include <vector>

namespace DB
{

class IQueryPlanStep;

struct WorkInterval
{
    UInt64 start_of_interval_ns;
    UInt64 duration_of_interval_ns;
    const IQueryPlanStep * step;
};

using WorkIntervals = std::vector<WorkInterval>;

using WorkIntervalsPerThread = std::vector<WorkIntervals>;

}
