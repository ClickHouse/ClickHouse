#pragma once

#include <base/types.h>
#include <vector>

namespace DB
{

class IProcessor;

struct WorkInterval
{
    UInt64 start_of_interval_ns;
    UInt64 duration_of_interval_ns;
    const IProcessor * processor;
};

using WorkIntervals = std::vector<WorkInterval>;

}
