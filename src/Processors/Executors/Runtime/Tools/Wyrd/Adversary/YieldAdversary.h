#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/IAdversary.h>

#include <cstddef>

namespace DB::Runtime::Wyrd
{

struct YieldAdversaryParams
{
    size_t yield_freq = 10;
};

/// Pins all threads to one core and yields the CPU at random fault points, one in `yield_freq`.
AdversaryPtr createYieldAdversary(YieldAdversaryParams params);

}
