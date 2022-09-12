#pragma once

#include <Common/Stopwatch.h>

#include <algorithm>

namespace DB
{

class PrefetchingHelper
{
public:
    size_t calcPrefetchLookAhead()
    {
        static constexpr size_t assumed_load_latency_ns = 100;
        static constexpr size_t just_coefficient = 4;
        const double single_iteration_latency = std::max<double>(1.0 * watch.elapsedNanoseconds() / iterations_to_measure, 1);
        return std::clamp<size_t>(
            just_coefficient * assumed_load_latency_ns / single_iteration_latency, min_look_ahead_value, max_look_ahead_value);
    }

    static constexpr size_t getInitialLookAheadValue() { return min_look_ahead_value; }

    static constexpr size_t iterationsToMeasure() { return iterations_to_measure; }

private:
    static constexpr size_t iterations_to_measure = 100;
    static constexpr size_t min_look_ahead_value = 8;
    static constexpr size_t max_look_ahead_value = 32;

    Stopwatch watch;
};

}
