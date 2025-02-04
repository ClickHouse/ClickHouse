#pragma once

#include <cstddef>
#include <span>
#include <array>
#include <atomic>
#include <cassert>
#include <base/types.h>
#include <base/strong_typedef.h>

namespace CurrentHistogramMetrics
{
    using Metric = StrongTypedef<size_t, struct MetricTag>;
    using Value = Int64;
    using AtomicCounter = std::atomic<UInt64>;
    using AtomicSum = std::atomic<Value>;

    struct MetricInfo
    {
        const std::string name;
        const std::string documentation;
        const std::vector<Value> buckets;
        std::span<AtomicCounter> counters;
    };

    extern AtomicCounter data[];
    extern AtomicSum sums[];
    extern MetricInfo metrics[];

    const char * getName(Metric metric);
    const char * getDocumentation(Metric metric);

    inline void observe(Metric metric, Value value);

    Metric end();
}
