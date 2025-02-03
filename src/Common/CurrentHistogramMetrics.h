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
    using CountType = UInt64;

    extern std::atomic<CountType> data[];

    struct MetricInfo
    {
        std::span<std::atomic<CountType>> counts;
        std::vector<Value> buckets;
    };

    const char * getName(Metric metric);
    const char * getDocumentation(Metric metric);

    inline void observe(Metric metric, Value value);
}
