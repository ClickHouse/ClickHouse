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
    using Metric = size_t;
    using Value = Int64;
    using AtomicCounter = std::atomic<UInt64>;
    using AtomicSum = std::atomic<Value>;

    template <Metric m> 
    struct MetricTraits;

    template <Metric m, auto label>
    struct MetricState
    {
        static_assert(
            std::is_same_v<decltype(label), typename MetricTraits<m>::LabelType>,
            "The passed label has an unexpected type."
        );

        static inline std::array<AtomicCounter, MetricTraits<m>::BUCKETS_COUNT> data{};
        static inline AtomicSum sum{};
    };

    struct MetricStatus
    {
        const std::string name;
        const std::string documentation;
        const std::pair<std::string, std::string> label;
        const std::span<AtomicCounter> counters;
        const std::span<Value> buckets;
        const AtomicSum * sum;
    };

    template <Metric m, auto label>
    void observe(Value value);

    const std::vector<MetricStatus> & getStatus();
}
