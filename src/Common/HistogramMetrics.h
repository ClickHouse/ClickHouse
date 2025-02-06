#pragma once

#include <Common/Labels.h>

#include <algorithm>
#include <span>
#include <atomic>
#include <base/types.h>

namespace HistogramMetrics
{
    using Metric = size_t;
    using Value = Int64;

    using AtomicCounter = std::atomic<UInt64>;
    using AtomicSum = std::atomic<Value>;

    namespace impl
    {
        template<typename ... Args>
        constexpr std::size_t va_count(Args && ...)
        {
            return sizeof...(Args);
        }

        template <typename... Args>
        constexpr auto make_buckets(Args... args)
        {
            return std::array<HistogramMetrics::Value, sizeof...(Args)>{args...};
        }
    }

    /// Supports exactly one label per metric.
    /// TODO(mstetsyuk): support any arbitrary number of labels per metric.
    #define APPLY_TO_METRICS(M) \
        M(KeeperResponseTime, "The response time of ClickHouse Keeper, in milliseconds", impl::make_buckets(1, 2, 5, 10, 20, 50), KeeperOperation) \
        M(TestingTMP, "Just a temporary second metric for testing the macros", impl::make_buckets(1, 2, 5, 10, 20, 50), KeeperOperation)

    #define M(NAME, DOCUMENTATION, BUCKETS, LABEL_NAME) constexpr Metric NAME = __COUNTER__;
    APPLY_TO_METRICS(M)
    #undef M

    template <Metric m> 
    struct MetricTraits;

    #define M(NAME, DOCUMENTATION, BUCKETS, LABEL_NAME) \
    template <> \
    struct MetricTraits<NAME> { \
        using LabelType = Labels::LABEL_NAME; \
        static constexpr std::array<Value, (BUCKETS).size()> buckets = BUCKETS; \
        static constexpr size_t BUCKETS_COUNT = (BUCKETS).size() + 1; \
    };
    APPLY_TO_METRICS(M)
    #undef M

    template <Metric m, auto label>
    struct MetricDataHolder
    {
        static_assert(
            std::is_same_v<decltype(label), typename MetricTraits<m>::LabelType>,
            "The passed label has an unexpected type."
        );

        using AtomicCounters = std::array<AtomicCounter, MetricTraits<m>::BUCKETS_COUNT>;

        static inline AtomicCounters counters{};
        static inline AtomicSum sum{};
    };

    template <Metric m, auto label>
    void observe(Value value) {
        const auto & buckets = MetricTraits<m>::buckets;
        const size_t bucket_idx = std::distance(
            buckets.begin(),
            std::lower_bound(buckets.begin(), buckets.end(), value)
        );
        
        MetricDataHolder<m, label>::counters[bucket_idx].fetch_add(1);
        MetricDataHolder<m, label>::sum.fetch_add(value);
    }

    struct MetricDescriptor
    {
        const std::string name;
        const std::string documentation;
        const std::pair<std::string, std::string> label;
        const std::span<const AtomicCounter> counters;
        const std::span<const Value> buckets;
        const AtomicSum * sum;
    };
    using MetricDescriptors = std::vector<MetricDescriptor>;

    const MetricDescriptors & collect();
}
