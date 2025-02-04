#include <atomic>
#include <Common/CurrentHistogramMetrics.h>


#define APPLY_TO_METRICS(M) \
    M(KeeperResponseTime, "The response time of ClickHouse Keeper, in milliseconds", 1, 2, 5, 10, 20, 50) \
    M(TestingTMP, "Just a temporary second metric for testing the macros", 1, 2, 5, 10, 20, 50) \

namespace impl
{
    template<typename ... Args>
    constexpr std::size_t va_count(Args && ...)
    {
        return sizeof...(Args);
    }
}

namespace CurrentHistogramMetrics
{
    constexpr size_t METRICS_COUNTER_START_ = __COUNTER__;
    #define M(NAME, DOCUMENTATION, ...) extern const Metric NAME = Metric(__COUNTER__);
        APPLY_TO_METRICS(M)
    #undef M
    constexpr Metric END = Metric(__COUNTER__ - METRICS_COUNTER_START_);

    #define M(NAME, DOCUMENTATION, ...) + impl::va_count(__VA_ARGS__) + 1 // NOLINT(bugprone-macro-parentheses)
    constexpr size_t TOTAL_BUCKETS =
        0 + APPLY_TO_METRICS(M);
    #undef M

    AtomicCounter data[TOTAL_BUCKETS]{};
    AtomicSum sums[END]{};

    template<typename ... Buckets>
    MetricInfo prepareMetricInfo(std::string name, std::string documentation, Buckets ... buckets)
    {
        static size_t offset = 0;
        constexpr size_t num_counters = impl::va_count(buckets...) + 1;
        MetricInfo result
        {
            .name = name,
            .documentation = documentation,
            .buckets = std::vector<Value>{ buckets... },
            .counters = std::span(data + offset, num_counters),
        };
        offset += num_counters;
        return result;
    }

    #define M(NAME, DOCUMENTATION, ...) prepareMetricInfo(#NAME, DOCUMENTATION, __VA_ARGS__),
    MetricInfo metrics [] = {
        APPLY_TO_METRICS(M)
    };
    #undef M

    inline void observe(Metric metric, Value value)
    {
        MetricInfo & info = metrics[metric];
        const auto it = std::lower_bound(info.buckets.begin(), info.buckets.end(), value);
        const size_t bucket_idx = std::distance(info.buckets.begin(), it);
        info.counters[bucket_idx].fetch_add(1, std::memory_order_relaxed);
        sums[metric].fetch_add(value, std::memory_order_relaxed);
    }

    Metric end()
    {
        return END;
    }
}
