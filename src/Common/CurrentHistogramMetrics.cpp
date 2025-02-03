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
    #define M(NAME, DOCUMENTATION, ...) extern const Metric NAME = Metric(__COUNTER__);
        APPLY_TO_METRICS(M)
    #undef M

    #define M(NAME, DOCUMENTATION, ...) + impl::va_count(__VA_ARGS__) + 1 // NOLINT(bugprone-macro-parentheses)
    constexpr size_t TOTAL_BUCKETS =
        0 + APPLY_TO_METRICS(M);
    #undef M

    std::atomic<CountType> data[TOTAL_BUCKETS]{};

    template<typename ... Buckets>
    MetricInfo prepareMetricInfo(Buckets ... buckets)
    {
        static size_t offset = 0;
        constexpr size_t bucket_count = impl::va_count(buckets...) + 1;
        MetricInfo metric_info;
        metric_info.counts = std::span(data + offset, bucket_count);
        metric_info.buckets = std::vector<Value>{ buckets... };
        offset += bucket_count;
        return metric_info;
    }

    #define M(NAME, DOCUMENTATION, ...) prepareMetricInfo(__VA_ARGS__),
    MetricInfo metrics [] = {
        APPLY_TO_METRICS(M)
    };
    #undef M

    inline void observe(Metric metric, Value value)
    {
        MetricInfo & info = metrics[metric];
        const auto it = std::lower_bound(info.buckets.begin(), info.buckets.end(), value);
        const size_t bucket_idx = std::distance(info.buckets.begin(), it);
        info.counts[bucket_idx].fetch_add(1, std::memory_order_relaxed);
    }
}
