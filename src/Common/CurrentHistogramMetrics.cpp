#include <magic_enum.hpp>
#include <Common/CurrentHistogramMetrics.h>

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
        return std::array<CurrentHistogramMetrics::Value, sizeof...(Args)>{args...};
    }
}

namespace Labels
{
    enum class KeeperOperation { Exists, Get, Create, List };
}

/// TODO(mstetsyuk): support any arbitrary number of labels per metric.

/// Supports exactly one label per metric.
#define APPLY_TO_METRICS(M) \
    M(KeeperResponseTime, "The response time of ClickHouse Keeper, in milliseconds", impl::make_buckets(1, 2, 5, 10, 20, 50), KeeperOperation) \
    M(TestingTMP, "Just a temporary second metric for testing the macros", impl::make_buckets(1, 2, 5, 10, 20, 50), KeeperOperation) \

namespace CurrentHistogramMetrics
{
    #define M(NAME, DOCUMENTATION, BUCKETS, LABEL_NAME) extern const Metric NAME = __COUNTER__;
    APPLY_TO_METRICS(M)
    #undef M

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
    void observe(Value value) {
        const auto & buckets = MetricTraits<m>::buckets;
        const size_t bucket_idx = std::distance(
            buckets.begin(),
            std::lower_bound(buckets.begin(), buckets.end(), value)
        );
        
        MetricState<m, label>::data[bucket_idx].fetch_add(1);
        MetricState<m, label>::sum.fetch_add(value);
    }

    const std::vector<MetricStatus> & getStatus()
    {
        static const std::vector<MetricStatus> status = []()
        {
            std::vector<MetricStatus> result;

            #define M(NAME, DOCUMENTATION, BUCKETS, LABEL_NAME) \
            { \
                constexpr auto entries = magic_enum::enum_entries<Labels::LABEL_NAME>(); \
                [&]<size_t... Is>(std::index_sequence<Is...>) { \
                    ([&]{ \
                        constexpr auto labelEnum = entries[Is].first; \
                        constexpr auto labelValue = entries[Is].second; \
                        result.emplace_back( \
                            #NAME, \
                            #DOCUMENTATION, \
                            std::pair{#LABEL_NAME, std::string(labelValue)}, \
                            MetricState<NAME, labelEnum>::data, \
                            MetricTraits<NAME>::buckets, \
                            &MetricState<NAME, labelEnum>::sum \
                        ); \
                    }(), ...); \
                }(std::make_index_sequence<entries.size()>{}); \
            }

            APPLY_TO_METRICS(M)
            #undef M

            return result;
        }();
        return status;
    }
}

#undef APPLY_TO_METRICS
#undef BB
