#include <magic_enum.hpp>
#include <Common/HistogramMetrics.h>
#include <Common/Labels.h>

namespace HistogramMetrics
{
    const MetricDescriptors & collect()
    {
        static const MetricDescriptors descriptors = []()
        {
            MetricDescriptors result;

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
                            MetricDataHolder<NAME, labelEnum>::counters, \
                            MetricTraits<NAME>::buckets, \
                            &MetricDataHolder<NAME, labelEnum>::sum \
                        ); \
                    }(), ...); \
                }(std::make_index_sequence<entries.size()>{}); \
            }

            APPLY_TO_METRICS(M)
            #undef M

            return result;
        }();
        return descriptors;
    }
}

#undef APPLY_TO_METRICS
#undef BB
