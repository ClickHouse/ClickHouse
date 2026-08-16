#include <Common/CurrentMetrics.h>


// clang-format off
/// Available metrics. Add something here as you wish.
/// If the metric is generic (i.e. not server specific)
/// it should be also added to src/Coordination/KeeperConstant.cpp
#include <Common/CurrentMetricsList.inc>

#ifdef APPLY_FOR_EXTERNAL_METRICS
    #define APPLY_FOR_METRICS(M) APPLY_FOR_BUILTIN_METRICS(M) APPLY_FOR_EXTERNAL_METRICS(M)
#else
    #define APPLY_FOR_METRICS(M) APPLY_FOR_BUILTIN_METRICS(M)
#endif


namespace CurrentMetrics
{
    #define M(NAME, DOCUMENTATION) extern const Metric NAME = Metric(__COUNTER__);
        APPLY_FOR_METRICS(M)
    #undef M
    constexpr Metric END = Metric(__COUNTER__);

    /// +1 to allow using END as a placeholder
    std::atomic<Value> values[END + 1] {};    /// Global variable, initialized by zeros.

    static const std::array<std::string_view, END> names =
    {
    #define M(NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_METRICS(M)
    #undef M
    };

    const std::string_view & getName(Metric event)
    {
        return names[event];
    }

    static const std::array<std::string_view, END> docs =
    {
    #define M(NAME, DOCUMENTATION) DOCUMENTATION,
        APPLY_FOR_METRICS(M)
    #undef M
    };

    const std::string_view & getDocumentation(Metric event)
    {
        return docs[event];
    }

    Metric end() { return END; }
}

#undef APPLY_FOR_METRICS
