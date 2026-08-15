#include <Common/CurrentMetrics.h>


// clang-format off
/// The list of metrics lives in its own file so that `.gitattributes` can mark it `merge=union`.
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
