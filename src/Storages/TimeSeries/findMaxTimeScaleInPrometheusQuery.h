#pragma once

#include <base/types.h>


namespace DB
{
    class PrometheusQueryTree;
}


namespace DB
{

/// Finds all time durations in a prometheus query (for example "50ms" in query "up[15ms]")
/// and returns the maximal decimal scale enough to represent all of them.
/// When a float literal is used to represent a time duration (for example, "0.05" in query "up[0.05]")
/// the function doesn't recognize that.
/// Thus the function returns either 0 or 3 (there is no promql syntax for microseconds or nanoseconds).
UInt32 findMaxTimeScaleInPrometheusQuery(const PrometheusQueryTree & promql_tree);

}
