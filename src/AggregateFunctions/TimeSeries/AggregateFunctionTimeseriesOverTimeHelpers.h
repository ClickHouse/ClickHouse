#pragma once

#include <algorithm>
#include <cmath>
#include <limits>

#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/// Shared PromQL-compatible helpers for the `_over_time` family.
///
/// Live here (and not in the per-Operation header) so that any aggregate sharing the
/// same numerical contract (currently `quantile_over_time` and `mad_over_time`, plus
/// any future PromQL aggregate that needs Prometheus-faithful quantile interpolation)
/// can include just this small file rather than pulling in every bucket type and
/// `*Operation` struct.


/// Mirrors Prometheus `promql/quantile.go` func quantile(q float64, values vectorByValueHeap) float64
/// https://github.com/prometheus/prometheus/blob/da1f89e7360a19c5de2b0df4b43411ac706a76a9/promql/quantile.go
/// Sorts `values` in place when returning an interpolated quantile (same as Go sort.Sort before indexing).
inline Float64 quantile(Float64 q, VectorWithMemoryTracking<Float64> & values)
{
    if (values.empty() || std::isnan(q))
        return std::numeric_limits<Float64>::quiet_NaN();
    if (q < 0)
        return -std::numeric_limits<Float64>::infinity();
    if (q > 1)
        return std::numeric_limits<Float64>::infinity();

    std::sort(values.begin(), values.end());

    const Float64 n = static_cast<Float64>(values.size());
    const Float64 rank = q * (n - 1.0);
    const Float64 lower_index_float = std::max(0.0, std::floor(rank));
    const size_t lower_index = static_cast<size_t>(lower_index_float);
    const size_t upper_index = std::min(values.size() - 1, lower_index + 1);
    const Float64 weight = rank - std::floor(rank);
    return values[lower_index] * (1.0 - weight) + values[upper_index] * weight;
}

}
