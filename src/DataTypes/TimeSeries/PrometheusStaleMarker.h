#pragma once

#include <base/types.h>

#include <bit>


namespace DB
{

/// The bits of the NaN which Prometheus stores as the value of a sample to mark the end of a time series.
constexpr UInt64 PROMETHEUS_STALE_MARKER_BITS = 0x7ff0000000000002ULL;

inline bool isPrometheusStaleMarker(Float64 value)
{
    return std::bit_cast<UInt64>(value) == PROMETHEUS_STALE_MARKER_BITS;
}

}
