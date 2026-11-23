#pragma once

#include <base/types.h>


namespace DB
{
class IColumn;

/// Conversions between Prometheus timestamps (milliseconds since the epoch) and the timestamps of a TimeSeries table: `DateTime64`
/// with a scale, or `DateTime` / `UInt32` with the scale 0. They throw INCORRECT_DATA for a timestamp the other side can't represent.
namespace PrometheusTimestamps
{
    /// The scale of a Prometheus timestamp as a `DateTime64`: milliseconds.
    constexpr UInt32 SCALE = 3;

    /// The Prometheus timestamp of a stored timestamp with the scale `scale`.
    Int64 fromStored(Int64 timestamp, UInt32 scale);

    /// Appends a Prometheus timestamp to a column of the timestamp type of a TimeSeries table.
    void insertInto(Int64 timestamp_ms, IColumn & column);
}

}
