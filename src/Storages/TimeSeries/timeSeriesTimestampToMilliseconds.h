#pragma once

#include <base/types.h>


namespace DB
{
class IColumn;

/// Converts a timestamp of a TimeSeries table with the scale `scale` (0 for `DateTime`) to milliseconds since the epoch,
/// the timestamp format of the Prometheus protocols. Throws `INCORRECT_DATA` if the result is out of range.
Int64 timeSeriesTimestampToMilliseconds(Int64 timestamp, UInt32 scale);

/// Appends a timestamp in milliseconds since the epoch to a column of the timestamp type of a TimeSeries table.
/// Throws `INCORRECT_DATA` if that type can't represent it.
void insertMillisecondsAsTimeSeriesTimestamp(Int64 timestamp_ms, IColumn & column);

}
