#pragma once

#include <Parsers/Prometheus/TimeSeriesVersion.h>


namespace DB
{
class StorageTimeSeries;

/// Whether a version is in the range [MIN_SUPPORTED, LATEST].
bool isTimeSeriesVersionSupported(UInt64 version);

/// Checks that the version of a TimeSeries table is in the range [MIN_SUPPORTED, LATEST], throws otherwise.
/// A table with a newer version can appear after a downgrade of ClickHouse; it can still be attached,
/// inspected and dropped, but the server must not read, write or alter it (that could corrupt data
/// which only a newer server understands).
/// The check is used by SELECT and ALTER queries, and by the other checks below.
void checkTimeSeriesVersionIsSupported(const StorageTimeSeries & time_series_storage);

/// Checks that the version of a TimeSeries table is in the range [MIN_WRITABLE, LATEST], throws otherwise.
/// The check is used by INSERT queries and the Prometheus remote-write protocol.
void checkTimeSeriesVersionIsWritable(const StorageTimeSeries & time_series_storage);

/// Checks that the version of a TimeSeries table is in the range [MIN_SUPPORTED_BY_PROMQL, LATEST], throws otherwise.
/// The check is used by every PromQL evaluation path: the `prometheusQuery`, `prometheusQueryRange` and
/// `timeSeriesSelector` table functions, the `promql` dialect, and the Prometheus HTTP query API.
void checkTimeSeriesVersionSupportedByPromQL(const StorageTimeSeries & time_series_storage);

}
