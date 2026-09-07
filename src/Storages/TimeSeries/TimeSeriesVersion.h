#pragma once

#include <base/types.h>


namespace DB
{

class StorageTimeSeries;

/// Versioning of TimeSeries tables.
///
/// The set of the target tables and their structure can change between ClickHouse versions, so every TimeSeries
/// table stores its version in the `version` setting, pinned into its CREATE query at creation
/// (see normalizeTimeSeriesDefinition). A definition without the setting is upgraded to version 0 on ATTACH.
///
/// Version history:
///   0 - Tables created before the `version` setting was introduced (including "prealpha" tables
///       and tables without the recent samples table).
///   1 - The `version` setting was introduced.
namespace TimeSeriesVersion
{
    /// The latest version, new tables get it unless the CREATE query specifies another supported version.
    /// Bump it each time the schema of the target tables or the semantics of the stored data changes;
    /// every version in [MIN_SUPPORTED, LATEST] must stay supported, so either make the schema generation
    /// version-aware or bump MIN_SUPPORTED too.
    constexpr UInt64 LATEST = 1;

    /// The minimum version which can be read with SELECT and whose creation can be replayed on another node.
    /// A table with an older version can still be attached, inspected with SHOW CREATE TABLE and dropped.
    constexpr UInt64 MIN_SUPPORTED = 0;

    /// The minimum version which can be written into (INSERT, Prometheus remote-write).
    /// Older supported tables are read-only, so the data can be copied out of them with INSERT-SELECT.
    constexpr UInt64 MIN_WRITABLE = 0;

    /// The minimum version supported by the PromQL execution layer (the `prometheusQuery`, `prometheusQueryRange`
    /// and `timeSeriesSelector` table functions, the `promql` dialect, and the Prometheus HTTP query API).
    /// The PromQL layer may support fewer versions than the table engine itself.
    constexpr UInt64 MIN_SUPPORTED_BY_PROMQL = 0;

    static_assert(MIN_SUPPORTED <= MIN_WRITABLE);
    static_assert(MIN_WRITABLE <= LATEST);
    static_assert(MIN_SUPPORTED <= MIN_SUPPORTED_BY_PROMQL);
    static_assert(MIN_SUPPORTED_BY_PROMQL <= LATEST);
}

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
