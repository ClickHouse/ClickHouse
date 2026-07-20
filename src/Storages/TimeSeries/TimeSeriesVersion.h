#pragma once

#include <base/types.h>


namespace DB
{

class StorageTimeSeries;

/// Versioning of TimeSeries tables.
///
/// The TimeSeries table engine and the PromQL execution layer are under active development:
/// the set of the target tables and their structure can change between ClickHouse versions.
/// To make such changes detectable, every TimeSeries table stores its version in the `version` setting,
/// which is stamped into its CREATE query when the table is created (see normalizeTimeSeriesDefinition)
/// and persisted in the table metadata.
///
/// NOTE: The default value of the `version` setting (see TimeSeriesSettings) must always stay equal to 1:
/// tables created before the setting was introduced don't have it in their metadata and must resolve to version 1.
namespace TimeSeriesVersion
{
    /// The initial version, and the version implied when the metadata of a table doesn't contain
    /// the `version` setting. This constant must never change - it matches the pinned default value
    /// of the `version` setting.
    constexpr UInt64 INITIAL = 1;

    /// The latest version of TimeSeries tables known to this server. New tables are always created with this version.
    /// Bump this constant each time the schema of the target tables or the semantics of the stored data changes.
    ///
    /// Version history:
    ///   1 - Three target tables: `samples` (id, timestamp, value), `tags` (id, metric_name, tags, ...),
    ///       `metrics` (metric_family_name, type, unit, help). Tables created before the `version` setting
    ///       was introduced (including "prealpha" tables upgraded on ATTACH) belong to this version too.
    constexpr UInt64 LATEST = 1;

    /// The minimum version of a TimeSeries table which the PromQL execution layer is able to work with.
    /// The PromQL layer targets only the latest schema, it doesn't support all the possible older schemas.
    /// Bump this constant each time the PromQL layer stops understanding older tables - then queries over
    /// such tables are rejected with an instructive error asking to re-create the table.
    constexpr UInt64 MIN_SUPPORTED_BY_PROMQL = 1;
}

/// Checks that the version of a TimeSeries table is supported by the PromQL execution layer, throws otherwise.
/// The check is used by every PromQL evaluation path: the `prometheusQuery`, `prometheusQueryRange` and
/// `timeSeriesSelector` table functions, the `promql` dialect, and the Prometheus HTTP query API.
void checkTimeSeriesVersionSupportedByPromQL(const StorageTimeSeries & time_series_storage);

/// Checks that the version of a TimeSeries table is not newer than the latest version known to this server,
/// throws otherwise. Such a table can appear after a downgrade of ClickHouse; it can still be attached,
/// inspected and dropped, but the server must not write into it or alter it (that could corrupt data
/// which only a newer server understands). Tables of older versions stay writable so that the data
/// can keep flowing while they are being migrated.
void checkTimeSeriesVersionIsKnown(const StorageTimeSeries & time_series_storage);

}
