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
///   2 - The `id_type` setting was introduced: a table with an external tags table records the type of the `id` column
///       in `id_type` and the expression generating identifiers in `id_generator`, so its definition doesn't depend on
///       the external table. `id_type` is also recorded when the `id_generator` setting is set.
///   3 - The outer column `time_series` was renamed to `samples`. The stored data didn't change, and tables of earlier
///       versions keep the old name of the column (see `TimeSeriesColumnNames::getOuterSamples`).
///   4 - The "metrics" target table was renamed to "metric families": the inner table is named
///       `.inner_id.metricfamilies.<uuid>` instead of `.inner_id.metrics.<uuid>`, the same name is used in backups,
///       and the definition is written with the keyword `METRIC FAMILIES` instead of `METRICS`.
///   5 - New inner tags tables with a `MergeTree` family engine get a `keyValuePairs` text index by default.
///   6 - The column `metric_family_name` of the "metric families" target table was renamed to `metric_family`, the name of
///       the corresponding outer column. Tables of earlier versions keep the old name of the column
///       (see `TimeSeriesColumnNames::getInnerMetricFamily`).
///   7 - The "histograms" target table was introduced: every table of this version has a fifth target table
///       storing native histogram samples, written with the keyword `HISTOGRAMS`. Tables of earlier versions have none.
namespace TimeSeriesVersion
{
    /// The latest version, new tables get it unless the CREATE query specifies another supported version.
    /// Bump it each time the schema of the target tables or the semantics of the stored data changes;
    /// every version in [MIN_SUPPORTED, LATEST] must stay supported, so either make the schema generation
    /// version-aware or bump MIN_SUPPORTED too.
    constexpr UInt64 LATEST = 7;

    /// The first version recording the `id_type` setting (see the version history above).
    /// A table of an earlier version must not have the setting: an older server wouldn't understand it.
    constexpr UInt64 MIN_WITH_ID_TYPE_SETTING = 2;

    /// The first version naming the outer column with samples `samples` instead of `time_series` (see the version history above).
    constexpr UInt64 MIN_WITH_SAMPLES_OUTER_COLUMN = 3;

    /// The first version creating a text index on the `tags` map by default.
    constexpr UInt64 MIN_WITH_TAGS_TEXT_INDEX = 5;

    /// The first version naming the column of the "metric families" target table with the name of a metric family
    /// `metric_family` instead of `metric_family_name` (see the version history above).
    constexpr UInt64 MIN_WITH_METRIC_FAMILY_INNER_COLUMN = 6;

    /// The first version with the "histograms" target table.
    constexpr UInt64 MIN_WITH_HISTOGRAMS_TARGET = 7;

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

    /// The first version whose "metric families" target is named "metricfamilies" in the names of inner tables
    /// and in backups, and is written with the keyword `METRIC FAMILIES` in the definition.
    /// The earlier versions name it "metrics" and write it with the keyword `METRICS`, so an older server can read them.
    constexpr UInt64 MIN_WITH_METRIC_FAMILIES_TARGET_NAME = 4;

    static_assert(MIN_SUPPORTED <= MIN_WRITABLE);
    static_assert(MIN_WITH_ID_TYPE_SETTING <= LATEST);
    static_assert(MIN_WITH_SAMPLES_OUTER_COLUMN <= LATEST);
    static_assert(MIN_WITH_TAGS_TEXT_INDEX <= LATEST);
    static_assert(MIN_WITH_METRIC_FAMILY_INNER_COLUMN <= LATEST);
    static_assert(MIN_WITH_HISTOGRAMS_TARGET <= LATEST);
    static_assert(MIN_WRITABLE <= LATEST);
    static_assert(MIN_SUPPORTED <= MIN_SUPPORTED_BY_PROMQL);
    static_assert(MIN_SUPPORTED_BY_PROMQL <= LATEST);
    static_assert(MIN_WITH_METRIC_FAMILIES_TARGET_NAME <= LATEST);
}

/// Whether a version is in the range [MIN_SUPPORTED, LATEST].
bool isTimeSeriesVersionSupported(UInt64 version);

/// Whether tables of the specified version have the histograms target (and the outer `histograms.*` columns).
inline bool timeSeriesVersionSupportsHistograms(UInt64 version)
{
    return version >= TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET;
}

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
