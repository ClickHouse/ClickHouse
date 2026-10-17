#pragma once

#include <base/types.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTCreateQuery;

/// Functions reading and writing the settings of a TimeSeries table in the SETTINGS clause of its CREATE TABLE query.
/// They work on the AST only (without the `TimeSeriesSettings` struct), so they can be used while formatting the query
/// and before the definition is normalized (see normalizeTimeSeriesDefinition).

/// Returns the value of `version` from the SETTINGS clause of a CREATE TABLE ... ENGINE=TimeSeries query,
/// or the latest version if the query doesn't specify it (the normalization pins an explicit version
/// into every query, so an absent setting means a new table getting the latest version).
UInt64 getTimeSeriesVersion(const ASTCreateQuery & query);

/// Whether a CREATE TABLE ... ENGINE=TimeSeries query has `version` in its SETTINGS clause.
bool hasExplicitTimeSeriesVersion(const ASTCreateQuery & query);

/// Sets `version` in the SETTINGS clause of a CREATE TABLE ... ENGINE=TimeSeries query,
/// creating the SETTINGS clause if the query doesn't have one yet.
/// The function just pins the version, it doesn't normalize the query to that version.
void setTimeSeriesVersion(ASTCreateQuery & query, UInt64 version);


/// Returns the value of `recent_samples_ttl_seconds` from the SETTINGS clause of a
/// CREATE TABLE ... ENGINE=TimeSeries query, or the setting's default value if the query
/// doesn't specify it (the normalization pins an explicit value into every query except
/// the initial CREATE query, so an absent setting means a new table getting the default).
/// A query restored from a backup (`for_restore`) can come from a version before the setting existed,
/// where the absent setting means zero (see convertDefinitionWithoutRecentSamplesTTL).
UInt64 getTimeSeriesRecentSamplesTTL(const ASTCreateQuery & query, bool for_restore = false);

/// Whether a CREATE TABLE ... ENGINE=TimeSeries query has `recent_samples_ttl_seconds` in its SETTINGS clause.
bool hasExplicitTimeSeriesRecentSamplesTTL(const ASTCreateQuery & query);

/// Whether a CREATE TABLE ... ENGINE=TimeSeries query enables the optional "recent samples" target table:
/// by a written `recent_samples_ttl_seconds`, otherwise by a RECENT SAMPLES clause, otherwise by the default TTL
/// (a backup without both was made before the table existed, `for_restore`).
/// For an existing table prefer `StorageTimeSeries::hasTarget`, which needs neither normalization nor a supported version.
bool isTimeSeriesRecentSamplesTargetEnabled(const ASTCreateQuery & query, bool for_restore = false);


/// Whether a CREATE TABLE ... ENGINE=TimeSeries query enables the optional "time ranges" target table:
/// by a written `store_time_ranges`, otherwise by a TIME RANGES clause, otherwise by the default for a version having
/// this table (a backup without both was made by a version without it, `for_restore`).
/// For an existing table prefer `StorageTimeSeries::hasTarget`, which needs neither normalization nor a supported version.
bool isTimeSeriesTimeRangesTargetEnabled(const ASTCreateQuery & query, bool for_restore = false);


/// Whether a CREATE TABLE ... ENGINE=TimeSeries query declares a target in a form written by users: inner columns,
/// an inner engine, or an external table. An inner UUID doesn't count: it's stamped by UUID generation,
/// which can happen before normalization (e.g. for an ON CLUSTER query using an old DDL entry format).
bool hasTimeSeriesTargetDefinition(const ASTCreateQuery & query, ViewTarget::Kind kind);

/// Returns the number of inner target tables created by a CREATE TABLE ... ENGINE=TimeSeries query: the mandatory targets
/// ("samples", "tags", "metric families") and the optional targets enabled by the settings (see the two functions above,
/// also for `for_restore`), without the targets which are external tables.
/// The function reads the settings of the query only: for a query with an `AS source_table` clause it doesn't check the
/// definition of the source table, the settings copied from it are seen once the query is normalized (see normalizeTimeSeriesDefinition).
size_t countTimeSeriesInnerTables(const ASTCreateQuery & query, bool for_restore = false);


namespace detail
{
    /// The default values of the settings read by the functions above, the same as in the declarations of the settings
    /// (see TimeSeriesSettings.cpp, which asserts that they are equal).
    constexpr UInt64 TIME_SERIES_RECENT_SAMPLES_TTL_SECONDS_DEFAULT = 345600;
    constexpr bool TIME_SERIES_STORE_TIME_RANGES_DEFAULT = true;
}

}
