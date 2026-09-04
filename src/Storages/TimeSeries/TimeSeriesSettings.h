#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingFieldASTFunction.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTCreateQuery;
class ASTStorage;
class SettingsChanges;
struct TimeSeriesSettingsImpl;

/// List of available types supported in TimeSeriesSettings object
#define TIMESERIES_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ASTFunction) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Map) \
    M(CLASS_NAME, UInt64) \

TIMESERIES_SETTINGS_SUPPORTED_TYPES(TimeSeriesSettings, DECLARE_SETTING_TRAIT)

/// Settings for the TimeSeries table engine.
/// Could be loaded from a CREATE TABLE query (SETTINGS clause). For example:
/// CREATE TABLE mytable ENGINE = TimeSeries() SETTINGS tags_to_columns = {'job':'job', 'instance':'instance'} SAMPLES ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
struct TimeSeriesSettings
{
    TimeSeriesSettings();
    TimeSeriesSettings(const TimeSeriesSettings & settings);
    TimeSeriesSettings(TimeSeriesSettings && settings) noexcept;
    TimeSeriesSettings & operator=(TimeSeriesSettings && settings) noexcept;
    ~TimeSeriesSettings();

    TIMESERIES_SETTINGS_SUPPORTED_TYPES(TimeSeriesSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// Loads the settings from a CREATE TABLE query (SETTINGS clause).
    void loadFromQuery(const ASTStorage & storage_def);

    /// Saves the settings to a CREATE TABLE query (SETTINGS clause), keeping any pre-existing entries.
    void copyToQuery(ASTStorage & storage_def) const;

    /// Returns only the settings that were explicitly changed from their defaults.
    SettingsChanges changes() const;

    /// Applies a list of settings changes, overwriting any existing values.
    void applyChanges(const SettingsChanges & changes);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<TimeSeriesSettingsImpl> impl;
};

/// Checks that the combination of settings is consistent.
void checkTimeSeriesSettings(const TimeSeriesSettings & settings);

/// Whether a CREATE TABLE ... ENGINE=TimeSeries query has `recent_samples_ttl_seconds` in its SETTINGS clause.
bool hasExplicitTimeSeriesSettingRecentSamplesTTL(const ASTCreateQuery & query);

/// Returns the value of `recent_samples_ttl_seconds` from the SETTINGS clause of a
/// CREATE TABLE ... ENGINE=TimeSeries query, or the setting's default value if the query
/// doesn't specify it (the normalization pins an explicit value into every query except
/// the initial CREATE query, so an absent setting means a new table getting the default).
/// A non-zero result means the query enables the optional "recent samples" target table.
UInt64 getTimeSeriesSettingRecentSamplesTTL(const ASTCreateQuery & query);

}
