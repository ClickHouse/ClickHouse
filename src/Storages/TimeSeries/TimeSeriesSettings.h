#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingFieldASTFunction.h>
#include <Core/SettingFieldDataType.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
class SettingsChanges;
struct TimeSeriesSettingsImpl;

/// List of available types supported in TimeSeriesSettings object
#define TIMESERIES_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ASTFunction) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, DataType) \
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

    /// Whether the setting was explicitly set, even if to its default value.
    bool isChanged(std::string_view name) const;

    /// Whether the table has the "recent samples" target.
    bool isRecentSamplesTargetEnabled() const;

    /// Whether the table has the "time ranges" target.
    bool isTimeRangesTargetEnabled() const;

    /// Whether the "tags" table stores the columns `min_time` and `max_time`: tables of earlier versions
    /// with the `store_min_time_and_max_time` setting enabled (see TimeSeriesVersion.h).
    bool hasMinTimeAndMaxTimeInTagsTable() const;

    /// Whether the generated sorting key of the inner "tags" table contains `min_time` and `max_time`:
    /// they are stored in that table but not aggregated.
    bool hasMinTimeAndMaxTimeInTagsSortingKey() const;

private:
    std::unique_ptr<TimeSeriesSettingsImpl> impl;
};

/// Checks that the combination of settings is consistent.
void checkTimeSeriesSettings(const TimeSeriesSettings & settings);

}
