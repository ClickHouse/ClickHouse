#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
struct TimeSeriesSettingsImpl;

/// List of available types supported in TimeSeriesSettings object
#define TIMESERIES_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Map)

TIMESERIES_SETTINGS_SUPPORTED_TYPES(TimeSeriesSettings, DECLARE_SETTING_TRAIT)

/// Settings for the TimeSeries table engine.
/// Could be loaded from a CREATE TABLE query (SETTINGS clause). For example:
/// CREATE TABLE mytable ENGINE = TimeSeries() SETTINGS tags_to_columns = {'job':'job', 'instance':'instance'} DATA ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
struct TimeSeriesSettings
{
    TimeSeriesSettings();
    TimeSeriesSettings(const TimeSeriesSettings & settings);
    TimeSeriesSettings(TimeSeriesSettings && settings) noexcept;
    ~TimeSeriesSettings();

    TIMESERIES_SETTINGS_SUPPORTED_TYPES(TimeSeriesSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);

private:
    std::unique_ptr<TimeSeriesSettingsImpl> impl;
};
}
