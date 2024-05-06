#pragma once

#include <Core/BaseSettings.h>


namespace DB
{
class ASTStorage;

#define LIST_OF_TIME_SERIES_SETTINGS(M, ALIAS) \
    M(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS)

/// Settings for the TimeSeries table engine.
/// Could be loaded from a CREATE TABLE query (SETTINGS clause). For example:
/// CREATE TABLE mytable ENGINE = TimeSeries() SETTINGS tags_to_columns = {'job':'job', 'instance':'instance'} DATA ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
struct TimeSeriesSettings : public BaseSettings<TimeSeriesSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

}
