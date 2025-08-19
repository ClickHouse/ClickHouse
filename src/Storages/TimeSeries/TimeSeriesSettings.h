#pragma once

#include <Core/BaseSettings.h>


namespace DB
{
class ASTStorage;

#define LIST_OF_TIME_SERIES_SETTINGS(M, ALIAS) \
    M(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    M(Bool, use_all_tags_column_to_generate_id, true, "When generating an expression to calculate an identifier of a time series, this flag enables using the 'all_tags' column in that calculation. The 'all_tags' is a virtual column containing all tags except the metric name", 0) \
    M(Bool, store_min_time_and_max_time, true, "If set to true then the table will store 'min_time' and 'max_time' for each time series", 0) \
    M(Bool, aggregate_min_time_and_max_time, true, "When creating an inner target 'tags' table, this flag enables using 'SimpleAggregateFunction(min, Nullable(DateTime64(3)))' instead of just 'Nullable(DateTime64(3))' as the type of the 'min_time' column, and the same for the 'max_time' column", 0) \
    M(Bool, filter_by_min_time_and_max_time, true, "If set to true then the table will use the 'min_time' and 'max_time' columns for filtering time series", 0) \

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
