#pragma once

#include <Core/BaseSettings.h>


namespace DB
{
class ASTStorage;

#define LIST_OF_TIME_SERIES_SETTINGS(M, ALIAS) \
    M(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    M(Bool, use_all_tags_column_for_calculating_id, true, "Enables using of the 'all_tags' column to calculate identifiers for the 'id' column. The 'all_tags' is a virtual column (it's never stored anywhere, it's always calculated on the fly)", 0) \
    M(Bool, set_id_default_expression_in_tags_table, true, "When creating an inner target 'tags' table, this flag enables setting the default expression for the 'id' column", 0)

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
