#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}


#define LIST_OF_TIME_SERIES_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    DECLARE(Bool, use_all_tags_column_to_generate_id, true, "When generating an expression to calculate an identifier of a time series, this flag enables using the 'all_tags' column in that calculation. The 'all_tags' is a virtual column containing all tags except the metric name", 0) \
    DECLARE(Bool, store_min_time_and_max_time, true, "If set to true then the table will store 'min_time' and 'max_time' for each time series", 0) \
    DECLARE(Bool, aggregate_min_time_and_max_time, true, "When creating an inner target 'tags' table, this flag enables using 'SimpleAggregateFunction(min, Nullable(DateTime64(3)))' instead of just 'Nullable(DateTime64(3))' as the type of the 'min_time' column, and the same for the 'max_time' column", 0) \
    DECLARE(Bool, filter_by_min_time_and_max_time, true, "If set to true then the table will use the 'min_time' and 'max_time' columns for filtering time series", 0) \

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS)

struct TimeSeriesSettingsImpl : public BaseSettings<TimeSeriesSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) TimeSeriesSettings##TYPE NAME = &TimeSeriesSettingsImpl ::NAME;

namespace TimeSeriesSetting
{
LIST_OF_TIME_SERIES_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

TimeSeriesSettings::TimeSeriesSettings() : impl(std::make_unique<TimeSeriesSettingsImpl>())
{
}

TimeSeriesSettings::TimeSeriesSettings(const TimeSeriesSettings & settings) : impl(std::make_unique<TimeSeriesSettingsImpl>(*settings.impl))
{
}

TimeSeriesSettings::TimeSeriesSettings(TimeSeriesSettings && settings) noexcept
    : impl(std::make_unique<TimeSeriesSettingsImpl>(std::move(*settings.impl)))
{
}

TimeSeriesSettings::~TimeSeriesSettings() = default;

TIMESERIES_SETTINGS_SUPPORTED_TYPES(TimeSeriesSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void TimeSeriesSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

}
