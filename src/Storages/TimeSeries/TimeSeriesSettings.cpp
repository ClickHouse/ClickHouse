#include <Storages/TimeSeries/TimeSeriesSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
}


#define LIST_OF_TIME_SERIES_SETTINGS(DECLARE, ALIAS) \
    DECLARE(ASTFunction, id_generator, String{}, "Expression that computes the identifier (fingerprint) of a time series from its tags.", 0) \
    DECLARE(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    DECLARE(Bool, use_all_tags_column_to_generate_id, true, "When generating an expression to calculate an identifier of a time series, this flag enables using the 'all_tags' column in that calculation. The 'all_tags' is a virtual column containing all tags except the metric name", 0) \
    DECLARE(Bool, store_min_time_and_max_time, true, "If set to true then the table will store 'min_time' and 'max_time' for each time series", 0) \
    DECLARE(Bool, aggregate_min_time_and_max_time, true, "When creating an inner target 'tags' table, this flag enables using 'SimpleAggregateFunction(min, Nullable(DateTime64(3)))' instead of just 'Nullable(DateTime64(3))' as the type of the 'min_time' column, and the same for the 'max_time' column", 0) \
    DECLARE(Bool, filter_by_min_time_and_max_time, true, "If set to true then the table will use the 'min_time' and 'max_time' columns for filtering time series", 0) \
    DECLARE(Bool, prometheus_remote_write_dynamic_routing_enabled, false, "Allow Prometheus remote-write dynamic URL routing to insert into this TimeSeries table", 0) \

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS, TIMESERIES_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS, TimeSeriesSettings, TimeSeriesSetting)

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

TimeSeriesSettings & TimeSeriesSettings::operator=(TimeSeriesSettings && settings) noexcept
{
    *impl = std::move(*settings.impl);
    return *this;
}

TimeSeriesSettings::~TimeSeriesSettings() = default;

TIMESERIES_SETTINGS_SUPPORTED_TYPES(TimeSeriesSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void TimeSeriesSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

void TimeSeriesSettings::copyToQuery(ASTStorage & storage_def) const
{
    if (!storage_def.settings)
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    auto & dest_changes = storage_def.settings->changes;
    for (const auto & src_change : changes())
    {
        bool exists = dest_changes.tryGet(src_change.name) != nullptr;
        if (!exists)
            dest_changes.push_back(src_change);
    }
}

SettingsChanges TimeSeriesSettings::changes() const
{
    return impl->changes();
}

void TimeSeriesSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

bool TimeSeriesSettings::hasBuiltin(std::string_view name)
{
    return TimeSeriesSettingsImpl::hasBuiltin(name);
}

void checkTimeSeriesSettings(const TimeSeriesSettings & settings)
{
    if (!settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        /// Reject only an explicit conflicting value.
        /// If the user just disables `store_min_time_and_max_time` and leaves other two
        /// defaulting to `true`, timeSeriesSelector() will skip filtering.
        if (settings[TimeSeriesSetting::filter_by_min_time_and_max_time]
            && settings[TimeSeriesSetting::filter_by_min_time_and_max_time].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `filter_by_min_time_and_max_time` cannot be enabled when `store_min_time_and_max_time` is disabled");

        if (settings[TimeSeriesSetting::aggregate_min_time_and_max_time]
            && settings[TimeSeriesSetting::aggregate_min_time_and_max_time].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `aggregate_min_time_and_max_time` cannot be enabled when `store_min_time_and_max_time` is disabled");
    }

    const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
    if (!tags_to_columns.empty())
    {
        static const std::unordered_set<std::string_view> reserved_tag_names = {
            TimeSeriesTagNames::MetricName,
        };
        static const std::unordered_set<std::string_view> reserved_column_names = {
            TimeSeriesColumnNames::ID,
            TimeSeriesColumnNames::MetricName,
            TimeSeriesColumnNames::Tags,
            TimeSeriesColumnNames::AllTags,
            TimeSeriesColumnNames::MinTime,
            TimeSeriesColumnNames::MaxTime,
        };
        std::unordered_set<std::string_view> seen_tag_names;
        std::unordered_set<std::string_view> seen_column_names;
        for (const auto & entry : tags_to_columns)
        {
            const auto & tuple = entry.safeGet<Tuple>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            if (tag_name.empty())
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting `tags_to_columns` has an entry with empty tag name");
            if (column_name.empty())
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `tags_to_columns`: tag `{}` maps to an empty column name", tag_name);
            if (reserved_tag_names.contains(tag_name))
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `tags_to_columns`: tag name `{}` is reserved for the TimeSeries tags table", tag_name);
            if (reserved_column_names.contains(column_name))
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `tags_to_columns`: column name `{}` is reserved for the TimeSeries tags table", column_name);
            if (!seen_tag_names.insert(tag_name).second)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `tags_to_columns` has duplicate tag name `{}`", tag_name);
            if (!seen_column_names.insert(column_name).second)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `tags_to_columns` has duplicate column name `{}`", column_name);
        }
    }
}

}
