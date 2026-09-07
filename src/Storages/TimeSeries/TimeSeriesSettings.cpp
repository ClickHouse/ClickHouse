#include <Storages/TimeSeries/TimeSeriesSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>

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
    DECLARE(Bool, use_all_tags_column_to_generate_id, false, "Obsolete setting, does nothing.", SettingsTierType::OBSOLETE) \
    DECLARE(Bool, store_min_time_and_max_time, true, "If set to true then the table will store 'min_time' and 'max_time' for each time series", 0) \
    DECLARE(Bool, aggregate_min_time_and_max_time, true, "When creating an inner target 'tags' table, this flag enables using 'SimpleAggregateFunction(min, Nullable(DateTime64(3)))' instead of just 'Nullable(DateTime64(3))' as the type of the 'min_time' column, and the same for the 'max_time' column", 0) \
    DECLARE(Bool, filter_by_min_time_and_max_time, true, "If set to true then the table will use the 'min_time' and 'max_time' columns for filtering time series", 0) \
    DECLARE(UInt64, samples_index_granularity, 32768, "Sets 'index_granularity' of the inner 'samples' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for an external samples table and a non-MergeTree engine", 0) \
    DECLARE(UInt64, recent_samples_ttl_seconds, 345600, "Retention of the additional 'recent samples' target table, which every inserted sample is written to as well. An inner recent samples table always gets 'TTL toDateTime(timestamp) + toIntervalSecond(recent_samples_ttl_seconds)' derived from this setting (overriding any TTL from the engine declaration); an external recent samples table must retain at least this many seconds of data, which is the user's responsibility. Queries whose time range fits in the TTL window prefer the recent samples table to the main samples table (see the query-level setting 'time_series_prefer_recent_samples_table'). The default is 4 days; set to 0 to disable the recent samples table", 0) \
    DECLARE(ASTFunction, recent_samples_partition_by, String{}, "Partition key of the inner 'recent samples' table, for example 'toStartOfHour(timestamp)'. When set explicitly, it overrides the partition key from the engine declaration; if neither is set, 'toStartOfInterval(toDateTime(timestamp), toIntervalHour(5))' is used. Ignored for an external recent samples table. Requires 'recent_samples_ttl_seconds' to be non-zero", 0) \
    DECLARE(UInt64, recent_samples_index_granularity, 8192, "Sets 'index_granularity' of the inner 'recent samples' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for an external recent samples table and a non-MergeTree engine. Requires 'recent_samples_ttl_seconds' to be non-zero", 0) \
    DECLARE(UInt64, tags_index_granularity, 8192, "Sets 'index_granularity' of the inner 'tags' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for an external tags table and a non-MergeTree engine", 0) \
    DECLARE(UInt64, version, TimeSeriesVersion::LATEST, "The version of the TimeSeries table: it determines the set of the target tables and their structure. The version is pinned automatically when a table is created and cannot be changed afterwards. Tables created before this setting was introduced are considered as version 0", 0) \

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS, TIMESERIES_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS, TimeSeriesSettings, TimeSeriesSetting)

TimeSeriesSettings::TimeSeriesSettings() : impl(std::make_unique<TimeSeriesSettingsImpl>())
{
}

TimeSeriesSettings::TimeSeriesSettings(const TimeSeriesSettings & settings) : impl(std::make_unique<TimeSeriesSettingsImpl>(*settings.impl))
{
}

TimeSeriesSettings::TimeSeriesSettings(TimeSeriesSettings && settings) noexcept = default;

TimeSeriesSettings & TimeSeriesSettings::operator=(TimeSeriesSettings && settings) noexcept = default;

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
    UInt64 version = settings[TimeSeriesSetting::version];

    if (!isTimeSeriesVersionSupported(version))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid value {} of the `version` setting: this server supports TimeSeries versions from {} to {}. "
            "A table definition with another version was written by a different version of ClickHouse",
            version, TimeSeriesVersion::MIN_SUPPORTED, TimeSeriesVersion::LATEST);

    if (!settings[TimeSeriesSetting::recent_samples_ttl_seconds])
    {
        /// Settings of the recent samples table make no sense without the table itself.
        if (settings[TimeSeriesSetting::recent_samples_partition_by].value)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_partition_by` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
        if (settings[TimeSeriesSetting::recent_samples_index_granularity].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_index_granularity` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
    }

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

bool hasExplicitTimeSeriesSettingRecentSamplesTTL(const ASTCreateQuery & query)
{
    return query.storage && query.storage->settings
        && query.storage->settings->changes.tryGet("recent_samples_ttl_seconds");
}

UInt64 getTimeSeriesSettingRecentSamplesTTL(const ASTCreateQuery & query)
{
    if (query.storage && query.storage->settings)
    {
        if (const auto * value = query.storage->settings->changes.tryGet("recent_samples_ttl_seconds"))
        {
            /// The conversion must be the same as in the `recent_samples_ttl_seconds` setting itself,
            /// so that every value the setting accepts (e.g. a string literal) is recognized here too.
            return SettingFieldUInt64{*value}.value;
        }
    }
    return TimeSeriesSettings{}[TimeSeriesSetting::recent_samples_ttl_seconds];
}

UInt64 getTimeSeriesSettingVersion(const ASTCreateQuery & query)
{
    if (query.storage && query.storage->settings)
    {
        if (const auto * value = query.storage->settings->changes.tryGet("version"))
            return SettingFieldUInt64{*value}.value;
    }
    return TimeSeriesVersion::LATEST;
}

bool hasExplicitTimeSeriesSettingVersion(const ASTCreateQuery & query)
{
    return query.storage && query.storage->settings
        && query.storage->settings->changes.tryGet("version");
}

void setTimeSeriesSettingVersion(ASTCreateQuery & query, UInt64 version)
{
    if (!query.storage)
        query.set(query.storage, make_intrusive<ASTStorage>());

    if (!query.storage->settings)
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        query.storage->set(query.storage->settings, settings_ast);
    }

    query.storage->settings->changes.setSetting("version", Field{version});
}

}
