#include <Storages/TimeSeries/TimeSeriesSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Prometheus/CreateQueryTimeSeriesSettings.h>
#include <Parsers/Prometheus/TimeSeriesVersion.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/checkTimeSeriesVersion.h>

#include <limits>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_SETTING;
}


#define LIST_OF_TIME_SERIES_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, version, TimeSeriesVersion::LATEST, "The version of the TimeSeries table: it determines the set of the target tables and their structure. The version is pinned automatically when a table is created and cannot be changed afterwards. Tables created before this setting was introduced are considered as version 0", 0) \
    DECLARE(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    DECLARE(Bool, store_time_ranges, true, "If set to true then the table stores the time range (the minimum and the maximum timestamp) of each time series in the 'time ranges' target table, and uses it to filter time series by time. Not applicable to tables of version 7 or earlier, they use the 'store_min_time_and_max_time' setting instead", 0) \
    DECLARE(UInt64, samples_bucket_step_seconds, 3600, "The length in seconds of the time buckets of the 'samples' table: a row of the table contains the samples of one time series with timestamps in one bucket, and the 'bucket' column contains the start of the bucket, i.e. the timestamp rounded down to a multiple of this setting. The default is 1 hour; the effective value is pinned into the table definition at CREATE time", 0) \
    DECLARE(String, samples_compression_codec, "ZSTD(3)", "Compatibility setting for persisted TimeSeries metadata. This build supports the existing 'ZSTD(3)' payload codec", 0) \
    DECLARE(ASTFunction, samples_partition_by, String{}, "Partition key of the inner 'samples' table, for example 'toStartOfWeek(bucket)'. When set explicitly, it overrides the partition key from the engine declaration; if neither is set, one partition per UTC month is used. Applies to version 11 or newer", 0) \
    DECLARE(UInt64, samples_index_granularity, 512, "Sets 'index_granularity' of the inner 'samples' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for an external samples table and a non-MergeTree engine", 0) \
    DECLARE(UInt64, samples_index_granularity_bytes, 524288, "Sets 'index_granularity_bytes' of the inner 'samples' table. When set explicitly, it overrides 'index_granularity_bytes' from the engine declaration. Ignored for an external samples table and a non-MergeTree engine", 0) \
    DECLARE(UInt64, recent_samples_ttl_seconds, 345600, "Retention of the additional 'recent samples' target table, which every inserted sample is written to as well. An inner recent samples table gets 'TTL toDateTime(timestamp) + toIntervalSecond(recent_samples_ttl_seconds)' through version 10, or a TTL on the UTC bucket plus 'toIntervalSecond(recent_samples_ttl_seconds + recent_samples_bucket_step_seconds)' in version 11 or newer, overriding any TTL from the engine declaration. In version 11 or newer, the sum of the retention and the bucket step must not exceed 9223372036854775807, the maximum Int64 value used by IntervalSecond. Queries whose time range fits in the TTL window prefer the recent samples table to the main samples table (see the query-level setting 'time_series_prefer_recent_samples_table'). The default is 4 days; set to 0 to disable the recent samples table", 0) \
    DECLARE(UInt64, recent_samples_bucket_step_seconds, 900, "The length in seconds of the time buckets of the 'recent samples' table, see 'samples_bucket_step_seconds'. The default is 15 minutes; the effective value is pinned into the table definition at CREATE time. Requires 'recent_samples_ttl_seconds' to be non-zero", 0) \
    DECLARE(String, recent_samples_compression_codec, "ZSTD(3)", "Compatibility setting for persisted TimeSeries metadata. This build supports the existing 'ZSTD(3)' payload codec. Requires 'recent_samples_ttl_seconds' to be non-zero when specified", 0) \
    DECLARE(ASTFunction, recent_samples_partition_by, String{}, "Partition key of the inner 'recent samples' table, for example 'toStartOfHour(bucket)'. When set explicitly, it overrides the partition key from the engine declaration; if neither is set, one partition per 5 hours is used, in UTC for version 9 or newer. Ignored for an external recent samples table. Requires 'recent_samples_ttl_seconds' to be non-zero", 0) \
    DECLARE(UInt64, recent_samples_index_granularity, 256, "Sets 'index_granularity' of the inner 'recent samples' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for an external recent samples table and a non-MergeTree engine. Requires 'recent_samples_ttl_seconds' to be non-zero", 0) \
    DECLARE(UInt64, recent_samples_index_granularity_bytes, 262144, "Sets 'index_granularity_bytes' of the inner 'recent samples' table. When set explicitly, it overrides 'index_granularity_bytes' from the engine declaration. Ignored for an external recent samples table and a non-MergeTree engine. Requires 'recent_samples_ttl_seconds' to be non-zero", 0) \
    DECLARE(UInt64, tags_index_granularity, 8192, "Sets 'index_granularity' of the inner 'tags' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for an external tags table and a non-MergeTree engine", 0) \
    DECLARE(UInt64, tags_deduplication_cache_expiration_seconds, 3600, "Time after which an entry of the deduplication cache of the 'tags' table expires, counted from the moment the time series was written. So every time series is written again at least once per this period, which limits any difference between the cache and the table. The cache is local to the server and cleared by 'TRUNCATE TABLE' executed on it or by 'SYSTEM DROP TIME SERIES CACHES'. Used only when the 'tags' table doesn't store 'min_time' and 'max_time', see 'tags_deduplication_cache_size_bytes'. Set to 0 to disable the cache", 0) \
    DECLARE(UInt64, tags_deduplication_cache_size_bytes, 104857600, "Maximum size in bytes of the deduplication cache of the 'tags' table. The cache remembers the time series written recently, so their tags aren't written again with every insert. When the cache is full, the entries used only once are evicted first, then the least recently used ones (SLRU). The cache is used only when the 'tags' table doesn't store 'min_time' and 'max_time' (tables of version 7 or earlier store them unless 'store_min_time_and_max_time' is disabled), because otherwise every insert changes these columns: the default value is ignored then, and an explicit non-zero value is rejected. Set to 0 to disable the cache, see also 'tags_deduplication_cache_expiration_seconds'", 0) \
    DECLARE(UInt64, metric_families_deduplication_cache_expiration_seconds, 3600, "Time after which an entry of the deduplication cache of the 'metric families' table expires, counted from the moment the metric family was written. So every metric family is written again at least once per this period, which limits any difference between the cache and the table. The cache is local to the server and cleared by 'TRUNCATE TABLE' executed on it or by 'SYSTEM DROP TIME SERIES CACHES'. Set to 0 to disable the cache", 0) \
    DECLARE(UInt64, metric_families_deduplication_cache_size_bytes, 10485760, "Maximum size in bytes of the deduplication cache of the 'metric families' table. The cache remembers the descriptions of the metric families written recently, so they aren't written again with every insert. When the cache is full, the entries used only once are evicted first, then the least recently used ones (SLRU). Set to 0 to disable the cache, see also 'metric_families_deduplication_cache_expiration_seconds'", 0) \
    \
    /** The settings of the earlier versions (see TimeSeriesVersion.h): they apply to tables of those versions only and are rejected for the latest version. \
      * They are not in the OBSOLETE tier because their values are still read for the tables of those versions, \
      * while an obsolete setting is accepted and ignored whatever the version of the table is. */ \
    DECLARE(DataType, id_type, String{}, "The type of the 'id' column of the target tables. Applies to tables of version 9 or earlier only, the later versions declare the type in the INNER COLUMNS clause of the 'tags' table. The setting is set automatically when such a table is created if the type isn't kept in the definition otherwise: if the 'tags' target is an external table, or if the 'id_generator' setting is set. Requires 'version' to be at least 2", 0) \
    DECLARE(ASTFunction, id_generator, String{}, "Expression that computes the identifier (fingerprint) of a time series from its tags. Applies to tables of version 9 or earlier only, the later versions declare the expression as the DEFAULT expression of the 'id' column in the INNER COLUMNS clause of the 'tags' table. If the 'tags' target is an external table and 'version' is at least 2, the setting is set automatically when the table is created: to the DEFAULT expression of the 'id' column of that table if any, otherwise to the expression chosen automatically for the 'id' type", 0) \
    DECLARE(Bool, store_min_time_and_max_time, true, "If set to true then the table will store 'min_time' and 'max_time' for each time series in the 'tags' table. Applies to tables of version 7 or earlier only, the later versions use the 'store_time_ranges' setting instead", 0) \
    DECLARE(Bool, aggregate_min_time_and_max_time, true, "When creating an inner target 'tags' table, this flag enables using 'SimpleAggregateFunction(min, Nullable(DateTime64(3)))' instead of just 'Nullable(DateTime64(3))' as the type of the 'min_time' column, and the same for the 'max_time' column. Applies to tables of version 7 or earlier only", 0) \
    DECLARE(Bool, filter_by_min_time_and_max_time, true, "If set to true then the table will use the 'min_time' and 'max_time' columns of the 'tags' table for filtering time series. Applies to tables of version 7 or earlier only", 0) \
    \
    /** Obsolete settings which do nothing, kept so that the definitions mentioning them still parse. */ \
    DECLARE(Bool, use_all_tags_column_to_generate_id, false, "Obsolete setting, does nothing.", SettingsTierType::OBSOLETE) \

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS, TIMESERIES_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS, TimeSeriesSettings, TimeSeriesSetting)

TimeSeriesSettings::TimeSeriesSettings() : impl(std::make_unique<TimeSeriesSettingsImpl>())
{
    /// The settings read from a CREATE query without this class must use the same defaults (see CreateQueryTimeSeriesSettings.h).
    chassert((*this)[TimeSeriesSetting::recent_samples_ttl_seconds].value == detail::TIME_SERIES_RECENT_SAMPLES_TTL_SECONDS_DEFAULT);
    chassert((*this)[TimeSeriesSetting::store_time_ranges].value == detail::TIME_SERIES_STORE_TIME_RANGES_DEFAULT);
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

bool TimeSeriesSettings::isChanged(std::string_view name) const
{
    return impl->isChanged(name);
}

bool TimeSeriesSettings::hasBuiltin(std::string_view name)
{
    return TimeSeriesSettingsImpl::hasBuiltin(name);
}

bool TimeSeriesSettings::isRecentSamplesTargetEnabled() const
{
    return (*this)[TimeSeriesSetting::recent_samples_ttl_seconds] != 0;
}

UInt64 TimeSeriesSettings::getBucketedRecentSamplesTTLSeconds() const
{
    const UInt64 ttl_seconds = (*this)[TimeSeriesSetting::recent_samples_ttl_seconds];
    const UInt64 bucket_step = (*this)[TimeSeriesSetting::recent_samples_bucket_step_seconds];
    constexpr UInt64 max_interval_seconds = std::numeric_limits<Int64>::max();

    /// `toIntervalSecond` stores its argument in `Int64`, so preventing unsigned overflow alone is not enough.
    if ((bucket_step > max_interval_seconds) || (ttl_seconds > max_interval_seconds - bucket_step))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "The sum of `recent_samples_ttl_seconds` ({}) and `recent_samples_bucket_step_seconds` ({}) "
            "must not exceed {} for a bucketed TimeSeries table",
            ttl_seconds, bucket_step, max_interval_seconds);

    return ttl_seconds + bucket_step;
}

void TimeSeriesSettings::checkBucketedSamplesSettings() const
{
    const UInt64 version = (*this)[TimeSeriesSetting::version];
    if (version < TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES)
    {
        bool has_bucket_layout_setting
            = (*this)[TimeSeriesSetting::samples_bucket_step_seconds].isChanged()
            || (*this)[TimeSeriesSetting::samples_partition_by].value
            || (*this)[TimeSeriesSetting::samples_index_granularity_bytes].isChanged()
            || (*this)[TimeSeriesSetting::samples_compression_codec].isChanged()
            || (*this)[TimeSeriesSetting::recent_samples_bucket_step_seconds].isChanged()
            || (*this)[TimeSeriesSetting::recent_samples_index_granularity_bytes].isChanged()
            || (*this)[TimeSeriesSetting::recent_samples_compression_codec].isChanged();
        if (has_bucket_layout_setting)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Bucket-layout settings require `version` to be at least {}, but the table has version {}",
                TimeSeriesVersion::MIN_WITH_BUCKETED_SAMPLES, version);
        return;
    }

    auto check_bucket_step = [](const SettingFieldUInt64 & bucket_step, std::string_view setting_name)
    {
        /// The step is multiplied by the scale multiplier of the timestamp type (up to 10^9) and stored in `Int64`,
        /// the limit prevents an overflow there; UInt32::max seconds is enough for any reasonable value.
        if (!bucket_step.value || (bucket_step.value > std::numeric_limits<UInt32>::max()))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `{}` must be in the range [1, {}], got {}", setting_name, std::numeric_limits<UInt32>::max(), bucket_step.value);
    };
    check_bucket_step((*this)[TimeSeriesSetting::samples_bucket_step_seconds], "samples_bucket_step_seconds");
    check_bucket_step((*this)[TimeSeriesSetting::recent_samples_bucket_step_seconds], "recent_samples_bucket_step_seconds");

    if (isRecentSamplesTargetEnabled())
        getBucketedRecentSamplesTTLSeconds();
}

bool TimeSeriesSettings::isTimeRangesTargetEnabled() const
{
    return ((*this)[TimeSeriesSetting::version] >= TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
        && (*this)[TimeSeriesSetting::store_time_ranges];
}

bool TimeSeriesSettings::hasMinTimeAndMaxTimeInTagsTable() const
{
    return ((*this)[TimeSeriesSetting::version] < TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
        && (*this)[TimeSeriesSetting::store_min_time_and_max_time];
}

bool TimeSeriesSettings::hasMinTimeAndMaxTimeInTagsSortingKey() const
{
    return hasMinTimeAndMaxTimeInTagsTable() && !(*this)[TimeSeriesSetting::aggregate_min_time_and_max_time];
}

void checkTimeSeriesSettings(const TimeSeriesSettings & settings)
{
    UInt64 version = settings[TimeSeriesSetting::version];

    if (!isTimeSeriesVersionSupported(version))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid value {} of the `version` setting: this server supports TimeSeries versions from {} to {}. "
            "A table definition with another version was written by a different version of ClickHouse",
            version, TimeSeriesVersion::MIN_SUPPORTED, TimeSeriesVersion::LATEST);

    /// A table of an earlier version must be readable by a server which doesn't know a setting introduced later.
    auto check_setting_requires_version = [&](std::string_view setting_name, UInt64 min_version)
    {
        if ((version < min_version) && settings.isChanged(setting_name))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `{}` requires `version` to be at least {}, but the table has version {}", setting_name, min_version, version);
    };

    check_setting_requires_version("id_type", TimeSeriesVersion::MIN_WITH_ID_TYPE_SETTING);
    check_setting_requires_version("metric_families_deduplication_cache_size_bytes", TimeSeriesVersion::MIN_WITH_DEDUPLICATION_CACHES);
    check_setting_requires_version("metric_families_deduplication_cache_expiration_seconds", TimeSeriesVersion::MIN_WITH_DEDUPLICATION_CACHES);
    check_setting_requires_version("tags_deduplication_cache_size_bytes", TimeSeriesVersion::MIN_WITH_DEDUPLICATION_CACHES);
    check_setting_requires_version("tags_deduplication_cache_expiration_seconds", TimeSeriesVersion::MIN_WITH_DEDUPLICATION_CACHES);

    if (settings.hasMinTimeAndMaxTimeInTagsTable())
    {
        /// Every insert changes `min_time` and `max_time` of a time series, so the rows of the tags table can't be deduplicated
        /// while it stores these columns (tables of earlier versions with `store_min_time_and_max_time` enabled).
        /// Reject only an explicit enabling value, the defaults are just ignored and an explicit zero is harmless.
        auto check_tags_cache_setting_is_not_enabled = [&](std::string_view setting_name, const auto & setting)
        {
            if (setting.isChanged() && setting.value)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `{}` cannot be used when `store_min_time_and_max_time` is enabled", setting_name);
        };

        check_tags_cache_setting_is_not_enabled("tags_deduplication_cache_size_bytes", settings[TimeSeriesSetting::tags_deduplication_cache_size_bytes]);
        check_tags_cache_setting_is_not_enabled("tags_deduplication_cache_expiration_seconds", settings[TimeSeriesSetting::tags_deduplication_cache_expiration_seconds]);
    }

    settings.checkBucketedSamplesSettings();

    if (!settings.isRecentSamplesTargetEnabled())
    {
        /// Settings of the recent samples table make no sense without the table itself.
        if (settings[TimeSeriesSetting::recent_samples_bucket_step_seconds].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_bucket_step_seconds` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
        if (settings[TimeSeriesSetting::recent_samples_compression_codec].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_compression_codec` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
        if (settings[TimeSeriesSetting::recent_samples_partition_by].value)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_partition_by` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
        if (settings[TimeSeriesSetting::recent_samples_index_granularity].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_index_granularity` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
        if (settings[TimeSeriesSetting::recent_samples_index_granularity_bytes].isChanged())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `recent_samples_index_granularity_bytes` requires `recent_samples_ttl_seconds` to be set to a non-zero value");
    }

    auto check_payload_codec = [](const SettingFieldString & codec, std::string_view setting_name)
    {
        if (codec.value != "ZSTD(3)")
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Setting `{}` only supports `ZSTD(3)` in this build, got `{}`",
                setting_name,
                codec.value);
    };
    check_payload_codec(settings[TimeSeriesSetting::samples_compression_codec], "samples_compression_codec");
    check_payload_codec(settings[TimeSeriesSetting::recent_samples_compression_codec], "recent_samples_compression_codec");

    /// The time range of a time series is stored in the "time ranges" table, tables of earlier versions store it in the "tags" table
    /// (see TimeSeriesVersion.h), so the settings of the other form must not be set.
    check_setting_requires_version("store_time_ranges", TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET);
    if (version >= TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET)
    {
        for (std::string_view setting_name : {"store_min_time_and_max_time", "aggregate_min_time_and_max_time", "filter_by_min_time_and_max_time"})
        {
            if (settings.isChanged(setting_name))
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `{}` applies to tables of version {} or earlier only, but the table has version {}; "
                    "use the `store_time_ranges` setting instead",
                    setting_name, TimeSeriesVersion::MIN_WITH_TIME_RANGES_TARGET - 1, version);
        }
    }

    /// The type of the `id` column and the expression generating identifiers are declared in the inner columns of the "tags" table,
    /// tables of earlier versions could declare them in the settings (see TimeSeriesVersion.h).
    if (version >= TimeSeriesVersion::MIN_WITHOUT_ID_SETTINGS)
    {
        for (std::string_view setting_name : {"id_type", "id_generator"})
        {
            if (settings.isChanged(setting_name))
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting `{}` applies to tables of version {} or earlier only, but the table has version {}: "
                    "the type of the `id` column and the expression generating identifiers can be declared now in the `id` column of the tags table",
                    setting_name, TimeSeriesVersion::MIN_WITHOUT_ID_SETTINGS - 1, version);
        }
    }

    if (settings.hasMinTimeAndMaxTimeInTagsTable() && !settings[TimeSeriesSetting::store_min_time_and_max_time])
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
