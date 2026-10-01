#pragma once

#include <algorithm>
#include <array>
#include <string_view>


namespace DB
{

/// The settings of the "histograms" target table, in the form `LIST_OF_TIME_SERIES_SETTINGS` uses (see TimeSeriesSettings.cpp,
/// which expands this list inside it). This is the single source of truth for which settings belong to the histograms table:
/// they exist from `TimeSeriesVersion::MIN_WITH_HISTOGRAMS_TARGET`, so a table pinned to an earlier version rejects them
/// (see `checkTimeSeriesSettings`) and doesn't copy them by the clause `AS <other_table>`.
#define LIST_OF_TIME_SERIES_HISTOGRAMS_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, histograms_index_granularity, 8192, "Sets 'index_granularity' of the inner 'histograms' table. When set explicitly, it overrides 'index_granularity' from the engine declaration. Ignored for a non-MergeTree engine. Requires a 'version' with the histograms table", 0) \
    DECLARE(UInt64, histograms_max_buckets, 0, "The maximum number of buckets (positive and negative together) a single histogram sample may have; an insert with a bigger histogram is rejected. 0 means no limit, like Prometheus without 'native_histogram_bucket_limit'. Requires a 'version' with the histograms table", 0) \

/// The registry of the settings of the "histograms" target table.
class TimeSeriesHistogramsSettings
{
public:
    /// The names of all the settings of the histograms table.
    static constexpr const auto & getNames()
    {
        return names;
    }

    /// Whether a setting belongs to the histograms table.
    static constexpr bool contains(std::string_view name)
    {
        return std::ranges::find(names, name) != names.end();
    }

private:
#define TIME_SERIES_HISTOGRAMS_SETTING_NAME(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) std::string_view{#NAME},
#define TIME_SERIES_HISTOGRAMS_SETTING_ALIAS(...)
    static constexpr std::array names = std::to_array<std::string_view>(
        {LIST_OF_TIME_SERIES_HISTOGRAMS_SETTINGS(TIME_SERIES_HISTOGRAMS_SETTING_NAME, TIME_SERIES_HISTOGRAMS_SETTING_ALIAS)});
#undef TIME_SERIES_HISTOGRAMS_SETTING_NAME
#undef TIME_SERIES_HISTOGRAMS_SETTING_ALIAS
};

}
