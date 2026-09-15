#pragma once

#include <Interpreters/MetricLog.h>


namespace DB
{

/** BucketedMetricLog is a variant of system.metric_log (schema_type = 'bucketed') that stores
  * all profile events and current metrics in a single `metrics` column of type Map(Enum16(...), Int64).
  * By default the Map is serialized with buckets (the `with_buckets` Map serialization
  * with a constant number of buckets), so reading a single metric reads only one of the buckets.
  * Zero values are not stored: a lookup of a missing key in a Map returns 0.
  * Every metric is also exposed through an ALIAS column (e.g. `ProfileEvent_Query`),
  * which makes this schema query-compatible with the wide one.
  */

struct BucketedMetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    std::vector<ProfileEvents::Count> profile_events;
    std::vector<CurrentMetrics::Metric> current_metrics;

    Array histogram_metric;
    Array histogram_labels;
    Array histogram_histogram;
    Array histogram_count;
    Array histogram_sum;

    /// The whole per-metric interface of this schema (`ProfileEvent_*` / `CurrentMetric_*`)
    /// consists of ALIAS columns over the `metrics` Map, so the table is unusable if
    /// alias columns are skipped (see `ISystemLogFlushPolicy::shouldSkipAliasColumns`).
    static constexpr bool alias_columns_are_required = true;

    static std::string name() { return "BucketedMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

class BucketedMetricLog : public PeriodicLog<BucketedMetricLogElement>
{
    using PeriodicLog<BucketedMetricLogElement>::PeriodicLog;

public:
    static constexpr auto DESCRIPTION = R"(
        Contains history of metrics values from tables system.metrics and system.events.
        Periodically flushed to disk. Stores all metrics in a single Map column with bucketed serialization.)";

    /// Serialize the Map column into a constant number of buckets, so reading a single metric
    /// reads only a small fraction of the data. Parts created by inserts (zero level) use the
    /// basic serialization to keep the number of files small, while merged parts are always
    /// split into the fixed number of buckets.
    static const char * getDefaultEngineSettings()
    {
        return "map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'basic',"
               " max_buckets_in_map = 128, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0";
    }

protected:
    void stepFunction(TimePoint current_time) override;

private:
    /// stepFunction and flushBufferToLog may be executed concurrently, hence the mutex
    std::vector<ProfileEvents::Count> previous_profile_events TSA_GUARDED_BY(previous_profile_events_mutex) = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    mutable std::mutex previous_profile_events_mutex;
};

}
