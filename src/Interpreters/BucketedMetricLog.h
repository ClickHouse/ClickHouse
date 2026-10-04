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
    static constexpr auto DOCUMENTATION = R"DOCS_MD(
.description
Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.

This is the `bucketed` schema of `system.metric_log`. It stores all profile events and current metrics in a single `metrics` column of type [Map](/reference/data-types/map)([Enum16](/reference/data-types/enum), [Int64](/reference/data-types/int-uint)). Profile events are stored as increments during the collection interval, and current metrics are stored as values at collection time. Zero values are omitted; reading a missing key returns `0`.

Every metric is also available through an `ALIAS` column named after the metric, so queries written for the default `wide` schema continue to work. The map uses bucketed serialization with 128 constant buckets, so reading one metric reads only one bucket.

Each row also contains a snapshot of registered histogram metrics in the `histograms` Nested column. Bucket counts are cumulative since server startup. By default, histograms whose total `count` is zero are omitted, as are zero-counter buckets within emitted histograms. Set `system_metric_log_show_zero_values_in_histograms = 1` in the default user profile to retain them.

Configure this schema with:

```xml
<clickhouse>
    <metric_log>
        <schema_type>bucketed</schema_type>
    </metric_log>
</clickhouse>
```

.examples
Read a profile event through its compatibility alias:

```sql
SELECT event_time, ProfileEvent_Query
FROM system.metric_log
ORDER BY event_time DESC
LIMIT 10;
```

Read the latest snapshot of a histogram:

```sql
SELECT h.metric, h.labels, h.histogram, h.count, h.sum
FROM system.metric_log
ARRAY JOIN histograms AS h
WHERE h.metric = 'keeper_response_time_ms' AND h.labels['operation_type'] = 'readonly'
ORDER BY event_time DESC
LIMIT 1;
```

.see_also
- [metric_log setting](/reference/settings/server-settings/settings/other#metric_log) — Enabling and configuring the log.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD";
    static constexpr const char * DOCUMENTATION_SOURCE = __builtin_FILE();

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
