#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

#include <ctime>


namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

struct TransposedMetricLogElement
{
    UInt16 event_date{};
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    std::string metric_name;
    Int64 value{};
    UInt8 is_event{};

    static std::string name() { return "TransposedMetricLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;

};

/// Transposed version of system.metric_log
class TransposedMetricLog : public PeriodicLog<TransposedMetricLogElement>
{
public:
    /// This table is usually queried by time range + some fixed metric name.
    static const char * getDefaultOrderBy() { return "event_date, toStartOfHour(event_time), metric"; }

    static constexpr auto DOCUMENTATION = R"DOCS_MD(
.description
Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.

This is the `transposed` schema of `system.metric_log`. Each profile event and current metric is stored as a separate row. The `metric` column uses the `ProfileEvent_` prefix for profile events and the `CurrentMetric_` prefix for current metrics. Profile-event values are increments during the collection interval, while current-metric values are snapshots taken at collection time.

Compared with the default `wide` schema, this representation avoids thousands of columns and reduces resource consumption during merges. It does not record histogram metrics; use the `wide` or `bucketed` schema when histogram snapshots are required.

Configure this schema with:

```xml
<clickhouse>
    <metric_log>
        <schema_type>transposed</schema_type>
    </metric_log>
</clickhouse>
```

.examples
Read selected profile events and current metrics:

```sql
SELECT event_time, metric, value
FROM system.metric_log
WHERE metric IN ('ProfileEvent_Query', 'CurrentMetric_GlobalThread')
ORDER BY event_time DESC
LIMIT 10;
```

.see_also
- [metric_log setting](/reference/settings/server-settings/settings/other#metric_log) — Enabling and configuring the log.
- [system.asynchronous_metrics](/reference/system-tables/asynchronous_metrics) — Contains periodically calculated metrics.
- [system.events](/reference/system-tables/events) — Contains a number of events that occurred.
- [system.metrics](/reference/system-tables/metrics) — Contains instantly calculated metrics.
- [Monitoring](/guides/oss/deployment-and-scaling/monitoring/monitoring) — Base concepts of ClickHouse monitoring.
)DOCS_MD";
    static constexpr const char * DOCUMENTATION_SOURCE = __builtin_FILE();

    TransposedMetricLog(
        ContextPtr context_,
        const SystemLogSettings & settings_,
        std::shared_ptr<SystemLogQueue<TransposedMetricLogElement>> queue_ = nullptr)
        : PeriodicLog<TransposedMetricLogElement>(context_, settings_, queue_)
    {
    }

protected:
    void stepFunction(TimePoint current_time) override;

private:
    /// stepFunction and flushBufferToLog may be executed concurrently, hence the mutex
    std::vector<ProfileEvents::Count> previous_profile_events TSA_GUARDED_BY(previous_profile_events_mutex) = std::vector<ProfileEvents::Count>(ProfileEvents::end());
    mutable std::mutex previous_profile_events_mutex;
};

}
