#include <string_view>
#include <Storages/System/StorageSystemDashboards.h>
#include <Common/StringUtils.h>
#include <Interpreters/Context.h>

namespace DB
{

ColumnsDescription StorageSystemDashboards::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"dashboard", std::make_shared<DataTypeString>(), "The dashboard name."},
        {"title", std::make_shared<DataTypeString>(), "The title of a chart."},
        {"query", std::make_shared<DataTypeString>(), "The query to obtain data to be displayed."},
    };
}

String trim(const char * text)
{
    std::string_view view(text);
    ::trim(view, '\n');
    return String(view);
}

void StorageSystemDashboards::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    static const std::vector<std::map<String, String>> default_dashboards
    {
        /// Default dashboard for self-managed ClickHouse
        {
            { "dashboard", "Overview" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_Query)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_Query)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_Merge)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_SelectedBytes)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "IO Wait" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSIOWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "OS CPU Usage (Userspace)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "OS CPU Usage (Kernel)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Read From Disk" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSReadBytes)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Read From Filesystem" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSReadChars)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Memory (tracked)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_MemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'LoadAverage15'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Selected Rows/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_SelectedRows)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Inserted Rows/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_InsertedRows)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Concurrent network connections" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    max(CurrentMetric_TCPConnection) AS TCP_Connections,
    max(CurrentMetric_MySQLConnection) AS MySQL_Connections,
    max(CurrentMetric_HTTPConnection) AS HTTP_Connections,
    max(CurrentMetric_InterserverConnection) AS Interserver_Connections
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Default per host dashboard for self-managed ClickHouse
        {
            { "dashboard", "Overview (host)" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_Query)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(CurrentMetric_Query)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(CurrentMetric_Merge)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_SelectedBytes)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "IO Wait" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSIOWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "OS CPU Usage (Userspace)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSUserTimeNormalized'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "OS CPU Usage (Kernel)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'OSSystemTimeNormalized'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Read From Disk" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSReadBytes)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Read From Filesystem" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSReadChars)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Memory (tracked)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(CurrentMetric_MemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'LoadAverage15'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Selected Rows/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_SelectedRows)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Inserted Rows/second" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_InsertedRows)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, max(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'MaxPartCountForPartition'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Memory usage per host dashboard for self-managed ClickHouse
        {
            { "dashboard", "Memory (host)" },
            { "title", "Tracked memory by ClickHouse" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(CurrentMetric_MemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "In-Memory Caches" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM merge('system', '^metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Primary key" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'TotalPrimaryKeyBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Index Granularity" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'TotalIndexGranularityBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Tracked memory by kernel (RSS)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'MemoryResident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Tracked memory by allocator" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'jemalloc.allocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Resident memory used by allocator (includes allocator metadata)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'jemalloc.resident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "ClickHouse vs Kernel Drift" },
            { "query", trim(R"EOQ(
SELECT
    t,
    hostname,
    metrics.value - async_metrics.value AS drift
FROM
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(CurrentMetric_MemoryTracking) AS value
    FROM merge('system', '^metric_log')
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32}))
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM merge('system', '^asynchronous_metric_log')
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32})) AND (metric = 'MemoryResident')
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "ClickHouse vs Allocator Drift" },
            { "query", trim(R"EOQ(
SELECT
    t,
    hostname,
    metrics.value - async_metrics.value AS drift
FROM
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(CurrentMetric_MemoryTracking) AS value
    FROM merge('system', '^metric_log')
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32}))
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM merge('system', '^asynchronous_metric_log')
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32})) AND (metric = 'jemalloc.allocated')
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Default dashboard for ClickHouse Cloud
        {
            { "dashboard", "Cloud overview" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) / 1000000
FROM (
  SELECT event_time, sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32} GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_Merge) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_SelectedBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "IO Wait (local fs)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "S3 read wait" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "S3 read errors/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3RequestsErrors) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "OS CPU Usage (Userspace, normalized)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "OS CPU Usage (Kernel, normalized)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From Disk (bytes/sec)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSReadBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From Filesystem (bytes/sec)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSReadChars) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Memory (tracked, bytes)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_MemoryTracking) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM (
  SELECT event_time, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
    AND metric = 'LoadAverage15'
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Selected Rows/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_SelectedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Inserted Rows/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_InsertedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From S3 (bytes/sec)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Filesystem Cache Size" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_FilesystemCacheSize) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk S3 write req/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_DiskS3PutObject + ProfileEvent_DiskS3UploadPart + ProfileEvent_DiskS3CreateMultipartUpload + ProfileEvent_DiskS3CompleteMultipartUpload) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk S3 read req/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_DiskS3GetObject + ProfileEvent_DiskS3HeadObject + ProfileEvent_DiskS3ListObjects) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "FS cache hit rate" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Page cache hit rate" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, greatest(0, (sum(ProfileEvent_OSReadChars) - sum(ProfileEvent_OSReadBytes)) / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes))) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Network receive bytes/sec" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM (
  SELECT event_time, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
    AND metric LIKE 'NetworkReceiveBytes%'
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Network send bytes/sec" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM (
  SELECT event_time, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
    AND metric LIKE 'NetworkSendBytes%'
  GROUP BY event_time)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Concurrent network connections" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(TCP_Connections), max(MySQL_Connections), max(HTTP_Connections) FROM (SELECT event_time, sum(CurrentMetric_TCPConnection) AS TCP_Connections, sum(CurrentMetric_MySQLConnection) AS MySQL_Connections, sum(CurrentMetric_HTTPConnection) AS HTTP_Connections FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        /// Default per host dashboard for ClickHouse Cloud
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(metric) / 1000000
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32} GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_Merge) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_SelectedBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "IO Wait (local fs)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "S3 read wait" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "S3 read errors/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3RequestsErrors) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "OS CPU Usage (Userspace, normalized)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'OSUserTimeNormalized'
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "OS CPU Usage (Kernel, normalized)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'OSSystemTimeNormalized'
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From Disk (bytes/sec)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSReadBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From Filesystem (bytes/sec)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSReadChars) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Memory (tracked, bytes)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_MemoryTracking) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM (
  SELECT event_time, hostname, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
    AND metric = 'LoadAverage15'
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Selected Rows/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_SelectedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Inserted Rows/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_InsertedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'TotalPartsOfMergeTreeTables'
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
AND metric = 'MaxPartCountForPartition'
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From S3 (bytes/sec)" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Filesystem Cache Size" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_FilesystemCacheSize) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Disk S3 write req/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_DiskS3PutObject + ProfileEvent_DiskS3UploadPart + ProfileEvent_DiskS3CreateMultipartUpload + ProfileEvent_DiskS3CompleteMultipartUpload) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
 GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Disk S3 read req/sec" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
 hostname,
 avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_DiskS3GetObject + ProfileEvent_DiskS3HeadObject + ProfileEvent_DiskS3ListObjects) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
GROUP BY t, hostname
ORDER BY t
WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "FS cache hit rate" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
 hostname,
 avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Page cache hit rate" },
            { "query", trim(R"EOQ(
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
 hostname,
 avg(metric)
FROM (
  SELECT event_time, hostname, greatest(0, (sum(ProfileEvent_OSReadChars) - sum(ProfileEvent_OSReadBytes)) / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes))) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
  GROUP BY event_time, hostname)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Network receive bytes/sec" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM (
  SELECT event_time, hostname, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
    AND metric LIKE 'NetworkReceiveBytes%'
  GROUP BY event_time, hostname)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Network send bytes/sec" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM (
  SELECT event_time, hostname, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date >= toDate(now() - {seconds:UInt32})
    AND event_time >= now() - {seconds:UInt32}
    AND metric LIKE 'NetworkSendBytes%'
  GROUP BY event_time, hostname)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        /// Memory usage per host dashboard in ClickHouse Cloud
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Tracked memory by ClickHouse" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(CurrentMetric_MemoryTracking)
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "In-Memory Caches" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Primary key" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'TotalPrimaryKeyBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Index Granularity" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'TotalIndexGranularityBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Tracked memory by kernel (RSS)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'MemoryResident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Tracked memory by allocator" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'jemalloc.allocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Resident memory used by allocator (includes allocator metadata)" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} AND metric = 'jemalloc.resident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "ClickHouse vs Kernel Drift" },
            { "query", trim(R"EOQ(
SELECT
    t,
    hostname,
    metrics.value - async_metrics.value AS drift
FROM
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(CurrentMetric_MemoryTracking) AS value
    FROM clusterAllReplicas(default, merge('system', '^metric_log'))
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32}))
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32})) AND (metric = 'MemoryResident')
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "ClickHouse vs Allocator Drift" },
            { "query", trim(R"EOQ(
SELECT
    t,
    hostname,
    metrics.value - async_metrics.value AS drift
FROM
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(CurrentMetric_MemoryTracking) AS value
    FROM clusterAllReplicas(default, merge('system', '^metric_log'))
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32}))
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
    WHERE (event_date >= toDate(now() - {seconds:UInt32})) AND (event_time >= (now() - {seconds:UInt32})) AND (metric = 'jemalloc.allocated')
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
    };

    auto add_dashboards = [&](const auto & dashboards)
    {
        for (const auto & row : dashboards)
        {
            size_t i = 0;
            res_columns[i++]->insert(row.at("dashboard"));
            res_columns[i++]->insert(row.at("title"));
            res_columns[i++]->insert(row.at("query"));
        }
    };

    const auto & context_dashboards = context->getDashboards();
    if (context_dashboards.has_value())
        add_dashboards(*context_dashboards);
    else
        add_dashboards(default_dashboards);
}

}
