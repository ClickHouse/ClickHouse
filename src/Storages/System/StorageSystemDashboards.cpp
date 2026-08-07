#include <string_view>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <Core/NamesAndTypes.h>
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

static String trim(const char * text)
{
    std::string_view view(text);
    ::trim(view, '\n');
    return String(view);
}

#if ENABLE_DISTRIBUTED_CACHE
/// Defined in StorageSystemDashboardsDistributedCache.cpp, which exists only in the private repo.
const std::vector<std::map<String, String>> & getDistributedCacheDashboards();
#endif

void StorageSystemDashboards::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    static const std::vector<std::map<String, String>> default_dashboards
    {
        /// Default dashboard for self-managed ClickHouse
        {
            { "dashboard", "Overview" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_Query)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_Query)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_Merge)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_SelectedBytes)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "IO Wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSIOWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "OS CPU Usage (Userspace)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "OS CPU Usage (Kernel)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Read From Disk" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSReadBytes)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Read From Filesystem" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSReadChars)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Memory (tracked)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_MemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'LoadAverage15'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Selected Rows/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_SelectedRows)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Inserted Rows/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_InsertedRows)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview" },
            { "title", "Concurrent network connections" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    max(CurrentMetric_TCPConnection) AS TCP_Connections,
    max(CurrentMetric_MySQLConnection) AS MySQL_Connections,
    max(CurrentMetric_HTTPConnection) AS HTTP_Connections,
    max(CurrentMetric_InterserverConnection) AS Interserver_Connections
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Default per host dashboard for self-managed ClickHouse
        {
            { "dashboard", "Overview (host)" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_Query)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(CurrentMetric_Query)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(CurrentMetric_Merge)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_SelectedBytes)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "IO Wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSIOWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "OS CPU Usage (Userspace)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSUserTimeNormalized'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "OS CPU Usage (Kernel)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSSystemTimeNormalized'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Read From Disk" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSReadBytes)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Read From Filesystem" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_OSReadChars)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Memory (tracked)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(CurrentMetric_MemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'LoadAverage15'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Selected Rows/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_SelectedRows)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Inserted Rows/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(ProfileEvent_InsertedRows)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Overview (host)" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t, hostname, max(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'MaxPartCountForPartition'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Memory usage per host dashboard for self-managed ClickHouse
        {
            { "dashboard", "Memory (host)" },
            { "title", "Tracked memory by ClickHouse" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(CurrentMetric_MemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Memory for merges/mutations" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(CurrentMetric_MergesMutationsMemoryTracking)
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "In-Memory Caches" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Primary key" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalPrimaryKeyBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Index Granularity" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalIndexGranularityBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Tracked memory by kernel (RSS)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'MemoryResident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Tracked memory by allocator" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'jemalloc.allocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "Resident memory used by allocator (includes allocator metadata)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'jemalloc.resident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "ClickHouse vs Kernel Drift" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
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
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM merge('system', '^asynchronous_metric_log')
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
        AND metric = 'MemoryResident'
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Memory (host)" },
            { "title", "ClickHouse vs Allocator Drift" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
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
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM merge('system', '^asynchronous_metric_log')
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
        AND metric = 'jemalloc.allocated'
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Filesystem cache dashboard for self-managed ClickHouse
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache hits and misses (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedReadBufferReadFromCacheHits) AS Hits,
    avg(ProfileEvent_CachedReadBufferReadFromCacheMisses) AS Misses
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Read from cache and from source (bytes/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedReadBufferReadFromCacheBytes) AS ReadFromCache,
    avg(ProfileEvent_CachedReadBufferReadFromSourceBytes) AS ReadFromSource,
    avg(ProfileEvent_CachedReadBufferPredownloadedBytes) AS Predownloaded,
    avg(ProfileEvent_CachedReadBufferPredownloadedFromSourceBytes) AS PredownloadedFromSource
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent reading (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedReadBufferReadFromCacheMicroseconds) / 1000000 AS ReadFromCache,
    avg(ProfileEvent_CachedReadBufferReadFromSourceMicroseconds) / 1000000 AS ReadFromSource,
    avg(ProfileEvent_CachedReadBufferPredownloadedFromSourceMicroseconds) / 1000000 AS PredownloadFromSource,
    avg(ProfileEvent_CachedReadBufferWaitReadBufferMicroseconds) / 1000000 AS WaitReadBuffer,
    avg(ProfileEvent_CachedReadBufferCreateBufferMicroseconds) / 1000000 AS CreateBuffer
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Written into cache (bytes/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedReadBufferCacheWriteBytes) AS OnRead,
    avg(ProfileEvent_CachedWriteBufferCacheWriteBytes) AS WriteThrough
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent writing into cache (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedReadBufferCacheWriteMicroseconds) / 1000000 AS OnRead,
    avg(ProfileEvent_CachedWriteBufferCacheWriteMicroseconds) / 1000000 AS WriteThrough
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache writes stopped (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedReadBufferCacheWriteStopped) AS OnRead,
    avg(ProfileEvent_CachedWriteBufferCacheWriteStopped) AS WriteThrough
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache size (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCacheSize) AS Size,
    avg(CurrentMetric_FilesystemCacheSizeLimit) AS SizeLimit
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache size on disk (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avgIf(value, metric = 'FilesystemCacheBytes') AS Bytes,
    avgIf(value, metric = 'FilesystemCacheCapacity') AS Capacity
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric IN ('FilesystemCacheBytes', 'FilesystemCacheCapacity')
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache elements" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCacheElements) AS Elements,
    avg(CurrentMetric_FilesystemCacheKeys) AS Keys,
    avg(CurrentMetric_FilesystemCacheFileSegments) AS FileSegments,
    avg(CurrentMetric_FilesystemCacheDetachedFileSegments) AS DetachedFileSegments
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cached file segments on disk" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avgIf(value, metric = 'FilesystemCacheFiles') AS Files
FROM merge('system', '^asynchronous_metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric IN ('FilesystemCacheFiles')
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Priority queue elements" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCachePriorityQueueElements) AS Total,
    avg(CurrentMetric_FilesystemCacheInvalidatedElements) AS Invalidated
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Background queues" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCacheDownloadQueueElements) AS DownloadQueue,
    avg(CurrentMetric_FilesystemCacheDelayedCleanupElements) AS CleanupQueue
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Buffers, holders and users" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCacheReadBuffers) AS ReadBuffers,
    avg(CurrentMetric_FilesystemCacheHoldFileSegments) AS HoldFileSegments,
    avg(CurrentMetric_FilesystemCacheOvercommitUsers) AS OvercommitUsers
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Threads" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCacheReserveThreads) AS ReserveThreads,
    avg(CurrentMetric_FilesystemCacheEvictionThreads) AS EvictionThreads,
    avg(CurrentMetric_FilesystemCacheEvictionThreadsActive) AS EvictionThreadsActive,
    avg(CurrentMetric_FilesystemCacheEvictionThreadsScheduled) AS EvictionThreadsScheduled,
    avg(CurrentMetric_FilesystemCacheDropCacheThreads) AS DropCacheThreads,
    avg(CurrentMetric_FilesystemCacheDropCacheThreadsActive) AS DropCacheThreadsActive,
    avg(CurrentMetric_FilesystemCacheDropCacheThreadsScheduled) AS DropCacheThreadsScheduled
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Evicted (bytes/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheEvictedBytes) AS Evicted,
    avg(ProfileEvent_FilesystemCacheBackgroundEvictedBytes) AS BackgroundEvicted
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Evicted file segments (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheEvictedFileSegments) AS Evicted,
    avg(ProfileEvent_FilesystemCacheBackgroundEvictedFileSegments) AS BackgroundEvicted,
    avg(ProfileEvent_FilesystemCacheDowngradedFileSegments) AS Downgraded
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Eviction attempts (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheEvictionTries) AS Tries,
    avg(ProfileEvent_FilesystemCacheEvictionReusedIterator) AS ReusedIterator,
    avg(ProfileEvent_FilesystemCacheFailedEvictionCandidates) AS FailedCandidates,
    avg(ProfileEvent_FilesystemCacheOvercommitCandidatesIterationSteps) AS OvercommitIterationSteps
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "File segments skipped for eviction (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheEvictionSkippedFileSegments) AS Unreleasable,
    avg(ProfileEvent_FilesystemCacheEvictionSkippedEvictingFileSegments) AS Evicting,
    avg(ProfileEvent_FilesystemCacheEvictionSkippedMovingFileSegments) AS Moving
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent on eviction (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheEvictMicroseconds) / 1000000 AS Evict
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Space reservation attempts (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheReserveAttempts) AS Attempts,
    avg(ProfileEvent_FilesystemCacheFailedReserveAttempts) AS Failed,
    avg(ProfileEvent_FilesystemCacheFailToReserveSpaceBecauseOfLockContention) AS SkippedOnLockContention,
    avg(ProfileEvent_FilesystemCacheFailToReserveSpaceBecauseOfCacheResize) AS SkippedOnCacheResize
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent on space reservation (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheReserveMicroseconds) / 1000000 AS Reserve
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent waiting for locks (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheLockMetadataMicroseconds) / 1000000 AS Metadata,
    avg(ProfileEvent_FilesystemCacheLockKeyMicroseconds) / 1000000 AS Key,
    avg(ProfileEvent_FilesystemCacheLockOriginPoolMicroseconds) / 1000000 AS OriginPool,
    avg(ProfileEvent_FilesystemCachePriorityWriteLockMicroseconds) / 1000000 AS PriorityWrite,
    avg(ProfileEvent_FilesystemCachePriorityReadLockMicroseconds) / 1000000 AS PriorityRead,
    avg(ProfileEvent_FilesystemCacheStateLockMicroseconds) / 1000000 AS State,
    avg(ProfileEvent_FilesystemCacheClientsMapLockWaitMicroseconds) / 1000000 AS ClientsMap,
    avg(ProfileEvent_FileSegmentLockMicroseconds) / 1000000 AS FileSegment
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent in cache lookups (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheGetOrSetMicroseconds) / 1000000 AS GetOrSet,
    avg(ProfileEvent_FilesystemCacheGetMicroseconds) / 1000000 AS Get
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache metadata" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheLoadMetadataMicroseconds) / 1000000 AS LoadMetadataSeconds,
    avg(ProfileEvent_FilesystemCacheCreatedKeyDirectories) AS CreatedKeyDirectories
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Waiting for a concurrently downloaded file segment" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FileSegmentWaitMicroseconds) / 1000000 AS WaitSeconds,
    avg(ProfileEvent_FileSegmentWaitTimeouts) AS Timeouts
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent in file segment operations (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FileSegmentWriteMicroseconds) / 1000000 AS Write,
    avg(ProfileEvent_FileSegmentCompleteMicroseconds) / 1000000 AS Complete,
    avg(ProfileEvent_FileSegmentHolderCompleteMicroseconds) / 1000000 AS HolderComplete,
    avg(ProfileEvent_FileSegmentRemoveMicroseconds) / 1000000 AS Remove,
    avg(ProfileEvent_FileSegmentIncreasePriorityMicroseconds) / 1000000 AS IncreasePriority
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Hold file segments (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheHoldFileSegments) AS Hold,
    avg(ProfileEvent_FilesystemCacheUnusedHoldFileSegments) AS Unused
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Priority updates skipped on lock contention (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FileSegmentFailToIncreasePriority) AS FailToIncreasePriority
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Background jobs (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheFreeSpaceKeepingThreadRun) AS FreeSpaceKeepingRuns,
    avg(ProfileEvent_FilesystemCacheFreeSpaceKeepingThreadErrors) AS FreeSpaceKeepingErrors,
    avg(ProfileEvent_FilesystemCacheBackgroundDownloadQueuePush) AS DownloadQueuePush,
    avg(ProfileEvent_FilesystemCacheBackgroundRemovedInvalidatedEntries) AS RemovedInvalidatedEntries,
    avg(ProfileEvent_FilesystemCacheIdleClientEvictions) AS IdleClientEvictions
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Time spent in background jobs (seconds/second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds) / 1000 AS FreeSpaceKeeping,
    avg(ProfileEvent_FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds) / 1000 AS InvalidatedEntriesCleanup
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache correctness checks" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheCheckCorrectness) AS Checks,
    avg(ProfileEvent_FilesystemCacheCheckCorrectnessMicroseconds) / 1000000 AS Seconds
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Cache warmer" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_FilesystemCacheWarmerBytesDownloaded) AS BytesDownloaded,
    avg(ProfileEvent_FilesystemCacheWarmerDataPartsDownloaded) AS DataPartsDownloaded,
    avg(CurrentMetric_FilesystemCacheWarmerBytesInProgress) AS BytesInProgress
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        /// Default dashboard for ClickHouse Cloud
        {
            { "dashboard", "Cloud overview" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) / 1000000
FROM (
  SELECT event_time, sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_Merge) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_SelectedBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "IO Wait (local fs)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "S3 read wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "S3 read errors/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3RequestsErrors) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "OS CPU Usage (Userspace, normalized)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "OS CPU Usage (Kernel, normalized)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From Disk (bytes/sec)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSReadBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From Filesystem (bytes/sec)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_OSReadChars) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Memory (tracked, bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_MemoryTracking) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM (
  SELECT event_time, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'LoadAverage15'
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Selected Rows/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_SelectedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Inserted Rows/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_InsertedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Inserted Bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_InsertedBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Merged Rows/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_MergedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Delayed inserts/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_DelayedInserts) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Delayed inserts wait (seconds)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_DelayedInsertsMilliseconds) / 1000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From S3 (bytes/sec)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Filesystem Cache Size" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_FilesystemCacheSize) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk S3 write req/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_DiskS3PutObject + ProfileEvent_DiskS3UploadPart + ProfileEvent_DiskS3CreateMultipartUpload + ProfileEvent_DiskS3CompleteMultipartUpload) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk S3 read req/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_DiskS3GetObject + ProfileEvent_DiskS3HeadObject + ProfileEvent_DiskS3ListObjects) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "FS cache hit rate" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Page cache hit rate" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, greatest(0, (sum(ProfileEvent_OSReadChars) - sum(ProfileEvent_OSReadBytes)) / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes))) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Network receive bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM (
  SELECT event_time, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric LIKE 'NetworkReceiveBytes%'
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Network send bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)
FROM (
  SELECT event_time, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric LIKE 'NetworkSendBytes%'
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Concurrent network connections" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(TCP_Connections), max(MySQL_Connections), max(HTTP_Connections)
FROM (
    SELECT event_time,
        sum(CurrentMetric_TCPConnection) AS TCP_Connections,
        sum(CurrentMetric_MySQLConnection) AS MySQL_Connections,
        sum(CurrentMetric_HTTPConnection) AS HTTP_Connections
    FROM clusterAllReplicas(default, merge('system', '^metric_log'))
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "ZooKeeper Transactions/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ZooKeeperTransactions) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "ZooKeeper Wait (seconds)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ZooKeeperWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "ZooKeeper Sent Bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ZooKeeperBytesSent) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "ZooKeeper Received Bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_ZooKeeperBytesReceived) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk Metadata From Keeper Cache Hits/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_MetadataFromKeeperCacheHit) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk Metadata From Keeper Cache Misses/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_MetadataFromKeeperCacheMiss) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk Metadata From Keeper Tx Commits/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_MetadataFromKeeperTransactionCommit) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk Metadata From Keeper Operations/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_MetadataFromKeeperOperations) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk Metadata From Keeper Cache Update Wait (seconds)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_MetadataFromKeeperCacheUpdateMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk Metadata From Keeper Cache Objects Count" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(CurrentMetric_MetadataFromKeeperCacheObjects) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Primary Index Cache Bytes (max/server)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(CurrentMetric_PrimaryIndexCacheBytes)
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Primary Index Cache Files (max/server)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(CurrentMetric_PrimaryIndexCacheFiles)
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Logger Elapsed Time (seconds)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
  avg(metric)
FROM (
  SELECT event_time, sum(ProfileEvent_LoggerElapsedNanoseconds) / 1000000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time
)
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        /// Default per host dashboard for ClickHouse Cloud
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Queries/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "CPU Usage (cores)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(metric) / 1000000
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Queries Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_Query) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Merges Running" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_Merge) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Selected Bytes/second" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_SelectedBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "IO Wait (local fs)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "S3 read wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "S3 read errors/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3RequestsErrors) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "CPU Wait" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "OS CPU Usage (Userspace, normalized)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSUserTimeNormalized'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "OS CPU Usage (Kernel, normalized)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'OSSystemTimeNormalized'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From Disk (bytes/sec)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSReadBytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From Filesystem (bytes/sec)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_OSReadChars) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Memory (tracked, bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_MemoryTracking) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "In-Memory Caches (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Load Average (15 minutes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM (
  SELECT event_time, hostname, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'LoadAverage15'
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Selected Rows/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_SelectedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Inserted Rows/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_InsertedRows) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Total MergeTree Parts" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Max Parts For Partition" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, max(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'MaxPartCountForPartition'
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From S3 (bytes/sec)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Filesystem Cache Size" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(CurrentMetric_FilesystemCacheSize) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Disk S3 write req/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,
 hostname,
  avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_DiskS3PutObject + ProfileEvent_DiskS3UploadPart + ProfileEvent_DiskS3CreateMultipartUpload + ProfileEvent_DiskS3CompleteMultipartUpload) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Disk S3 read req/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
 hostname,
 avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_DiskS3GetObject + ProfileEvent_DiskS3HeadObject + ProfileEvent_DiskS3ListObjects) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t
WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "FS cache hit rate" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
 hostname,
 avg(metric)
FROM (
  SELECT event_time, hostname, sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Page cache hit rate" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT
  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
 hostname,
 avg(metric)
FROM (
  SELECT event_time, hostname, greatest(0, (sum(ProfileEvent_OSReadChars) - sum(ProfileEvent_OSReadBytes)) / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes))) AS metric
  FROM clusterAllReplicas(default, merge('system', '^metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Network receive bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM (
  SELECT event_time, hostname, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric LIKE 'NetworkReceiveBytes%'
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Network send bytes/sec" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM (
  SELECT event_time, hostname, sum(value) AS value
  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
  WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric LIKE 'NetworkSendBytes%'
  GROUP BY event_time, hostname
)
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        /// Memory usage per host dashboard in ClickHouse Cloud
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Tracked memory by ClickHouse" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(CurrentMetric_MemoryTracking)
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Memory for merges/mutations" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(CurrentMetric_MergesMutationsMemoryTracking)
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "In-Memory Caches" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, arraySum([COLUMNS('CurrentMetric_.*CacheBytes') EXCEPT 'CurrentMetric_FilesystemCache.*' APPLY avg]) AS metric
FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t, hostname
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Primary key" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalPrimaryKeyBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Index Granularity" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'TotalIndexGranularityBytesInMemoryAllocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Tracked memory by kernel (RSS)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'MemoryResident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Tracked memory by allocator" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'jemalloc.allocated'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "Resident memory used by allocator (includes allocator metadata)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)
FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    AND metric = 'jemalloc.resident'
GROUP BY ALL
ORDER BY t WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "ClickHouse vs Kernel Drift" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
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
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
        AND metric = 'MemoryResident'
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Cloud Memory (host)" },
            { "title", "ClickHouse vs Allocator Drift" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
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
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
    GROUP BY ALL
) AS metrics
JOIN
(
    SELECT
        CAST(toStartOfInterval(event_time, toIntervalSecond({rounding:UInt32})), 'INT') AS t,
        hostname,
        avg(value) AS value
    FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
        AND metric = 'jemalloc.allocated'
    GROUP BY ALL
) AS async_metrics USING (t, hostname)
ORDER BY t ASC WITH FILL STEP {rounding:UInt32}
SETTINGS skip_unavailable_shards = 1
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
    {
        add_dashboards(*context_dashboards);
    }
    else
    {
        add_dashboards(default_dashboards);
#if ENABLE_DISTRIBUTED_CACHE
        add_dashboards(getDistributedCacheDashboards());
#endif
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDashboards) }
