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
    sum(CurrentMetric_TCPConnection) AS TCP_Connections,
    sum(CurrentMetric_MySQLConnection) AS MySQL_Connections,
    sum(CurrentMetric_HTTPConnection) AS HTTP_Connections
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
        /// Default dashboard for ClickHouse Cloud
        {
            { "dashboard", "Cloud overview" },
            { "title", "Queries/second" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_Query) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "CPU Usage (cores)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) / 1000000\nFROM (\n  SELECT event_time, sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32} GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Queries Running" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(CurrentMetric_Query) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Merges Running" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(CurrentMetric_Merge) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Selected Bytes/second" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_SelectedBytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "IO Wait (local fs)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "S3 read wait" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000 AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "S3 read errors/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3RequestsErrors) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "CPU Wait" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "OS CPU Usage (Userspace, normalized)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'OSUserTimeNormalized'\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "OS CPU Usage (Kernel, normalized)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'OSSystemTimeNormalized'\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From Disk (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_OSReadBytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From Filesystem (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_OSReadChars) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Memory (tracked, bytes)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(CurrentMetric_MemoryTracking) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Load Average (15 minutes)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM (\n  SELECT event_time, sum(value) AS value\n  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n    AND metric = 'LoadAverage15'\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Selected Rows/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_SelectedRows) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Inserted Rows/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_InsertedRows) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Total MergeTree Parts" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'TotalPartsOfMergeTreeTables'\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Max Parts For Partition" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'MaxPartCountForPartition'\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Read From S3 (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Filesystem Cache Size" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(CurrentMetric_FilesystemCacheSize) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk S3 write req/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_DiskS3PutObject + ProfileEvent_DiskS3UploadPart + ProfileEvent_DiskS3CreateMultipartUpload + ProfileEvent_DiskS3CompleteMultipartUpload) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Disk S3 read req/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_DiskS3GetObject + ProfileEvent_DiskS3HeadObject + ProfileEvent_DiskS3ListObjects) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "FS cache hit rate" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Page cache hit rate" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, greatest(0, (sum(ProfileEvent_OSReadChars) - sum(ProfileEvent_OSReadBytes)) / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes))) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Network receive bytes/sec" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM (\n  SELECT event_time, sum(value) AS value\n  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n    AND metric LIKE 'NetworkReceiveBytes%'\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Network send bytes/sec" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM (\n  SELECT event_time, sum(value) AS value\n  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n    AND metric LIKE 'NetworkSendBytes%'\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview" },
            { "title", "Concurrent network connections" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(TCP_Connections), max(MySQL_Connections), max(HTTP_Connections) FROM (SELECT event_time, sum(CurrentMetric_TCPConnection) AS TCP_Connections, sum(CurrentMetric_MySQLConnection) AS MySQL_Connections, sum(CurrentMetric_HTTPConnection) AS HTTP_Connections FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        /// Default per host dashboard for ClickHouse Cloud
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Queries/second" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_Query) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "CPU Usage (cores)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(metric) / 1000000\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32} GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Queries Running" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(CurrentMetric_Query) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Merges Running" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(CurrentMetric_Merge) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Selected Bytes/second" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_SelectedBytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "IO Wait (local fs)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "S3 read wait" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000 AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "S3 read errors/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3RequestsErrors) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "CPU Wait" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "OS CPU Usage (Userspace, normalized)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'OSUserTimeNormalized'\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "OS CPU Usage (Kernel, normalized)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'OSSystemTimeNormalized'\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From Disk (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_OSReadBytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From Filesystem (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_OSReadChars) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Memory (tracked, bytes)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(CurrentMetric_MemoryTracking) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Load Average (15 minutes)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)\nFROM (\n  SELECT event_time, hostname, sum(value) AS value\n  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n    AND metric = 'LoadAverage15'\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Selected Rows/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_SelectedRows) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Inserted Rows/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_InsertedRows) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Total MergeTree Parts" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, max(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'TotalPartsOfMergeTreeTables'\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Max Parts For Partition" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, max(value)\nFROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = 'MaxPartCountForPartition'\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Read From S3 (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Filesystem Cache Size" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(CurrentMetric_FilesystemCacheSize) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Disk S3 write req/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT as t,\n hostname,\n  avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_DiskS3PutObject + ProfileEvent_DiskS3UploadPart + ProfileEvent_DiskS3CreateMultipartUpload + ProfileEvent_DiskS3CompleteMultipartUpload) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\n GROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Disk S3 read req/sec" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n hostname,\n avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_DiskS3GetObject + ProfileEvent_DiskS3HeadObject + ProfileEvent_DiskS3ListObjects) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\nGROUP BY t, hostname\nORDER BY t\nWITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "FS cache hit rate" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n hostname,\n avg(metric)\nFROM (\n  SELECT event_time, hostname, sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\nGROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Page cache hit rate" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n hostname,\n avg(metric)\nFROM (\n  SELECT event_time, hostname, greatest(0, (sum(ProfileEvent_OSReadChars) - sum(ProfileEvent_OSReadBytes)) / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes))) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time, hostname)\nGROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Network receive bytes/sec" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)\nFROM (\n  SELECT event_time, hostname, sum(value) AS value\n  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n    AND metric LIKE 'NetworkReceiveBytes%'\n  GROUP BY event_time, hostname)\nGROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Cloud overview (host)" },
            { "title", "Network send bytes/sec" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, hostname, avg(value)\nFROM (\n  SELECT event_time, hostname, sum(value) AS value\n  FROM clusterAllReplicas(default, merge('system', '^asynchronous_metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n    AND metric LIKE 'NetworkSendBytes%'\n  GROUP BY event_time, hostname)\nGROUP BY t, hostname\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        /// Distributed cache client metrics start
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Read from Distributed Cache (bytes/sec)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(ProfileEvent_DistrCacheReadBytesFromCache) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Read from Distributed Cache fallback buffer (bytes/sec)" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(ProfileEvent_DistrCacheReadBytesFromFallbackBuffer) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Read From Filesystem (no Distributed Cache) (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_OSReadChars) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Read From S3 (no Distributed Cache) (bytes/sec)" },
            { "query", "SELECT \n  toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,\n  avg(metric)\nFROM (\n  SELECT event_time, sum(ProfileEvent_ReadBufferFromS3Bytes) AS metric \n  FROM clusterAllReplicas(default, merge('system', '^metric_log'))\n  WHERE event_date >= toDate(now() - {seconds:UInt32})\n    AND event_time >= now() - {seconds:UInt32}\n  GROUP BY event_time)\nGROUP BY t\nORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache read requests" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(CurrentMetric_DistrCacheReadRequests) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache write requests" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(CurrentMetric_DistrCacheWriteRequests) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache open connections" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(CurrentMetric_DistrCacheOpenedConnections) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache registered servers" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(CurrentMetric_DistrCacheRegisteredServersCurrentAZ) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache read errors" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(ProfileEvent_DistrCacheReadErrors) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache make request errors" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheMakeRequestErrors) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache receive response errors" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheReceiveResponseErrors) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache registry updates" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(metric) FROM (SELECT event_time, sum(ProfileEvent_DistrCacheHashRingRebuilds) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache packets" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(ProfileEvent_DistrCachePackets) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        {
            { "dashboard", "Distributed cache client overview" },
            { "title", "Distributed Cache unused packets" },
            { "query", "SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric) FROM (SELECT event_time, sum(ProfileEvent_DistrCacheUnusedPackets) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log')) WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32} GROUP BY event_time) GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1" }
        },
        /// Distributed cache client metrics end
        ///
        /// Distributed cache server metrics start
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache open connections" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(CurrentMetric_DistrCacheServerConnections) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache StartRequest packets" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheServerStartRequestPackets) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache ContinueRequest packets" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheServerContinueRequestPackets) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache EndRequest packets" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheServerEndRequestPackets) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache AckRequest packets" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheServerAckRequestPackets) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache reused s3 clients" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheServerReusedS3CachedClients) AS metric FROM clusterAllReplicas(default, system.metric_log)
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        {
            { "dashboard", "Distributed cache server overview" },
            { "title", "Distributed Cache new s3 clients" },
            { "query", trim(R"EOQ(
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(metric)
FROM (SELECT event_time, sum(ProfileEvent_DistrCacheNewS3CachedClients) AS metric FROM clusterAllReplicas(default, merge('system', '^metric_log'))
WHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}
GROUP BY event_time)
GROUP BY t ORDER BY t WITH FILL STEP {rounding:UInt32} SETTINGS skip_unavailable_shards = 1
)EOQ") }
        },
        /// Distributed cache server metrics end
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
