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
        }
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
