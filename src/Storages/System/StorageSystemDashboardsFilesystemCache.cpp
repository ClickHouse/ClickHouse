#include <map>
#include <vector>
#include <Core/Types.h>
#include <fmt/format.h>

/// The `Filesystem cache overview` dashboards of system.dashboards, kept out of StorageSystemDashboards.cpp
/// because they cover every filesystem cache metric and so change on every metric addition.

namespace DB
{

const std::vector<std::map<String, String>> & getFilesystemCacheDashboards();

namespace
{

struct Series
{
    const char * label;
    /// Aggregates the rows of all replicas at one `event_time`, the same way as `Cloud overview` does.
    const char * expression;
    /// Aggregates the values of `expression` within one chart interval.
    const char * per_interval = "avg";
    /// If set, the series is `expression / divisor`, both summed over the interval, so a ratio stays weighted.
    const char * divisor = nullptr;
};

struct Chart
{
    const char * title;
    std::vector<Series> series;
    const char * table = "metric_log";
    /// Extra condition on the rows of `table`, e.g. which asynchronous metrics to read.
    const char * filter = "";
};

const std::vector<Chart> & getCharts()
{
    static const std::vector<Chart> charts
    {
        {
            "Cache hits and misses (per second)",
            {
                {"Hits", "sum(ProfileEvent_CachedReadBufferReadFromCacheHits)"},
                {"Misses", "sum(ProfileEvent_CachedReadBufferReadFromCacheMisses)"},
            },
        },
        {
            "Read from cache and from source (bytes/second)",
            {
                {"ReadFromCache", "sum(ProfileEvent_CachedReadBufferReadFromCacheBytes)"},
                {"ReadFromSource", "sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)"},
                {"PredownloadedFromSource", "sum(ProfileEvent_CachedReadBufferPredownloadedFromSourceBytes)"},
            },
        },
        {
            "Average read size (bytes)",
            {
                {.label = "FromCache", .expression = "sum(ProfileEvent_CachedReadBufferReadFromCacheBytes)", .divisor = "sum(ProfileEvent_CachedReadBufferReadFromCacheHits)"},
                {.label = "FromSource", .expression = "sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)", .divisor = "sum(ProfileEvent_CachedReadBufferReadFromCacheMisses)"},
            },
        },
        {
            "Time spent reading (seconds/second)",
            {
                {"ReadFromCache", "sum(ProfileEvent_CachedReadBufferReadFromCacheMicroseconds) / 1000000"},
                {"ReadFromSource", "sum(ProfileEvent_CachedReadBufferReadFromSourceMicroseconds) / 1000000"},
                {"PredownloadFromSource", "sum(ProfileEvent_CachedReadBufferPredownloadedFromSourceMicroseconds) / 1000000"},
                {"WaitReadBuffer", "sum(ProfileEvent_CachedReadBufferWaitReadBufferMicroseconds) / 1000000"},
                {"CreateBuffer", "sum(ProfileEvent_CachedReadBufferCreateBufferMicroseconds) / 1000000"},
            },
        },
        {
            "Written into cache (bytes/second)",
            {
                {"OnRead", "sum(ProfileEvent_CachedReadBufferCacheWriteBytes)"},
                {"WriteThrough", "sum(ProfileEvent_CachedWriteBufferCacheWriteBytes)"},
            },
        },
        {
            "Time spent writing into cache (seconds/second)",
            {
                {"OnRead", "sum(ProfileEvent_CachedReadBufferCacheWriteMicroseconds) / 1000000"},
                {"WriteThrough", "sum(ProfileEvent_CachedWriteBufferCacheWriteMicroseconds) / 1000000"},
            },
        },
        {
            "Cache writes stopped (per second)",
            {
                {"OnRead", "sum(ProfileEvent_CachedReadBufferCacheWriteStopped)"},
                {"WriteThrough", "sum(ProfileEvent_CachedWriteBufferCacheWriteStopped)"},
            },
        },
        {
            "Write-through covering segments shrunk (per second)",
            {
                {"Shrunk", "sum(ProfileEvent_CachedWriteBufferCoveringSegmentShrunk)"},
                {"ShrinkFailed", "sum(ProfileEvent_CachedWriteBufferCoveringSegmentShrinkFailed)"},
            },
        },
        {
            "Cache size (bytes)",
            {
                {"Size", "sum(CurrentMetric_FilesystemCacheSize)"},
                {"SizeLimit", "sum(CurrentMetric_FilesystemCacheSizeLimit)"},
            },
        },
        {
            "Cache size on disk (bytes)",
            {
                {"Bytes", "sumIf(value, metric = 'FilesystemCacheBytes')"},
                {"Capacity", "sumIf(value, metric = 'FilesystemCacheCapacity')"},
            },
            "asynchronous_metric_log",
            "metric IN ('FilesystemCacheBytes', 'FilesystemCacheCapacity')",
        },
        {
            "Cache elements",
            {
                {"Elements", "sum(CurrentMetric_FilesystemCacheElements)"},
                {"Keys", "sum(CurrentMetric_FilesystemCacheKeys)"},
                {"FileSegments", "sum(CurrentMetric_CacheFileSegments)"},
                {"DetachedFileSegments", "sum(CurrentMetric_CacheDetachedFileSegments)"},
            },
        },
        {
            "Average cached file segment size (bytes)",
            {
                {.label = "AverageSize", .expression = "sum(CurrentMetric_FilesystemCacheSize)", .divisor = "sum(CurrentMetric_FilesystemCacheElements)"},
            },
        },
        {
            "Cached file segments on disk",
            {
                {"Files", "sumIf(value, metric = 'FilesystemCacheFiles')"},
            },
            "asynchronous_metric_log",
            "metric IN ('FilesystemCacheFiles')",
        },
        {
            "Priority queue elements",
            {
                {"Total", "sum(CurrentMetric_FilesystemCachePriorityQueueElements)"},
                {"Invalidated", "sum(CurrentMetric_FilesystemCacheInvalidatedElements)"},
            },
        },
        {
            "Background queues",
            {
                {"DownloadQueue", "sum(CurrentMetric_FilesystemCacheDownloadQueueElements)"},
                {"CleanupQueue", "sum(CurrentMetric_FilesystemCacheDelayedCleanupElements)"},
            },
        },
        {
            "Buffers, holders and users",
            {
                {"ReadBuffers", "sum(CurrentMetric_FilesystemCacheReadBuffers)"},
                {"HoldFileSegments", "sum(CurrentMetric_FilesystemCacheHoldFileSegments)"},
                {"OvercommitUsers", "sum(CurrentMetric_FilesystemCacheOvercommitUsers)"},
            },
        },
        {
            "Threads",
            {
                {"ReserveThreads", "sum(CurrentMetric_FilesystemCacheReserveThreads)"},
                {"EvictionThreads", "sum(CurrentMetric_FilesystemCacheEvictionThreads)"},
                {"EvictionThreadsActive", "sum(CurrentMetric_FilesystemCacheEvictionThreadsActive)"},
                {"EvictionThreadsScheduled", "sum(CurrentMetric_FilesystemCacheEvictionThreadsScheduled)"},
                {"DropCacheThreads", "sum(CurrentMetric_FilesystemCacheDropCacheThreads)"},
                {"DropCacheThreadsActive", "sum(CurrentMetric_FilesystemCacheDropCacheThreadsActive)"},
                {"DropCacheThreadsScheduled", "sum(CurrentMetric_FilesystemCacheDropCacheThreadsScheduled)"},
            },
        },
        {
            "Evicted (bytes/second)",
            {
                {"Evicted", "sum(ProfileEvent_FilesystemCacheEvictedBytes)"},
                {"BackgroundEvicted", "sum(ProfileEvent_FilesystemCacheBackgroundEvictedBytes)"},
            },
        },
        {
            "Evicted file segments (per second)",
            {
                {"Evicted", "sum(ProfileEvent_FilesystemCacheEvictedFileSegments)"},
                {"BackgroundEvicted", "sum(ProfileEvent_FilesystemCacheBackgroundEvictedFileSegments)"},
                {"Downgraded", "sum(ProfileEvent_FilesystemCacheDowngradedFileSegments)"},
                {"Promoted", "sum(ProfileEvent_FilesystemCachePromotedFileSegments)"},
            },
        },
        {
            "Eviction attempts (per second)",
            {
                {"Tries", "sum(ProfileEvent_FilesystemCacheEvictionTries)"},
                {"ReusedIterator", "sum(ProfileEvent_FilesystemCacheEvictionReusedIterator)"},
                {"FailedCandidates", "sum(ProfileEvent_FilesystemCacheFailedEvictionCandidates)"},
                {"OvercommitIterationSteps", "sum(ProfileEvent_FilesystemCacheOvercommitCandidatesIterationSteps)"},
            },
        },
        {
            "File segments skipped for eviction (per second)",
            {
                {"Unreleasable", "sum(ProfileEvent_FilesystemCacheEvictionSkippedFileSegments)"},
                {"Evicting", "sum(ProfileEvent_FilesystemCacheEvictionSkippedEvictingFileSegments)"},
                {"Moving", "sum(ProfileEvent_FilesystemCacheEvictionSkippedMovingFileSegments)"},
            },
        },
        {
            "Time spent on eviction (seconds/second)",
            {
                {"Evict", "sum(ProfileEvent_FilesystemCacheEvictMicroseconds) / 1000000"},
            },
        },
        {
            "Space reservation attempts (per second)",
            {
                {"Attempts", "sum(ProfileEvent_FilesystemCacheReserveAttempts)"},
                {"Failed", "sum(ProfileEvent_FilesystemCacheFailedReserveAttempts - ProfileEvent_FilesystemCacheReserveAheadRetries)"},
                {"SkippedOnLockContention", "sum(ProfileEvent_FilesystemCacheFailToReserveSpaceBecauseOfLockContention)"},
                {"SkippedOnCacheResize", "sum(ProfileEvent_FilesystemCacheFailToReserveSpaceBecauseOfCacheResize)"},
                {"ReserveAheadRetries", "sum(ProfileEvent_FilesystemCacheReserveAheadRetries)"},
            },
        },
        {
            "Time spent on space reservation (seconds/second)",
            {
                {"Reserve", "sum(ProfileEvent_FilesystemCacheReserveMicroseconds) / 1000000"},
            },
        },
        {
            "Time spent waiting for locks (seconds/second)",
            {
                {"Metadata", "sum(ProfileEvent_FilesystemCacheLockMetadataMicroseconds) / 1000000"},
                {"Key", "sum(ProfileEvent_FilesystemCacheLockKeyMicroseconds) / 1000000"},
                {"OriginPool", "sum(ProfileEvent_FilesystemCacheLockOriginPoolMicroseconds) / 1000000"},
                {"PriorityWrite", "sum(ProfileEvent_FilesystemCachePriorityWriteLockMicroseconds) / 1000000"},
                {"PriorityRead", "sum(ProfileEvent_FilesystemCachePriorityReadLockMicroseconds) / 1000000"},
                {"State", "sum(ProfileEvent_FilesystemCacheStateLockMicroseconds) / 1000000"},
                {"ClientsMap", "sum(ProfileEvent_FilesystemCacheClientsMapLockWaitMicroseconds) / 1000000"},
                {"FileSegment", "sum(ProfileEvent_FileSegmentLockMicroseconds) / 1000000"},
            },
        },
        {
            "Time spent in cache lookups (seconds/second)",
            {
                {"GetOrSet", "sum(ProfileEvent_FilesystemCacheGetOrSetMicroseconds) / 1000000"},
                {"Get", "sum(ProfileEvent_FilesystemCacheGetMicroseconds) / 1000000"},
            },
        },
        {
            "Cache metadata",
            {
                {"LoadMetadataSeconds", "sum(ProfileEvent_FilesystemCacheLoadMetadataMicroseconds) / 1000000"},
                {"CreatedKeyDirectories", "sum(ProfileEvent_FilesystemCacheCreatedKeyDirectories)"},
            },
        },
        {
            "Waiting for a concurrently downloaded file segment",
            {
                {"WaitSeconds", "sum(ProfileEvent_FileSegmentWaitMicroseconds) / 1000000"},
                {"WaitWithoutDataSeconds", "sum(ProfileEvent_FileSegmentWaitWithoutDataMicroseconds) / 1000000"},
                {"Timeouts", "sum(ProfileEvent_FileSegmentWaitTimeouts)"},
                {"WithoutData", "sum(ProfileEvent_FileSegmentWaitWithoutData)"},
            },
        },
        {
            "Time spent in file segment operations (seconds/second)",
            {
                {"Write", "sum(ProfileEvent_FileSegmentWriteMicroseconds) / 1000000"},
                {"Complete", "sum(ProfileEvent_FileSegmentCompleteMicroseconds) / 1000000"},
                {"HolderComplete", "sum(ProfileEvent_FileSegmentHolderCompleteMicroseconds) / 1000000"},
                {"IncreasePriority", "sum(ProfileEvent_FileSegmentIncreasePriorityMicroseconds) / 1000000"},
            },
        },
        {
            "Hold file segments (per second)",
            {
                {"Hold", "sum(ProfileEvent_FilesystemCacheHoldFileSegments)"},
                {"Unused", "sum(ProfileEvent_FilesystemCacheUnusedHoldFileSegments)"},
            },
        },
        {
            "Priority updates skipped on lock contention (per second)",
            {
                {"FailToIncreasePriority", "sum(ProfileEvent_FileSegmentFailToIncreasePriority)"},
            },
        },
        {
            "Background jobs (per second)",
            {
                {"FreeSpaceKeepingRuns", "sum(ProfileEvent_FilesystemCacheFreeSpaceKeepingThreadRun)"},
                {"FreeSpaceKeepingErrors", "sum(ProfileEvent_FilesystemCacheFreeSpaceKeepingThreadErrors)"},
                {"DownloadQueuePush", "sum(ProfileEvent_FilesystemCacheBackgroundDownloadQueuePush)"},
                {"RemovedInvalidatedEntries", "sum(ProfileEvent_FilesystemCacheBackgroundRemovedInvalidatedEntries)"},
                {"IdleClientEvictions", "sum(ProfileEvent_FilesystemCacheIdleClientEvictions)"},
            },
        },
        {
            "Time spent in background jobs (seconds/second)",
            {
                {"FreeSpaceKeeping", "sum(ProfileEvent_FilesystemCacheFreeSpaceKeepingThreadWorkMilliseconds) / 1000"},
                {"InvalidatedEntriesCleanup", "sum(ProfileEvent_FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds) / 1000"},
            },
        },
        {
            "Cache correctness checks",
            {
                {"Checks", "sum(ProfileEvent_FilesystemCacheCheckCorrectness)"},
                {"Seconds", "sum(ProfileEvent_FilesystemCacheCheckCorrectnessMicroseconds) / 1000000"},
            },
        },
        {
            "Cache warmer",
            {
                {"BytesDownloaded", "sum(ProfileEvent_CacheWarmerBytesDownloaded)"},
                {"DataPartsDownloaded", "sum(ProfileEvent_CacheWarmerDataPartsDownloaded)"},
                {"BytesInProgress", "sum(CurrentMetric_CacheWarmerBytesInProgress)"},
            },
        },
        {
            "IO and CPU wait (seconds/second)",
            {
                {"IOWait", "sum(ProfileEvent_OSIOWaitMicroseconds) / 1000000"},
                {"CPUWait", "sum(ProfileEvent_OSCPUWaitMicroseconds) / 1000000"},
            },
        },
        {
            /// A share of CPU time normalized per core, so replicas are averaged, not summed.
            "OS IO wait (share of CPU time, normalized)",
            {
                {"IOWaitNormalized", "avg(value)"},
            },
            "asynchronous_metric_log",
            "metric = 'OSIOWaitTimeNormalized'",
        },
        {
            /// The metric exists per block device, under its own name with the device in `key`, or under the legacy
            /// name with the device appended. The busiest device is the one that saturates.
            "Busiest block device active time (seconds/second)",
            {
                {"ActiveTime", "max(value)"},
            },
            "asynchronous_metric_log",
            "(metric = 'BlockActiveTime' OR startsWith(metric, 'BlockActiveTime_'))",
        },
        {
            "Busiest block device queue time per operation (seconds)",
            {
                {"QueueTimePerOp", "max(value)"},
            },
            "asynchronous_metric_log",
            "(metric = 'BlockQueueTimePerOp' OR startsWith(metric, 'BlockQueueTimePerOp_'))",
        },
        {
            "Local IO throttling sleep (seconds/second)",
            {
                {"Read", "sum(ProfileEvent_LocalReadThrottlerSleepMicroseconds) / 1000000"},
                {"Write", "sum(ProfileEvent_LocalWriteThrottlerSleepMicroseconds) / 1000000"},
            },
        },
        {
            "IO pressure stall (seconds/second)",
            {
                {"Some", "sumIf(value, endsWith(metric, '_some')) / 1000000"},
                {"Full", "sumIf(value, endsWith(metric, '_full')) / 1000000"},
            },
            "asynchronous_metric_log",
            "startsWith(metric, 'PSI_IO_')",
        },
        {
            "OS read (bytes/second)",
            {
                {"FromDisk", "sum(ProfileEvent_OSReadBytes)"},
                {"FromFilesystem", "sum(ProfileEvent_OSReadChars)"},
            },
        },
        {
            "OS write (bytes/second)",
            {
                {"ToDisk", "sum(ProfileEvent_OSWriteBytes)"},
                {"ToFilesystem", "sum(ProfileEvent_OSWriteChars)"},
            },
        },
        {
            "Local file IO (bytes/second)",
            {
                {"Read", "sum(ProfileEvent_ReadBufferFromFileDescriptorReadBytes)"},
                {"Written", "sum(ProfileEvent_WriteBufferFromFileDescriptorWriteBytes)"},
            },
        },
        {
            "Time spent in read and write syscalls (seconds/second)",
            {
                {"Read", "sum(ProfileEvent_DiskReadElapsedMicroseconds) / 1000000"},
                {"Write", "sum(ProfileEvent_DiskWriteElapsedMicroseconds) / 1000000"},
            },
        },
        {
            "Read from S3 (bytes/second)",
            {
                {"ReadFromS3", "sum(ProfileEvent_ReadBufferFromS3Bytes)"},
            },
        },
        {
            "S3 read wait (seconds/second)",
            {
                {"ReadFromS3", "sum(ProfileEvent_ReadBufferFromS3Microseconds) / 1000000"},
            },
        },
        {
            "Server uptime (seconds, drops on restart)",
            {
                {"Uptime", "min(value)", "min"},
            },
            "asynchronous_metric_log",
            "metric = 'Uptime'",
        },
    };
    return charts;
}

/// Takes `series` separately from `chart` because the per-host dashboard plots each series of a chart on its own.
String makeQuery(const Chart & chart, const std::vector<Series> & series, bool per_host)
{
    String inner_list;
    String outer_list;
    for (const auto & [label, expression, per_interval, divisor] : series)
    {
        String value;
        if (divisor)
        {
            inner_list += fmt::format(", {} AS {}_dividend, {} AS {}_divisor", expression, label, divisor, label);
            value = fmt::format("sum({}_dividend) / nullIf(sum({}_divisor), 0)", label, label);
        }
        else
        {
            inner_list += fmt::format(", {} AS {}", expression, label);
            value = fmt::format("{}({})", per_interval, label);
        }
        outer_list += fmt::format("{}\n    {} AS {}", outer_list.empty() ? "" : ",", value, label);
    }

    String filter = chart.filter[0] != '\0' ? fmt::format("\n        AND {}", chart.filter) : "";
    const char * host = per_host ? ", hostname" : "";

    return fmt::format(R"EOQ(WITH toDateTimeOrDefault({{from:String}}, '', now() - {{seconds:UInt32}}) AS from,
    toDateTimeOrDefault({{to:String}}, '', now()) AS to
SELECT
    toStartOfInterval(event_time, INTERVAL {{rounding:UInt32}} SECOND)::INT AS t{},{}
FROM (
    SELECT event_time{}{}
    FROM clusterAllReplicas(default, merge('system', '^{}'))
    WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to{}
    GROUP BY event_time{}
)
GROUP BY t{}
ORDER BY t WITH FILL STEP {{rounding:UInt32}} SETTINGS skip_unavailable_shards = 1)EOQ",
        host, outer_list, host, inner_list, chart.table, filter, host, host);
}

}

const std::vector<std::map<String, String>> & getFilesystemCacheDashboards()
{
    static const std::vector<std::map<String, String>> dashboards = []
    {
        std::vector<std::map<String, String>> res;
        for (const auto & chart : getCharts())
            res.push_back({{"dashboard", "Filesystem cache overview"}, {"title", chart.title}, {"query", makeQuery(chart, chart.series, false)}});

        /// The dashboard draws one line per host only for a single value column, so every series gets its own chart.
        for (const auto & chart : getCharts())
        {
            for (const auto & series : chart.series)
            {
                String title = chart.series.size() == 1 ? chart.title : fmt::format("{}: {}", chart.title, series.label);
                res.push_back({{"dashboard", "Filesystem cache overview (host)"}, {"title", title}, {"query", makeQuery(chart, {series}, true)}});
            }
        }
        return res;
    }();
    return dashboards;
}

}
