#include <map>
#include <string_view>
#include <vector>
#include <Core/Types.h>
#include <Common/StringUtils.h>

/// Charts of the `Filesystem cache` dashboard for system.dashboards, kept out of
/// StorageSystemDashboards.cpp because it covers every filesystem cache metric and
/// therefore changes on every metric addition: the shared file carries just a small
/// hook, which minimizes conflicts.

namespace DB
{

const std::vector<std::map<String, String>> & getFilesystemCacheDashboards();

static String trim(const char * text)
{
    std::string_view view(text);
    ::trim(view, '\n');
    return String(view);
}

const std::vector<std::map<String, String>> & getFilesystemCacheDashboards()
{
    static const std::vector<std::map<String, String>> dashboards
    {
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
            { "title", "Average read size (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    sum(ProfileEvent_CachedReadBufferReadFromCacheBytes)
        / nullIf(sum(ProfileEvent_CachedReadBufferReadFromCacheHits), 0) AS FromCache,
    sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)
        / nullIf(sum(ProfileEvent_CachedReadBufferReadFromCacheMisses), 0) AS FromSource
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
            { "title", "Write-through covering segments shrunk (per second)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(ProfileEvent_CachedWriteBufferCoveringSegmentShrunk) AS Shrunk,
    avg(ProfileEvent_CachedWriteBufferCoveringSegmentShrinkFailed) AS ShrinkFailed
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
    avg(CurrentMetric_CacheFileSegments) AS FileSegments,
    avg(CurrentMetric_CacheDetachedFileSegments) AS DetachedFileSegments
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
        {
            { "dashboard", "Filesystem cache" },
            { "title", "Average cached file segment size (bytes)" },
            { "query", trim(R"EOQ(
WITH toDateTimeOrDefault({from:String}, '', now() - {seconds:UInt32}) AS from,
    toDateTimeOrDefault({to:String}, '', now()) AS to
SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t,
    avg(CurrentMetric_FilesystemCacheSize)
        / nullIf(avg(CurrentMetric_FilesystemCacheElements), 0) AS AverageSize
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
    avg(ProfileEvent_CacheWarmerBytesDownloaded) AS BytesDownloaded,
    avg(ProfileEvent_CacheWarmerDataPartsDownloaded) AS DataPartsDownloaded,
    avg(CurrentMetric_CacheWarmerBytesInProgress) AS BytesInProgress
FROM merge('system', '^metric_log')
WHERE event_date BETWEEN toDate(from) AND toDate(to) AND event_time BETWEEN from AND to
GROUP BY t
ORDER BY t WITH FILL STEP {rounding:UInt32}
)EOQ") }
        },
    };
    return dashboards;
}

}
