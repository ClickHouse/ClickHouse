#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/FetchMachineRunner.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/HistogramMetrics.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/getThreadId.h>
#include <Interpreters/ReaderExecutorLog.h>
#include <chrono>

#include "config.h"

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
    extern const Event ReaderExecutorBytesFromPageCache;
    extern const Event ReaderExecutorBytesFromFilesystemCache;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorBytesPushedToCacheSync;
    extern const Event ReaderExecutorBytesPushedToCacheAsync;
    extern const Event ReaderExecutorBytesPromoted;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorOverReadBytes;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorCacheGetMicroseconds;
    extern const Event ReaderExecutorCachePopulateMicroseconds;
    extern const Event ReaderExecutorSourceReadMicroseconds;
    extern const Event ReaderExecutorDecryptMicroseconds;
    extern const Event ReaderExecutorPrefetchWaitMicroseconds;
    extern const Event ReaderExecutorSyncReadMicroseconds;
    extern const Event ReaderExecutorWorkMicroseconds;
    extern const Event ReaderExecutorPrefetchHits;
    extern const Event ReaderExecutorPrefetchCancelled;
    extern const Event ReaderExecutorPrefetchPoolFull;
    extern const Event ReaderExecutorPrefetchDiscardedRunning;
    extern const Event ReaderExecutorPrefetchDiscardWaitMicroseconds;
    extern const Event ReaderExecutorPrefetchIssuedSourceBytes;
    extern const Event ReaderExecutorPrefetchIssuedCacheBytes;
    extern const Event ReaderExecutorPrefetchWastedSourceBytes;
    extern const Event ReaderExecutorPrefetchWastedCacheBytes;
    extern const Event ReaderExecutorMachineInterrupted;
    extern const Event ReaderExecutorPartialCollects;
    extern const Event ReaderExecutorPutScheduled;
    extern const Event ReaderExecutorPutPoolFull;
    extern const Event ReaderExecutorPutAbandoned;
    extern const Event ReaderExecutorPutFailed;
    extern const Event ReaderExecutorPutWaitMicroseconds;
    extern const Event ReaderExecutorPromoteSkipped;
    extern const Event ReaderExecutorBufferSlotAcquired;
    extern const Event ReaderExecutorBufferSlotFailed;
}

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorActive;
    extern const Metric ReaderExecutorPrefetchInFlight;
}

namespace HistogramMetrics
{
    extern Metric & ReaderExecutorCacheReadLatency;
    extern Metric & ReaderExecutorCachePopulateLatency;
    extern Metric & ReaderExecutorSourceReadLatency;
    extern Metric & ReaderExecutorPrefetchWaitLatency;
    extern Metric & ReaderExecutorSyncReadLatency;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::FailPoints
{
    /// Pauses after a sequential window has filled and pinned its in-flight
    /// FileCache segment, so a test can drop/evict the cache and verify the
    /// pinned segment survives. No-op unless enabled via `SYSTEM ENABLE FAILPOINT`.
    extern const char reader_executor_pause_after_window[];
    /// Pauses after a cache handle reported a hit but before `get` reads it, so a
    /// test can drop the cache in that window and verify the hit is still honored
    /// (the handle's holder keeps the segment non-releasable). No-op unless enabled.
    extern const char reader_executor_pause_after_cache_status[];
}

#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromMemory.h>
#endif

#include <Common/logger_useful.h>
#include <Core/LogsLevel.h>
#include <Common/VectorWithMemoryTracking.h>
#include <algorithm>
#include <cstring>

namespace DB
{

/// The ONE place a counter is mapped to its ProfileEvent. Bump the counter, emit the event,
/// and (for the cost-model counters) add the modeled-cost contribution - so a running query's
/// events advance as the read happens. The prefetch worker runs in the submitter's thread
/// group (attached by `PrefetchThreadPool`), so a worker-thread emit attributes to the query.
/// The bytes term's per-increment integer rounding is negligible against the millisecond model.
void ReaderExecutor::Stats::add(Counter c, UInt64 value)
{
    values[c] += value;
    switch (c)
    {
        case BytesFromPageCache:        ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromPageCache, value); break;
        case BytesFromFilesystemCache:  ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromFilesystemCache, value); break;
        case BytesFromSource:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromSource, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 20000ULL * value / (1024 * 1024));
            break;
        case BytesPushedToCacheSync:    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesPushedToCacheSync, value); break;
        case BytesPushedToCacheAsync:   ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesPushedToCacheAsync, value); break;
        case BytesPromoted:             ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesPromoted, value); break;
        case CacheGetRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 50 * value);
            break;
        case CachePopulateRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 100 * value);
            break;
        case SourceRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 30000 * value);
            break;
        case IncompleteConnections:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorIncompleteConnections, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 5000 * value);
            break;
        case OverReadBytes:             ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadBytes, value); break;
        case RequestedBytes:            ProfileEvents::increment(ProfileEvents::ReaderExecutorRequestedBytes, value); break;
        case CacheGetMicroseconds:      ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetMicroseconds, value); break;
        case CachePopulateMicroseconds: ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateMicroseconds, value); break;
        case SourceReadMicroseconds:    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceReadMicroseconds, value); break;
        case DecryptMicroseconds:       ProfileEvents::increment(ProfileEvents::ReaderExecutorDecryptMicroseconds, value); break;
        case PrefetchWaitMicroseconds:  ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWaitMicroseconds, value); break;
        case SyncReadMicroseconds:      ProfileEvents::increment(ProfileEvents::ReaderExecutorSyncReadMicroseconds, value); break;
        case WorkMicroseconds:          ProfileEvents::increment(ProfileEvents::ReaderExecutorWorkMicroseconds, value); break;
        case PrefetchHits:              ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchHits, value); break;
        case PrefetchCancelled:         ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchCancelled, value); break;
        case PrefetchPoolFull:          ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchPoolFull, value); break;
        case PrefetchSkippedResident:   break;  /// report-only: no ProfileEvent
        case PrefetchDiscardedRunning:  ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardedRunning, value); break;
        case PrefetchDiscardWaitMicroseconds: ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardWaitMicroseconds, value); break;
        case PrefetchIssuedSourceBytes: ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchIssuedSourceBytes, value); break;
        case PrefetchIssuedCacheBytes:  ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchIssuedCacheBytes, value); break;
        case PrefetchWastedSourceBytes: ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWastedSourceBytes, value); break;
        case PrefetchWastedCacheBytes:  ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWastedCacheBytes, value); break;
        case MachineInterrupted:        ProfileEvents::increment(ProfileEvents::ReaderExecutorMachineInterrupted, value); break;
        case PartialCollects:           ProfileEvents::increment(ProfileEvents::ReaderExecutorPartialCollects, value); break;
        case PutScheduled:              ProfileEvents::increment(ProfileEvents::ReaderExecutorPutScheduled, value); break;
        case PutPoolFull:               ProfileEvents::increment(ProfileEvents::ReaderExecutorPutPoolFull, value); break;
        case PutAbandoned:              ProfileEvents::increment(ProfileEvents::ReaderExecutorPutAbandoned, value); break;
        case PutFailed:                 ProfileEvents::increment(ProfileEvents::ReaderExecutorPutFailed, value); break;
        case PutWaitMicroseconds:       ProfileEvents::increment(ProfileEvents::ReaderExecutorPutWaitMicroseconds, value); break;
        case PromoteSkipped:            ProfileEvents::increment(ProfileEvents::ReaderExecutorPromoteSkipped, value); break;
        case NumCounters:               break;
    }
}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<IFileBasedSourceReader> source_,
    const StoredObjects & objects,
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches_,
    size_t window_size_,
    size_t min_bytes_for_seek_,
    size_t block_size_,
    String log_file_path_,
    size_t max_tail_for_drain_,
    size_t plan_look_ahead_window_)
    : source(std::move(source_))
    , stored_objects(objects)
    , caches(std::move(caches_))
    , log_file_path(std::move(log_file_path_))
    , window_size(window_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , block_size(block_size_)
    , max_tail_for_drain(max_tail_for_drain_)
    , live_connection_min_read_bytes(window_size_)
    , plan_look_ahead_window(std::max(plan_look_ahead_window_, window_size_))
    , active_metric(CurrentMetrics::ReaderExecutorActive)
{
    if (window_size == 0 || block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "reader_executor_window_size and reader_executor_block_size must be > 0, "
            "got window_size={}, block_size={}", window_size, block_size);

    offset_map.build(stored_objects);
    creator_query_id = String(CurrentThread::getQueryId());
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, block_size={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, block_size, caches.size());
}

ReaderExecutor::~ReaderExecutor()
{
    /// Cleanup, not a seek-away (not counted as a cancellation). The abandon slot is
    /// pre-reserved at machine-launch time, so stashing the in-flight machine here never
    /// allocates - safe from this `noexcept` destructor.
    cancelMachine(/*cancelled=*/false);
    drainAbandonedMachines(/*wait_finished=*/true);
    /// Deferred fills hold plan writers; let the bounded writes finish (one
    /// window of local I/O each) so bytes in hand are not dropped.
    sweepPutMachines(/*wait=*/true);

    /// A transient `readBigAt` executor rolls its stats into the parent via
    /// mergeTransientStats; emitting ProfileEvents / a reader_executor_log row
    /// here too would double-count. The parent's destructor reports the aggregate.
    if (is_transient)
        return;

    /// A live connection still open here was never drained to its bound (else
    /// releaseLiveConnectionAtBound would have reset it): an incomplete connection.
    /// (cancelMachine above already reclaimed/dropped any in-flight machine's `conn`.)
    accountLiveConnectionDrop(foreground_connection_state, /*at_eof=*/false, stats);

    LOG_DEBUG(log,
        "Destroyed: from_page_cache={} from_filesystem_cache={} from_source={} "
        "pushed_to_cache_sync={} pushed_to_cache_async={} "
        "get_reqs={} populate_reqs={} src_reqs={} "
        "get_us={} populate_us={} src_us={} decrypt_us={} "
        "prefetch_wait_us={} sync_read_us={} work_us={} "
        "prefetch_hits={} prefetch_cancelled={} prefetch_pool_full={} "
        "prefetch_discarded_running={} prefetch_discard_wait_us={} "
        "prefetch_issued_source_bytes={} prefetch_issued_cache_bytes={} "
        "prefetch_wasted_source_bytes={} prefetch_wasted_cache_bytes={} "
        "incomplete_connections={} over_read_bytes={}",
        stats.get(Stats::BytesFromPageCache), stats.get(Stats::BytesFromFilesystemCache), stats.get(Stats::BytesFromSource),
        stats.get(Stats::BytesPushedToCacheSync), stats.get(Stats::BytesPushedToCacheAsync),
        stats.get(Stats::CacheGetRequests), stats.get(Stats::CachePopulateRequests), stats.get(Stats::SourceRequests),
        stats.get(Stats::CacheGetMicroseconds), stats.get(Stats::CachePopulateMicroseconds),
        stats.get(Stats::SourceReadMicroseconds), stats.get(Stats::DecryptMicroseconds),
        stats.get(Stats::PrefetchWaitMicroseconds), stats.get(Stats::SyncReadMicroseconds), stats.get(Stats::WorkMicroseconds),
        stats.get(Stats::PrefetchHits), stats.get(Stats::PrefetchCancelled), stats.get(Stats::PrefetchPoolFull),
        stats.get(Stats::PrefetchDiscardedRunning), stats.get(Stats::PrefetchDiscardWaitMicroseconds),
        stats.get(Stats::PrefetchIssuedSourceBytes), stats.get(Stats::PrefetchIssuedCacheBytes),
        stats.get(Stats::PrefetchWastedSourceBytes), stats.get(Stats::PrefetchWastedCacheBytes),
        stats.get(Stats::IncompleteConnections), stats.get(Stats::OverReadBytes));

    if (reader_executor_log)
    {
        ReaderExecutorLogElement elem;
        elem.event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        elem.query_id = creator_query_id;
        elem.source_file_path = log_file_path;
        /// Logical (user-visible) bytes — `totalSize()` subtracts
        /// `data_start_offset` for encrypted reads so the value lines up
        /// with the per-tier byte counters. `nullopt` when the underlying
        /// object had `StoredObject::UnknownSize`.
        elem.total_size = offset_map.hasUnknownSize()
            ? std::optional<UInt64>{}
            : std::optional<UInt64>{totalSize()};
        elem.bytes_from_page_cache = stats.get(Stats::BytesFromPageCache);
        elem.bytes_from_filesystem_cache = stats.get(Stats::BytesFromFilesystemCache);
        elem.bytes_from_source = stats.get(Stats::BytesFromSource);
        elem.bytes_pushed_to_cache_sync = stats.get(Stats::BytesPushedToCacheSync);
        elem.bytes_pushed_to_cache_async = stats.get(Stats::BytesPushedToCacheAsync);
        elem.cache_get_requests = stats.get(Stats::CacheGetRequests);
        elem.cache_populate_requests = stats.get(Stats::CachePopulateRequests);
        elem.source_requests = stats.get(Stats::SourceRequests);
        elem.incomplete_connections = stats.get(Stats::IncompleteConnections);
        elem.over_read_bytes = stats.get(Stats::OverReadBytes);
        elem.cache_get_us = stats.get(Stats::CacheGetMicroseconds);
        elem.cache_populate_us = stats.get(Stats::CachePopulateMicroseconds);
        elem.source_read_us = stats.get(Stats::SourceReadMicroseconds);
        elem.decrypt_us = stats.get(Stats::DecryptMicroseconds);
        elem.prefetch_wait_us = stats.get(Stats::PrefetchWaitMicroseconds);
        elem.sync_read_us = stats.get(Stats::SyncReadMicroseconds);
        elem.prefetch_hits = stats.get(Stats::PrefetchHits);
        elem.prefetch_cancelled = stats.get(Stats::PrefetchCancelled);
        elem.prefetch_pool_full = stats.get(Stats::PrefetchPoolFull);
        elem.prefetch_discarded_running = stats.get(Stats::PrefetchDiscardedRunning);
        elem.prefetch_discard_wait_us = stats.get(Stats::PrefetchDiscardWaitMicroseconds);
        elem.prefetch_issued_source_bytes = stats.get(Stats::PrefetchIssuedSourceBytes);
        elem.prefetch_issued_cache_bytes = stats.get(Stats::PrefetchIssuedCacheBytes);
        elem.prefetch_wasted_source_bytes = stats.get(Stats::PrefetchWastedSourceBytes);
        elem.prefetch_wasted_cache_bytes = stats.get(Stats::PrefetchWastedCacheBytes);

        /// `SystemLogQueue::push_back` allocates and can throw; this is a `noexcept`
        /// destructor (often unwinding from another exception), so suppress and log
        /// rather than `std::terminate`. The log row is best-effort observability.
        try
        {
            reader_executor_log->add(std::move(elem));
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to emit reader_executor_log row", LogsLevel::debug);
        }
    }
}

VectorWithMemoryTracking<ByteRange> ReaderExecutor::mergeRanges(const VectorWithMemoryTracking<ByteRange> & ranges, size_t min_gap)
{
    if (ranges.empty() || min_gap == 0)
        return ranges;

    VectorWithMemoryTracking<ByteRange> sorted = ranges;
    std::sort(sorted.begin(), sorted.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });

    VectorWithMemoryTracking<ByteRange> merged;
    merged.push_back(sorted[0]);

    for (size_t i = 1; i < sorted.size(); ++i)
    {
        auto & prev = merged.back();
        /// Saturating subtraction: overlapping ranges (sorted[i].offset < prev.end())
        /// collapse to gap = 0 and merge via the same branch as adjacent ranges.
        size_t gap = sorted[i].offset > prev.end() ? sorted[i].offset - prev.end() : 0;

        if (gap <= min_gap)
        {
            size_t new_end = std::max(prev.end(), sorted[i].end());
            prev.size = new_end - prev.offset;
        }
        else
        {
            merged.push_back(sorted[i]);
        }
    }

    return merged;
}

ReaderExecutor::FetchMachine::FetchMachine()
    : inflight_gauge(CurrentMetrics::ReaderExecutorPrefetchInFlight)
{
}

void ReaderExecutor::setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool)
{
    prefetch_pool = std::move(pool);
    runner = prefetch_pool ? std::make_unique<FetchMachineRunner>(prefetch_pool) : nullptr;
}

void ReaderExecutor::setBufferLimit(std::shared_ptr<LiveConnectionLimit> limit)
{
    buffer_limit = std::move(limit);
}

void ReaderExecutor::setLiveConnectionMinReadBytes(size_t bytes)
{
    /// 0 keeps the `window_size` default the constructor set.
    if (bytes)
        live_connection_min_read_bytes = bytes;
}

void ReaderExecutor::setReaderExecutorLog(std::shared_ptr<ReaderExecutorLog> log_)
{
    reader_executor_log = std::move(log_);
}

ReaderExecutor::ConnState::ConnState() = default;
ReaderExecutor::ConnState::~ConnState() = default;

ReaderExecutor::ConnState::ConnState(ConnState && other) noexcept
    : connection(std::move(other.connection))
    , inflight_segment_pin(std::move(other.inflight_segment_pin))
{
    other.connection.reset();
    other.inflight_segment_pin = {};
}

ReaderExecutor::ConnState & ReaderExecutor::ConnState::operator=(ConnState && other) noexcept
{
    if (this != &other)
    {
        connection = std::move(other.connection);
        inflight_segment_pin = std::move(other.inflight_segment_pin);
        other.connection.reset();
        other.inflight_segment_pin = {};
    }
    return *this;
}

LiveConnectionSlot ReaderExecutor::acquireSlotCounted()
{
    auto slot = buffer_limit->tryAcquire(buffer_limit);
    if (slot)
        ProfileEvents::increment(ProfileEvents::ReaderExecutorBufferSlotAcquired);
    else
        ProfileEvents::increment(ProfileEvents::ReaderExecutorBufferSlotFailed);
    return slot;
}

void ReaderExecutor::acquireLeaseIfWide()
{
    /// Take the live-connection lease only when the GAP at the cursor is wider than a
    /// window - such a gap is streamed across several window reads on one kept-live
    /// connection, so it is worth a global-limit unit. A gap that fits in a single window
    /// (a mostly-resident plan with small scattered misses, or the tail) is served by a
    /// one-shot and needs no lease. This is more precise than the plan span: a wide plan
    /// can still be mostly cache hits with only tiny gaps. Applies to a `readBigAt`
    /// transient too: its plan is clamped to the requested extent, so a wide (8-32 MB)
    /// random read takes its own live connection while a small one stays a one-shot. Skips
    /// a lease already held; `readFromSource` only reads the lease, never takes it.
    /// Best-effort: empty at capacity, in which case the read falls back to a one-shot.
    if (connection_lease || !buffer_limit || !read_plan.geometry())
        return;
    /// Anchor at the next real gap (robust even if the cursor is somehow resident), then
    /// measure how far a live connection would stream/bridge from there. A reach beyond a
    /// window means the connection is reused across windows (or bridges scattered cached
    /// holes), so it is worth a lease; otherwise a one-shot serves the read.
    const size_t pos = position + data_start_offset;
    const size_t gap_start = read_plan.geometry()->nextGapStart(pos);
    if (gap_start >= read_plan.geometry()->plan_end)
        return;
    const size_t reach = read_plan.geometry()->streamReach(gap_start, min_bytes_for_seek);
    const bool wide = reach - gap_start > live_connection_min_read_bytes;
    LOG_TRACE(log, "acquireLeaseIfWide: gap [{}, {}) span={} threshold={} wide={}",
        gap_start, reach, reach - gap_start, live_connection_min_read_bytes, wide);
    if (wide)
        connection_lease = acquireSlotCounted();
}

namespace
{

struct WindowAndBlock
{
    size_t window_bytes;
    size_t block_bytes;
};

/// Divisors applied to the configured base window/block sizes, indexed by
/// `MemoryPressureLevel` (Normal, Elevated, High, Critical). Normal divides by
/// 1 (the configured base); higher pressure shrinks more. Per-level arrays so
/// each step is tunable independently.
constexpr size_t WINDOW_REDUCTION[memoryPressureLevelCount()] = {1, 4, 16, 64};
constexpr size_t BLOCK_REDUCTION[memoryPressureLevelCount()]  = {1, 2, 2,  8};

/// Whether read-ahead runs at each `MemoryPressureLevel`. Prefetch is speculative —
/// a seek-away wastes both the bytes it read and the memory holding them — so it is
/// suppressed entirely once memory is High/Critical. When it runs it reads the same
/// window as a synchronous read (no prefetch-specific reduction).
constexpr bool PREFETCH_ENABLED[memoryPressureLevelCount()] = {true, true, false, false};

/// The configured base is the ceiling; the 128 KiB floor only bounds the
/// pressure shrink and never raises a base that is itself below it (e.g. a tiny
/// test/manual window). The block never exceeds the window.
WindowAndBlock sizesAtPressure(MemoryPressureLevel pressure, size_t base_window, size_t base_block)
{
    const size_t level = static_cast<size_t>(pressure);
    static constexpr size_t FLOOR = 128ULL << 10;
    const size_t window = std::min(std::max(base_window / WINDOW_REDUCTION[level], FLOOR), base_window);
    size_t block = std::min(std::max(base_block / BLOCK_REDUCTION[level], FLOOR), base_block);
    block = std::min(block, window);
    return {window, block};
}

}

size_t ReaderExecutor::effectiveBlockSize(MemoryPressureLevel level) const
{
    return sizesAtPressure(level, window_size, block_size).block_bytes;
}

size_t ReaderExecutor::effectiveWindowSize(MemoryPressureLevel level) const
{
    const auto sizes = sizesAtPressure(level, window_size, block_size);
    /// Only the live path streams one block at a time, reusing the open
    /// connection across windows. Stateless reads - local files and remote
    /// reads with live connections disabled - keep the full (pressure-scaled)
    /// window so each one-shot open amortises its setup over a window, not a
    /// block.
    /// `machine` covers the in-flight window: while a machine is in flight the
    /// connection cluster has been moved into it, so `foreground_connection_state` is
    /// empty even though a live connection conceptually exists - treat that as the
    /// live path too, else the sizing flips to a full window mid-stream.
    if (foreground_connection_state.connection || connection_lease || machine)
        return sizes.block_bytes;
    return sizes.window_bytes;
}

size_t ReaderExecutor::effectivePrefetchWindowSize(MemoryPressureLevel level) const
{
    if (!PREFETCH_ENABLED[static_cast<size_t>(level)])
        return 0;
    /// Prefetch reads the same window as a synchronous read; under High/Critical it
    /// is suppressed entirely (above) rather than shrunk.
    return effectiveWindowSize(level);
}

size_t ReaderExecutor::clampToExtent(size_t win_size) const
{
    if (!read_extent_end)
        return win_size;
    const size_t remaining = *read_extent_end > position ? *read_extent_end - position : 0;
    return std::min(win_size, remaining);
}

size_t ReaderExecutor::boundedReadSize(size_t want) const
{
    if (!offset_map.hasUnknownSize())
        want = std::min(want, totalSize() - position);
    return clampToExtent(want);
}

void ReaderExecutor::releaseLiveConnectionAtBound(ConnState & conn) const
{
    if (conn.connection && conn.connection->atBound())
    {
        conn.connection.reset();
        conn.inflight_segment_pin.reset();
    }
}

void ReaderExecutor::accountLiveConnectionDrop(ConnState & conn, bool at_eof, Stats & out_stats) const
{
    /// A connection dropped before it was fully consumed (not read to its right
    /// bound or to EOF) is abandoned mid-response and not pool-reusable.
    if (conn.connection && !conn.connection->isComplete(at_eof))
        out_stats.add(Stats::IncompleteConnections);
}

void ReaderExecutor::maybeKeepLiveConnectionBefore(size_t next_physical, ConnState & conn, bool eof_latch, Stats & out_stats) const
{
    if (!conn.connection || eof_latch)
        return;

    /// Keep the connection only if the next read continues it forward within its
    /// bound (a small, bridgeable gap on the same object); otherwise drain its tail
    /// and drop it so the slot is not held idle.
    size_t next_obj_file_offset = 0;
    const StoredObject * next_obj = offset_map.findObjectAt(next_physical, &next_obj_file_offset);
    const bool keep = next_obj
        && conn.connection->object_path == next_obj->remote_path
        && conn.connection->canContinueTo(next_physical - next_obj_file_offset, min_bytes_for_seek);
    if (!keep)
        dropLiveConnection(conn, out_stats);
    /// The lease (`connection_lease`) is not touched here - it is `const`. The caller's
    /// end-of-`readNextWindow` check releases it once this leaves no live connection.
}

bool ReaderExecutor::maybeDrainLiveTail(ConnState & conn, Stats & out_stats) const
{
    /// Drain a small remaining tail before dropping a live connection so it completes
    /// and is returned to the pool reusable rather than counted incomplete. The
    /// drained bytes cross the wire (over-read) - worth it only below the
    /// I-weight/bandwidth breakeven, bounded by `max_tail_for_drain`.
    ///
    /// Returns whether the drain reached EOF *before* the bound: a source shorter than
    /// its advertised right bound ends inside the tail, so the connection is spent at
    /// EOF (complete and reusable) yet NOT `atBound()` - the caller must account the drop
    /// as an EOF drop, not an abandoned one. A drain that reaches the bound returns
    /// false (the bound itself makes it complete) as does a no-op drain.
    if (!conn.connection)
        return false;
    const size_t drained = conn.connection->drainTail(max_tail_for_drain, block_size);
    out_stats.add(Stats::BytesFromSource, drained);
    out_stats.add(Stats::OverReadBytes, drained);
    return drained > 0 && !conn.connection->atBound();
}

void ReaderExecutor::dropLiveConnection(ConnState & conn, Stats & out_stats) const
{
    /// Close a live connection: drain a small tail (so it returns to the pool reusable),
    /// account a still-incomplete drop (the drain reports whether it ended at EOF), then
    /// clear the connection and its in-flight segment pin. Does NOT touch the lease - the
    /// lease (`connection_lease`) is owned by the plan and managed in `planResidencyWindow`
    /// / at EOF, not per connection-close.
    if (!conn.connection)
        return;
    const bool drained_to_eof = maybeDrainLiveTail(conn, out_stats);
    accountLiveConnectionDrop(conn, /*at_eof=*/drained_to_eof, out_stats);
    conn.connection.reset();
    conn.inflight_segment_pin.reset();
}

void ReaderExecutor::setReadExtent(std::optional<size_t> logical_end)
{
    if (logical_end == read_extent_end)
        return;

    /// The extent only advances or clears; it must not move below the read cursor,
    /// which would strand already-buffered bytes beyond the new bound. MergeTree
    /// advances the mark-range end per task and never rewinds it; a backward shrink
    /// would need explicit buffer trimming, which the executor does not support.
    chassert(!logical_end || *logical_end >= position);

    /// Drain any in-flight machine before changing the extent: the worker reads
    /// `read_extent_end` to bound its source connection, so mutating it
    /// underneath the worker would race, and a read-ahead issued for the old
    /// range must not be served for the new one. No-op when no machine is in
    /// flight (the common per-mark-range boundary, where prefetch is clamped to
    /// the extent), so it is free on the hot path. Mirrors the legacy
    /// `AsynchronousBoundedReadBuffer::setReadUntilPosition` contract.
    cancelMachine(/*cancelled=*/true);
    read_extent_end = logical_end;
}

void ReaderExecutor::maybeTriggerPrefetch()
{
    if (!prefetch_pool || machine || atEnd())
        return;

    drainAbandonedMachines();

    const size_t position_phys = position + data_start_offset;

    /// The live-connection lease is decided per plan in `planResidencyWindow`, not here,
    /// so this path takes/releases nothing - it only refreshes the plan, skips a resident
    /// cursor, and launches the read-ahead, telling the worker whether the plan is leased.

    /// Bound the read-ahead to the file end and the advertised extent. `residentAt` is a
    /// point query, so this plain `window_size` probe (no pressure-scaled sizing) is
    /// enough to refresh and consult the plan. At the boundary there is nothing to read
    /// ahead - return.
    const size_t probe_size = boundedReadSize(window_size);
    if (probe_size == 0)
        return;

    /// Read-ahead the FIRST GAP in the plan at or after the cursor (`nextGapStart`), even
    /// when the cursor itself is resident: the gap fills in the background while the resident
    /// run before it streams from cache (the resident/prefetch overlap). Skip only when the
    /// plan holds no gap left to fetch (everything resident to `plan_end`).
    if (!read_plan.geometry() || !read_plan.geometry()->covers(ByteRange{position_phys, probe_size}))
        planResidencyWindow(position_phys);
    const size_t gap_start = read_plan.geometry()->nextGapStart(position_phys);
    if (gap_start >= read_plan.geometry()->plan_end)
    {
        LOG_TRACE(log, "Prefetch: no gap ahead of {} in plan, nothing to read ahead", position);
        stats.add(Stats::PrefetchSkippedResident);
        return;
    }

    /// A gap within the extent: commit to a prefetch. The live-connection lease
    /// (`connection_lease`) was decided per plan in `planResidencyWindow`; this path only
    /// reads it (via `job->leased`), never acquires/releases it.
    const size_t prefetch_window = effectivePrefetchWindowSize(read_plan.geometry()->pressure_level);
    if (prefetch_window == 0)
        return;  /// read-ahead suppressed under High/Critical memory pressure

    size_t next_size = boundedReadSize(prefetch_window);
    if (next_size == 0)
        return;  /// at the file end / extent boundary, nothing left to prefetch

    /// Bound the read-ahead to the gap `[gap_start, gapEnd)`, mirroring the synchronous gap
    /// read: one pure run per fetch, never straddling a resident run. The gap clamp keeps it
    /// within the plan (and thus the extent) even though `boundedReadSize` measured from the
    /// cursor.
    next_size = std::min(next_size, read_plan.geometry()->gapEnd(gap_start) - gap_start);
    const size_t next_logical_offset = gap_start - data_start_offset;

    /// Align the worker's fetch to the cache cells from the plan's immutable geometry
    /// (`fetchWindowAt` unions the aligned miss ranges - whole page-cache blocks,
    /// disk-segment boundary - that overlap this gap): the worker is a pure source fetch
    /// and cannot align itself, so the foreground bounds the aligned window here so the
    /// collect `write` lands aligned in every tier. The machine's `requested_range` stays
    /// the logical REQUESTED range (seek and the collect slice work in that space); collect
    /// backfills the aligned `physical_window` and slices back to the request.
    const ByteRange next_physical_window = read_plan.geometry()->fetchWindowAt(ByteRange{gap_start, next_size});

    LOG_TRACE(log, "Prefetch: submitting physical [{}, {}) (requested gap [{}, {}))",
        next_physical_window.offset, next_physical_window.end(), gap_start, gap_start + next_size);

    /// The co-owned machine: the worker accumulates served-byte counters into
    /// `m->stats` (never the shared `this->stats`) and operates ONLY on `m->conn`
    /// (never the shared `foreground_connection_state`). Merged/reclaimed at collect.
    auto m = std::make_shared<FetchMachine>();

    /// Reserve the stash slot up front so a later cancel of this machine (seek or
    /// the readNextWindow revoke path) can move it into `abandoned_machines`
    /// WITHOUT allocating. A `push_back` realloc there could throw; on the revoke
    /// path that drops the machine before its queued job is joined (the no-op
    /// pickup still runs - use-after-free). Capacity is retained across drains, so
    /// this allocates only on the first launch; reserving here keeps it off the
    /// hot cancel paths.
    abandoned_machines.reserve(abandoned_machines.size() + 1);

    /// Track the LOGICAL requested range (the space `position`, seek and the
    /// collect slice work in) and the PHYSICAL, cache-aligned window the worker
    /// fetches - collect backfills the caches over the latter (so each tier's
    /// `put` aligns), pins at its frontier, and slices back to the former.
    m->requested_range = ByteRange{next_logical_offset, next_size};
    m->physical_window = next_physical_window;
    /// Immutable snapshot, co-owned by design; the worker consults only its
    /// cached `pressure_level` (no cache lookup, no resident serve), so there
    /// is nothing for a foreground re-plan to race.
    m->geometry = read_plan.geometry();

    /// Hand the source-connection cluster (live connection + pin) to the machine:
    /// `foreground_connection_state` goes EMPTY, so the worker - which operates on
    /// `m->conn` - cannot touch any foreground member (the connection
    /// use-after-free is a compile-time impossibility). Must run AFTER the early
    /// returns above (they act on `foreground_connection_state`) and BEFORE
    /// schedule (the worker may start the instant it returns). Reclaimed at
    /// collect / revoke, or dropped on a running-discard.
    /// NB: `std::optional`'s move leaves the SOURCE engaged (holding a moved-from
    /// value), so `std::move` alone would leave `foreground_connection_state`'s
    /// optionals truthy-but-empty - explicitly clear it so it is genuinely empty.
    /// The reclaim paths assert this, and `effectiveWindowSize` consults `machine`
    /// (not `foreground_connection_state`) while one is in flight.
    m->conn = std::move(foreground_connection_state);
    foreground_connection_state = {};

    /// Take the live-connection lease for this read-ahead iff the plan is wide (and one
    /// is not already held), then tell the worker whether it is leased so it opens a
    /// kept-live connection vs a one-shot WITHOUT reading the shared `connection_lease`.
    acquireLeaseIfWide();
    m->leased = static_cast<bool>(connection_lease);

    /// The machine's single step: a PURE source fetch of the pre-bounded aligned
    /// gap window into machine-owned state - no shared `this->`, no cache, no
    /// mutable plan. The foreground does the cache backfill + logical shift at
    /// collect. `interrupt_requested` is polled between source blocks; a set flag
    /// wraps the step up with the partial rope as its product - the executor then
    /// keeps it (collect takeover) or destroys it (cancel). (`self` stays valid
    /// for the step's whole run: the runner's pool job co-owns the machine.)
    m->run_step = [this, self = m.get()]
    {
        self->fetched = fetchGapsFromSource(
            self->physical_window, /*from_prefetch=*/true, /*keep_live=*/self->leased,
            self->conn, self->reached_eof, self->geometry->pressure_level,
            &self->interrupt_requested, self->stats);
        if (self->interrupt_requested.load())
        {
            self->stats.add(Stats::MachineInterrupted);
            return StepResult::Interrupted;
        }
        return StepResult::AwaitCollect;
    };

    /// The machine's window must not race a still-uncommitted fill of the same
    /// cells (its fetch would re-read them from the source). Writer ranges are
    /// NOT joined on: the fetch step never touches writers, and waiting here
    /// would serialize the next fetch behind the previous window's put whenever
    /// one writer spans both (the common case).
    joinPutMachinesOverlapping(next_physical_window, /*writers_too=*/false);

    if (!runner->schedule(m, StepKind::Fetch))
    {
        LOG_TRACE(log, "Prefetch: pool queue full, will fetch synchronously on next read");
        stats.add(Stats::PrefetchPoolFull);
        /// No worker ran (the queue rejected the step). Reclaim the cluster into
        /// `foreground_connection_state`. The lease was taken for the read-ahead that did
        /// not launch and no connection opened, so release it unless the reclaimed cluster
        /// still has a live connection (a kept one needs it); the next sync read re-takes.
        foreground_connection_state = std::move(m->conn);
        if (!foreground_connection_state.connection)
            connection_lease = {};
        return;
    }

    machine = std::move(m);
}

void ReaderExecutor::cancelMachine(bool cancelled)
{
    drainAbandonedMachines();

    auto m = std::move(machine);
    if (!m)
        return;

    LOG_TRACE(log, "Prefetch: discarding [{}, {})", m->requested_range.offset, m->requested_range.end());

    if (runner->tryCancelQueued(*m))
    {
        /// Revoked before the worker ran - count it like the readNextWindow
        /// revoke path (but not destructor cleanup, which passes `cancelled=false`) so
        /// `ReaderExecutorPrefetchCancelled` / `reader_executor_log.prefetch_cancelled`
        /// includes seek-cancelled read-aheads.
        if (cancelled)
            stats.add(Stats::PrefetchCancelled);
        /// The worker provably never ran, so `m->conn` is the UNTOUCHED cluster
        /// handed over at launch - reclaim it into `foreground_connection_state`
        /// BEFORE stashing the machine, so the caller's connection logic (seek's
        /// keep/drop, or the destructor's `accountLiveConnectionDrop`) operates on
        /// the real connection instead of silently dropping it (which would
        /// regress R and mis-count an incomplete connection). Stats stay zero
        /// (worker never ran), so no merge.
        chassert(!foreground_connection_state.connection);
        foreground_connection_state = std::move(m->conn);
        abandoned_machines.push_back(std::move(m));
    }
    else
    {
        /// Already running (or finished); ask it to wrap up at the next interrupt
        /// point and block until the step releases the machine - the wait is
        /// bounded by one source block, not one window. Work wasted.
        stats.add(Stats::PrefetchDiscardedRunning);
        StatTimer wait_scope(stats, Stats::PrefetchDiscardWaitMicroseconds);
        runner->requestInterrupt(*m);
        runner->waitReleased(*m);
        if (m->failure)
            tryLogException(m->failure, log, "Discarded prefetch task threw", LogsLevel::debug);
        /// The worker advanced `m->conn` to the WRONG frontier (a seek /
        /// extent-change is discarding this read-ahead), so the connection is
        /// DROPPED rather than reclaimed - but a connection dropped before its
        /// bound is an incomplete connection, so account it BEFORE the drop
        /// (`~ConnState` at machine destruction would otherwise destroy it
        /// silently and under-count `I`). Don't drain its tail: the rope is
        /// discarded.
        if (m->conn.connection && !m->conn.connection->isComplete(/*at_eof=*/false))
            stats.add(Stats::IncompleteConnections);
        /// The worker ran, so the machine-local stats hold this read-ahead's
        /// I/O - it really happened, fold it in. Starting from zero, the
        /// machine's `prefetch_issued_*` ARE exactly this read-ahead's wasted
        /// bytes (read but never served or cached - collect is skipped).
        stats += m->stats;
        stats.add(Stats::PrefetchWastedSourceBytes, m->stats.get(Stats::PrefetchIssuedSourceBytes));
        stats.add(Stats::PrefetchWastedCacheBytes, m->stats.get(Stats::PrefetchIssuedCacheBytes));
        /// `m` is destroyed here: `~ConnState` drops the advanced connection
        /// (accounted above) and the in-flight gauge decrements.
    }
}

void ReaderExecutor::drainAbandonedMachines(bool wait_finished)
{
    abandoned_machines.erase(
        std::remove_if(abandoned_machines.begin(), abandoned_machines.end(),
            [this, wait_finished](std::shared_ptr<FetchMachine> & m)
            {
                if (!m->current_step)
                    return true;
                if (!wait_finished && !m->current_step->isFinished())
                    return false;
                try
                {
                    m->current_step->get();
                }
                catch (...)
                {
                    /// Cancellation throws `JobHandle: task was cancelled` — every
                    /// abandoned-on-revoke machine reaches here. Debug level keeps
                    /// the error log clean.
                    tryLogCurrentException(log, "Abandoned prefetch task threw", LogsLevel::debug);
                }
                return true;
            }),
        abandoned_machines.end());
}

void ReaderExecutor::addDecryptionLayer(
    [[maybe_unused]] String path,
    [[maybe_unused]] size_t buffer_size,
    [[maybe_unused]] KeyFinderFunc key_finder)
{
#if USE_SSL
    decryption_layers.push_back(DecryptionLayer{
        .path = std::move(path),
        .buffer_size = buffer_size,
        .key_finder = std::move(key_finder),
        .key = {},
    });
    data_start_offset = decryption_layers.size() * FileEncryption::Header::kSize;
    LOG_DEBUG(log, "Added decryption layer, data_start_offset={}", data_start_offset);
#endif
}

void ReaderExecutor::initDecryption()
{
#if USE_SSL
    if (decryption_initialized || decryption_layers.empty())
        return;

    size_t total_source_size = offset_map.totalSize();

    /// An empty underlying source (e.g. DiskObjectStorage's empty-file
    /// fallback for paths with no storage objects) has no encryption header.
    /// Skip — subsequent reads will return 0 bytes, matching the contract of
    /// reading an empty file on an unencrypted disk.
    if (total_source_size == 0)
    {
        LOG_DEBUG(log, "initDecryption: source is empty, skipping");
        return;
    }

    /// Source exists but is smaller than the header(s) — file is corrupted.
    if (total_source_size < data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source has {} bytes, less than header size {}",
            total_source_size, data_start_offset);

    LOG_DEBUG(log, "initDecryption: reading {} headers ({} bytes)",
        decryption_layers.size(), data_start_offset);

    /// No plan built yet at init time: pass an empty geometry so the header is read
    /// purely via the source/gap path. `serveLateHits` still serves a header byte already
    /// cached by a sibling reader (a read-only `planResidencyView` probe), but with no
    /// held write buffers the header itself is not populated here - it is read once and is
    /// tiny. Foreground call, so `foreground_connection_state` / `this->reached_eof`.
    ReadPlanGeometry init_geometry;
    Rope header_rope = readPhysicalWindow(ByteRange{0, data_start_offset},
        foreground_connection_state, init_geometry, reached_eof, stats);

    /// Under size-unknown sources `readPhysicalWindow` latches `reached_eof`
    /// on short returns instead of throwing, so an empty rope means
    /// "empty object" (same as the size-known empty branch above) and a
    /// partial rope means corrupted/truncated.
    if (offset_map.hasUnknownSize() && header_rope.totalBytes() == 0)
    {
        LOG_DEBUG(log, "initDecryption: unknown-size source returned 0 bytes (empty object), skipping");
        return;
    }
    if (header_rope.totalBytes() != data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source returned {} header bytes, expected {} (corrupted/truncated)",
            header_rope.totalBytes(), data_start_offset);

    /// Stacked encryption layout: only `h0` is plaintext; every later header
    /// is wrapped by all layers above it (`[h0, enc0(h1), enc0(enc1(h2)), ...]`).
    /// Parsing `hi` peels layers `0..i-1` at their current keystream offsets —
    /// same per-layer stepping as the payload path; see `decryptInPlace`.
    VectorWithMemoryTracking<FileEncryption::Encryptor> initialized_encryptors;
    initialized_encryptors.reserve(decryption_layers.size());
    size_t offset = 0;
    for (size_t i = 0; i < decryption_layers.size(); ++i)
    {
        auto & layer = decryption_layers[i];

        /// Copy the header's 64 bytes into a mutable buffer so we can
        /// decrypt in place across the already-initialized layers.
        std::array<char, FileEncryption::Header::kSize> hdr_bytes{};
        {
            Rope slice = header_rope.slice(ByteRange{offset, FileEncryption::Header::kSize});
            chassert(slice.totalBytes() == FileEncryption::Header::kSize);
            slice.copyTo(hdr_bytes.data(), ByteRange{offset, FileEncryption::Header::kSize});
        }

        for (size_t j = 0; j < initialized_encryptors.size(); ++j)
        {
            const size_t layer_keystream_offset = (i - 1 - j) * FileEncryption::Header::kSize;
            initialized_encryptors[j].setOffset(layer_keystream_offset);
            initialized_encryptors[j].decrypt(hdr_bytes.data(), hdr_bytes.size(), hdr_bytes.data());
        }

        ReadBufferFromMemory rb(hdr_bytes.data(), hdr_bytes.size());
        FileEncryption::Header header;
        header.read(rb);
        layer.key = layer.key_finder(header.key_fingerprint, layer.path);
        decryption_headers.push_back(std::move(header));

        /// Materialise this layer's encryptor for the next iteration to use.
        initialized_encryptors.emplace_back(
            decryption_headers.back().algorithm,
            layer.key,
            decryption_headers.back().init_vector);

        offset += FileEncryption::Header::kSize;

        LOG_DEBUG(log, "initDecryption: parsed header for {}, algorithm={}",
            layer.path, static_cast<int>(decryption_headers.back().algorithm));
    }

    decryption_initialized = true;
#endif
}

Rope ReaderExecutor::readWindowLogical(ByteRange physical_window, ConnState & conn,
    const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats)
{
    Rope rope = readPhysicalWindow(physical_window, conn, geometry, eof_latch, out_stats);
    /// Physical offsets include the encryption header prefix; the consumer works
    /// in logical (post-header) offsets. Shift once here. No-op when not encrypted.
    if (data_start_offset)
        rope.shift(-static_cast<ssize_t>(data_start_offset));
    return rope;
}

void ReaderExecutor::decryptInPlace(
    [[maybe_unused]] char * data, [[maybe_unused]] size_t size, [[maybe_unused]] size_t logical_offset)
{
#if USE_SSL
    if (decryption_layers.empty() || size == 0)
        return;

    StatTimer decrypt_scope(stats, Stats::DecryptMicroseconds);

    /// Build the per-layer CTR encryptors once and reuse them across served
    /// chunks. CTR is position-addressable, so each call just re-seeks the
    /// keystream. (Lazy: also covers the transient made by makeTransientForReadAt,
    /// which copies the parsed headers but not the encryptors.)
    if (payload_encryptors.empty())
    {
        payload_encryptors.reserve(decryption_layers.size());
        for (size_t i = 0; i < decryption_layers.size(); ++i)
            payload_encryptors.emplace_back(
                decryption_headers[i].algorithm,
                decryption_layers[i].key,
                decryption_headers[i].init_vector);
    }

    /// Per-layer keystream offset: with `N` layers (0 = outermost, N-1 =
    /// innermost), layer `i`'s stream carries the inner layers' headers ahead of
    /// the payload, so its CTR offset for `logical_offset` is
    /// `logical_offset + (N - 1 - i) * Header::kSize`; the innermost uses
    /// `logical_offset`. See `ReadBufferFromEncryptedFile::nextImpl`.
    for (size_t i = 0; i < payload_encryptors.size(); ++i)
    {
        const size_t layer_keystream_offset = logical_offset
            + (payload_encryptors.size() - 1 - i) * FileEncryption::Header::kSize;
        payload_encryptors[i].setOffset(layer_keystream_offset);
        payload_encryptors[i].decrypt(data, size, data);
    }
#endif
}

/// One window of bytes, or empty at EOF. An in-flight prefetch is consumed
/// FIRST, before the EOF gate: an unknown-size worker can latch `reached_eof`
/// while still returning the file's final bytes, so gating first would drop
/// them. With no prefetch, read synchronously (or return empty at EOF).
/// Releases the live connection + slot once EOF is latched, then reads one
/// window ahead.
Rope ReaderExecutor::readNextWindow()
{
    /// Total foreground time in the read call (planning, cache reads, source reads,
    /// prefetch waits) - the executor's direct contribution to query read latency.
    StatTimer work_timer(stats, Stats::WorkMicroseconds);

    /// Reap finished deferred fills; grant a parked one its reschedule.
    sweepPutMachines(/*wait=*/false);

    const size_t logical_size = totalSize();

    /// EOF return - but a machine launched before EOF can have its worker latch
    /// `reached_eof` via a short read on an unknown-size source while still holding
    /// the final bytes. Defer the EOF return until that machine is collected in the
    /// gap branch below; only return here once nothing is in flight.
    if (atEnd() && !machine)
    {
        LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
        /// Release per-stream resources at EOF instead of waiting for the caller to
        /// drop the `PipelineReadBuffer`; a subsequent seek-back re-opens and re-acquires.
        /// No machine is in flight here (`!machine`), so the cluster is on the
        /// foreground, not in a machine.
        accountLiveConnectionDrop(foreground_connection_state, /*at_eof=*/true, stats);
        foreground_connection_state.connection.reset();
        foreground_connection_state.inflight_segment_pin.reset();
        connection_lease = {};  /// scan done - release the plan's lease
        return {};
    }

    const size_t position_phys = position + data_start_offset;
    /// Pressure-free "is there anything left to read?" (the old `win_size > 0` guard,
    /// minus the `effectiveWindowSize` memory-pressure query): clamp to the advertised
    /// extent / file end. An unknown-size source has no known end here (EOF is latched
    /// by a short read), so it reads up to the extent (or is effectively unbounded).
    const size_t to_read = offset_map.hasUnknownSize()
        ? clampToExtent(window_size)
        : clampToExtent(logical_size - position);

    Rope rope;

    /// Plan-first: build/refresh the geometry and ask what sits at the cursor.
    /// RESIDENT -> stream the run from the held cache handle; GAP -> consume the
    /// in-flight gap read-ahead (launched last call) or fetch synchronously.
    ReadPlanGeometry::Resident at;
    if (to_read > 0)
    {
        /// Re-plan only when the cursor leaves the planned span. The margin is the
        /// BASE `window_size` (a constant), so deciding whether to plan never queries
        /// memory pressure; the plan span and every read are clamped to `plan_end`,
        /// and the per-plan pressure level is sampled once inside `planResidencyWindow`.
        /// NB: never re-plan while a machine is in flight. A read-ahead is launched only
        /// at a gap cursor, so this cursor IS a gap and must be collected via the gap
        /// branch below. A re-plan here would re-probe residency and could see the
        /// worker's just-fetched gap as RESIDENT, wrongly taking the resident
        /// fast-path while `machine` is still set (the invariant the
        /// `planResidencyWindow` `chassert(!machine)` guards).
        if (!machine
            && (!read_plan.geometry()
                || position_phys < read_plan.geometry()->plan_start
                || position_phys + window_size > read_plan.geometry()->plan_end))
            planResidencyWindow(position_phys);
        if (read_plan.geometry())
            at = read_plan.geometry()->residentAt(position_phys);
    }

    if (at.resident())
        rope = serveCacheBlock(position_phys, to_read);
    else
        rope = coverWindow(position_phys, to_read);

    stats.add(Stats::RequestedBytes, rope.range().size);
    position += rope.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        rope.range().size, rope.getNodes().size(), position);

    /// Unknown-size EOF is latched by a short read here, not the pre-read gate,
    /// and the caller stops on the empty rope without a follow-up call — so
    /// release the live connection now rather than leaking it. A consume above already
    /// moved the cluster back into `foreground_connection_state`.
    if (reached_eof)
    {
        accountLiveConnectionDrop(foreground_connection_state, /*at_eof=*/true, stats);
        foreground_connection_state.connection.reset();
        foreground_connection_state.inflight_segment_pin.reset();
    }

    /// The lease is held only while a live connection is open: a cache-only window, a
    /// stale-connection drop, or EOF leaves none here, so release it (a later wide gap
    /// read re-acquires). A gap read that kept its connection holds the lease for itself
    /// or the worker `maybeTriggerPrefetch` is about to hand it to. Runs BEFORE the
    /// prefetch launch, while the connection (if any) is still on the foreground.
    if (!foreground_connection_state.connection)
        connection_lease = {};

    maybeTriggerPrefetch();

    return rope;
}

Rope ReaderExecutor::serveCacheBlock(size_t position_phys, size_t to_read)
{
    /// Stream the contiguous resident run straight from the plan's held (pinning) cache
    /// readers - no per-window discovery, no source. Serve each tier's range from its own
    /// reader, advancing the cursor so the appended runs stay disjoint; stop at the first
    /// gap (the next call serves it). A machine for a downstream gap may be in flight here
    /// (the resident/prefetch overlap): this path touches ONLY the caches and the (empty,
    /// moved-to-the-machine) foreground connection cluster, never the worker's machine.
    Rope rope;

    /// Test hook: pause after the plan classifies this run as a hit but before the read, so
    /// a test can drop/evict the cache in that window and verify the plan-pinned segment
    /// survives. No-op in production.
    FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_cache_status);

    /// Serve a BLOCK at a time (not a full window): a cache hit has no remote open to
    /// amortise over a window, so block-sizing just bounds the in-flight Rope memory per
    /// call. The loop also stops at the resident run end / `plan_end`.
    const size_t window_end = position_phys
        + std::min(effectiveBlockSize(read_plan.geometry()->pressure_level), to_read);
    StatTimer get_scope(stats, Stats::CacheGetMicroseconds);
    for (size_t pos = position_phys; pos < window_end;)
    {
        auto run = read_plan.geometry()->residentAt(pos);
        /// Map the resident geometry entry to its foreground-private held view (1:1
        /// positional). A resident entry always has a view; guard defensively.
        if (!run.resident() || run.entry >= read_plan.bufs.size()
            || !read_plan.bufs[run.entry].view)
            break;
        const size_t serve_end = std::min(run.run_end, window_end);
        Rope chunk = readHitFromView(*read_plan.bufs[run.entry].view, ByteRange{pos, serve_end - pos});
        const size_t got = chunk.range().size;
        if (got == 0)
            break;
        stats.add(Stats::CacheGetRequests);
        const bool is_page = run.tier == CacheTier::PageCache;
        stats.add(is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache, got);
        /// Promote this run up into any faster tier that misses it (no-op when served from
        /// the fastest tier or nothing faster populates) - deferred to a put-only machine
        /// when pools are present; skipped outright under contention (optional work).
        schedulePromoteStep(run.tier, ByteRange{pos, got}, chunk, stats);
        rope.append(std::move(chunk));
        pos += got;
        if (pos < serve_end)
            break;
    }
    HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
        static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));

    /// Cache-only serve: settle the foreground's own live connection for the next read (keep
    /// it if the next read bridges, else drop it). When a downstream-gap machine is in flight
    /// the foreground cluster is EMPTY (moved into the machine), so this is a no-op on it -
    /// the worker's connection lives in `machine->conn` and is never touched here.
    maybeKeepLiveConnectionBefore(position_phys + rope.range().size, foreground_connection_state, reached_eof, stats);

    if (data_start_offset)
        rope.shift(-static_cast<ssize_t>(data_start_offset));
    LOG_TRACE(log, "serveCacheBlock: streamed resident [{}, {}) from cache",
        position_phys, position_phys + rope.range().size);
    return rope;
}

Rope ReaderExecutor::coverWindow(size_t position_phys, size_t to_read)
{
    /// A gap (or extent reached): the source-fetching path. Bound the read to one plan gap
    /// `[position, gapEnd)` so each call returns one pure run (the next resident run is
    /// served from cache on the following call). A remote gap reads a full (pressure-scaled,
    /// cached-level) window to amortise the source open, clamped to the extent and the gap.
    size_t gap_size = to_read;
    if (to_read > 0)
        gap_size = std::min(clampToExtent(effectiveWindowSize(read_plan.geometry()->pressure_level)),
            read_plan.geometry()->gapEnd(position_phys) - position_phys);
    const ByteRange physical_window{position_phys, gap_size};

    /// Ensure a (possibly empty) geometry snapshot exists for the read below: an
    /// extent-reached (`to_read == 0`) gap could still see a null snapshot.
    if (!read_plan.geometry())
        planResidencyWindow(position_phys);

    /// Collect an in-flight read-ahead for this gap if it has started; if it was still
    /// queued, `tryCollectMachine` revokes it and we read synchronously below.
    Rope rope;
    if (machine && tryCollectMachine(rope))
        return rope;
    return syncGapRead(physical_window);
}

bool ReaderExecutor::tryCollectMachine(Rope & rope)
{
    /// The worker may own the connection mid-read, so the revoke/release handoff
    /// must complete before any source touch.
    auto m = std::move(machine);

    if (runner->tryCancelQueued(*m))
    {
        /// Still queued: revoke and let the caller read synchronously. The worker
        /// never ran, so `m->conn` is the UNTOUCHED cluster handed over at launch -
        /// reclaim it so the sync read reuses the same open connection (cold R=1).
        /// Then stash the machine - the pool's no-op pickup attaches a
        /// `ThreadGroupSwitcher` before checking cancellation, so ~ReaderExecutor
        /// must join it before our state is freed (a throw on the unwind would
        /// otherwise drop it un-joined; see `cancelMachine`).
        LOG_TRACE(log, "coverWindow: prefetch was queued, cancelling and reading from position {}", position);
        stats.add(Stats::PrefetchCancelled);
        chassert(!foreground_connection_state.connection);
        foreground_connection_state = std::move(m->conn);
        abandoned_machines.push_back(std::move(m));
        return false;
    }

    /// Started/finished: collect the worker's raw PHYSICAL gap bytes. The takeover:
    /// ask a still-running step to wrap up at its next interrupt point instead of
    /// blocking for the whole window - the wait is bounded by one source block. An
    /// already-parked machine ignores the flag (nothing reads it any more). Then
    /// reclaim the connection cluster FIRST (so the backfill pins the in-flight
    /// segment on it and the next read continues the same open GET - cold R=1),
    /// then fold the machine-local source I/O into `this->stats`.
    LOG_TRACE(log, "coverWindow: waiting on prefetched [{}, {})", m->requested_range.offset, m->requested_range.end());
    StatTimer wait_scope(stats, Stats::PrefetchWaitMicroseconds);
    runner->requestInterrupt(*m);
    runner->waitReleased(*m);

    /// The fetch step failed: mandatory work, so the read fails. Account the
    /// advanced connection (dropped with the machine) and keep the machine's
    /// issued-I/O counters before rethrowing - the bytes crossed the wire.
    if (m->failure)
    {
        if (m->conn.connection && !m->conn.connection->isComplete(/*at_eof=*/false))
            stats.add(Stats::IncompleteConnections);
        stats += m->stats;
        std::rethrow_exception(m->failure);
    }

    const bool interrupted = m->state.load() == MachineState::Interrupted;
    chassert(!foreground_connection_state.connection);
    foreground_connection_state = std::move(m->conn);
    /// Reconcile the worker's one-way EOF latch - ONLY here (its bytes are kept); the
    /// cancel paths must not, or a wasted read-ahead's EOF strands us at false EOF.
    /// (An interrupt-short return never latches it - see `fetchGapsFromSource`.)
    reached_eof |= m->reached_eof;
    stats += m->stats;
    HistogramMetrics::ReaderExecutorPrefetchWaitLatency.observe(
        static_cast<HistogramMetrics::Value>(wait_scope.elapsedMicroseconds()));

    const ByteRange requested_phys{m->requested_range.offset + data_start_offset, m->requested_range.size};

    if (interrupted)
    {
        /// An interrupted step that produced nothing degrades to the revoke path:
        /// the connection is reclaimed (above), the caller reads synchronously.
        if (m->fetched.empty())
            return false;

        /// A prefix that cannot serve the cursor (extension-only bytes below the
        /// requested range, or a kept seek moved past it) is still BANKED in the
        /// caches - the fetch already paid for it - and then the caller reads
        /// synchronously: serving an empty window here would read as a false EOF
        /// upstream.
        const size_t fetched_logical_end = m->fetched.range().end() - data_start_offset;
        if (fetched_logical_end <= position)
        {
            Rope assembled;
            IntervalSet covered_unused;
            backfillBytes(m->physical_window, requested_phys, m->fetched, assembled, covered_unused,
                /*push_to_writers=*/false, stats);
            schedulePutStep(std::move(m), assembled);
            return false;
        }
        stats.add(Stats::PartialCollects);
    }
    else
        stats.add(Stats::PrefetchHits);

    /// Backfill the cache for the fetched window (the worker did none), pin the
    /// in-flight segment at the frontier the fetch actually reached (an interrupted
    /// step stops short of the aligned window end; a full fetch reaches it), slice
    /// back to the REQUESTED window and shift to logical. A partial rope is
    /// structurally an EOF-short window: the backfill clamps to delivered bytes and
    /// the contiguity contract holds for a prefix - the remainder is just the next
    /// gap, found by the normal dispatch (usually relaunched as the next machine on
    /// the same live connection). The slice is additionally clamped to the fetched
    /// prefix when interrupted: a late hit BEYOND the prefix would otherwise leave a
    /// disjoint island in `result` and trip the contiguity guard; those bytes stay
    /// cached and the next window serves them from the plan.
    const size_t pin_frontier = std::min(m->physical_window.end(), m->fetched.range().end());
    const ByteRange slice_window = interrupted
        ? ByteRange{requested_phys.offset,
            std::min(requested_phys.end(), m->fetched.range().end()) - requested_phys.offset}
        : requested_phys;
    Rope result;
    IntervalSet covered;
    backfillBytes(m->physical_window, requested_phys, m->fetched, result, covered,
        /*push_to_writers=*/false, stats);
    rope = finalizeAssembledWindow(slice_window, pin_frontier,
        result, foreground_connection_state, reached_eof);
    /// The deferred write side of this window: the put step takes the writers and
    /// the assembled rope to the background. After `finalizeAssembledWindow` - the
    /// pin was just taken from the plan's writers while they were still here.
    schedulePutStep(std::move(m), result);
    if (data_start_offset)
        rope.shift(-static_cast<ssize_t>(data_start_offset));

    /// A seek landed inside the fetched window: trim the prefix so `rope` starts at `position`.
    if (!rope.empty() && position > rope.range().offset)
    {
        const size_t end = rope.range().end();
        rope = rope.slice(ByteRange{position, end - position});
    }
    return true;
}

Rope ReaderExecutor::syncGapRead(ByteRange physical_window)
{
    LOG_TRACE(log, "coverWindow: synchronous gap read physical [{}, {})",
        physical_window.offset, physical_window.end());
    acquireLeaseIfWide();  /// keep this gap read's connection live iff the plan is wide
    StatTimer sync_scope(stats, Stats::SyncReadMicroseconds);
    Rope rope = readWindowLogical(physical_window, foreground_connection_state, *read_plan.geometry(), reached_eof, stats);
    HistogramMetrics::ReaderExecutorSyncReadLatency.observe(
        static_cast<HistogramMetrics::Value>(sync_scope.elapsedMicroseconds()));
    return rope;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_DEBUG(log, "seek to {}, current position={}", new_position, position);

    if (machine
        && new_position >= machine->requested_range.offset
        && new_position < machine->requested_range.end())
    {
        LOG_TRACE(log, "seek: target within prefetch [{}, {}), keeping prefetch",
            machine->requested_range.offset, machine->requested_range.end());
        position = new_position;
        return;
    }

    cancelMachine(/*cancelled=*/true);

    const size_t new_physical = new_position + data_start_offset;
    size_t new_obj_file_offset = 0;
    const StoredObject * new_obj = offset_map.findObjectAt(new_physical, &new_obj_file_offset);

    /// `cancelMachine` above reclaimed any revoked machine's connection cluster
    /// back into `foreground_connection_state` (or dropped a running one), so the keep/drop below
    /// operates on the real connection. The lease (`slot`) is object-agnostic, so a
    /// seek to a different object keeps it and just reopens the connection.

    /// Decide the live connection's fate across the seek. Keep it for a forward seek
    /// small enough to bridge within its right bound: the next `readFromSource`
    /// skips the seeked-over gap on the open GET instead of reopening (the same
    /// rule and `min_bytes_for_seek` bound used there). A backward seek, a
    /// different object, or a gap past that bound closes it. A cache-hit path
    /// skips `readFromSource`'s check, so this is also where a stale connection +
    /// slot would otherwise leak until EOF/destruction.
    if (foreground_connection_state.connection)
    {
        auto & lc = *foreground_connection_state.connection;
        const bool same_obj = new_obj && lc.object_path == new_obj->remote_path;
        const size_t new_local = same_obj ? new_physical - new_obj_file_offset : 0;
        const bool keep = same_obj
            && new_local >= lc.current_position
            && new_local - lc.current_position <= min_bytes_for_seek
            && (!lc.read_until || new_local <= *lc.read_until);
        if (!keep)
        {
            LOG_TRACE(log, "seek: live connection for {} (at {}) no longer matches target, closing",
                lc.object_path, lc.current_position);
            dropLiveConnection(foreground_connection_state, stats);
        }
    }

    /// With the stale connection (if any) closed, release the lease AND the in-flight
    /// segment pin unless a connection is kept (a kept connection still fills that
    /// segment, and the next window re-points the pin). A stateless read keeps no
    /// connection, so `dropLiveConnection` above did not run - drop its pin here so a
    /// seek away from the old frontier does not strand it. A later wide gap read
    /// re-acquires the lease; `maybeTriggerPrefetch` below may re-take it.
    if (!foreground_connection_state.connection)
    {
        foreground_connection_state.inflight_segment_pin.reset();
        connection_lease = {};
    }

    position = new_position;
    reached_eof = false;

    maybeTriggerPrefetch();
}

VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> ReaderExecutor::allocateBlocks(
    size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits)
{
    chassert(block_size > 0);
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks;
    blocks.reserve((size + block_size - 1) / block_size + splits.size());

    size_t pos = 0;
    auto split_it = splits.begin();
    while (pos < size)
    {
        while (split_it != splits.end() && *split_it <= pos)
            ++split_it;

        const size_t boundary = (split_it != splits.end()) ? std::min(*split_it, size) : size;
        const size_t chunk = std::min(block_size, boundary - pos);
        blocks.push_back(std::make_shared<OwnedRopeBuffer>(chunk));
        pos += chunk;
    }
    return blocks;
}

/// Zero-copy set()+next() path when the buffer supports it. Asynchronous
/// readers (`pread_threadpool`, io_uring) read into their own allocation
/// assuming `memory.size() == internal_buffer.size()`, so `set()` would
/// corrupt the heap when `chunk` exceeds the buffer's constructor-time size —
/// for those, fall back to `read()`.
///
/// Returned value is `0` only when the source signals EOF. Short positive
/// `next` returns are looped so a partial fill never reaches the caller as
/// `actual < pr.size`.
static size_t readIntoBlock(ReadBuffer & buf, char * dest, size_t chunk)
{
    if (buf.supportsExternalBufferMode())
    {
        size_t total = 0;
        while (total < chunk)
        {
            /// Re-arm at `dest + total`: the source's internal position has
            /// advanced by `total` already, so successive `next` calls land
            /// contiguously in `dest`.
            buf.set(dest + total, chunk - total);
            if (!buf.next())
                break;
            size_t got = buf.available();
            if (got == 0)
                break;  /// Defensive: source returned `true` with no data.
            buf.position() = buf.buffer().end();
            total += got;
        }
        return total;
    }

    return buf.read(dest, chunk);
}

Rope ReaderExecutor::Connection::readInto(
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset, const LoggerPtr & logger,
    const std::atomic<bool> * interrupt)
{
    Rope rope;
    size_t total_read = 0;

    for (auto & block : blocks)
    {
        /// The interrupt point: stop BETWEEN blocks on a set flag, returning the
        /// blocks read so far - bounding a waiting executor's release latency by
        /// one block instead of one window. The caller distinguishes this short
        /// return from EOF by re-checking the flag.
        if (interrupt && interrupt->load(std::memory_order_relaxed))
            break;

        size_t chunk = block->size();
        size_t got = readIntoBlock(*buffer, block->data(), chunk);

        LOG_DEBUG(logger, "Connection::readInto: block {}, chunk={}, got={}, first_byte=0x{:02x}",
            rope.getNodes().size(), chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
            break;

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    current_position += total_read;
    return rope;
}

size_t ReaderExecutor::Connection::skipForward(size_t gap, size_t block_bytes)
{
    /// Discard `gap` bytes from the open source read so the frontier advances over an
    /// already-cached gap. Uses a scratch block because the source is in
    /// external-buffer mode (mirrors `readIntoBlock`); the bytes are transferred and
    /// thrown away - only the source request is saved. Returns bytes actually skipped
    /// (< `gap` only if the source hit EOF).
    const size_t scratch_size = std::min(gap, block_bytes);
    auto scratch = std::make_shared<OwnedRopeBuffer>(scratch_size);

    size_t skipped = 0;
    while (skipped < gap)
    {
        const size_t chunk = std::min(gap - skipped, scratch_size);
        const size_t got = readIntoBlock(*buffer, scratch->data(), chunk);
        if (got == 0)
            break;
        skipped += got;
    }
    current_position += skipped;
    return skipped;
}

size_t ReaderExecutor::Connection::drainTail(size_t max_tail, size_t block_bytes)
{
    if (!read_until || current_position >= *read_until)
        return 0;
    const size_t tail = *read_until - current_position;
    if (tail > max_tail)
        return 0;
    return skipForward(tail, block_bytes);
}

Rope ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset,
    bool keep_live, ConnState & conn, const std::atomic<bool> * interrupt, Stats & out_stats)
{
    size_t want = 0;
    for (const auto & block : blocks)
        want += block->size();

    /// Reuse the live connection for a contiguous read, or bridge a small
    /// forward cached gap by discarding it on the open source read so the
    /// connection stays reusable instead of reopening - the same over-read vs
    /// separate-read trade `mergeRanges` makes, so it shares `min_bytes_for_seek`
    /// as the gap bound (0 for local sources, which never bridge). A read that
    /// would pass the right bound still reopens (the bounded connection is
    /// already drained at that point and reusable).
    if (conn.connection
        && conn.connection->object_path == object.remote_path
        && offset >= conn.connection->current_position
        && offset - conn.connection->current_position <= min_bytes_for_seek
        && (!conn.connection->read_until || offset + want <= *conn.connection->read_until))
    {
        const size_t gap = offset - conn.connection->current_position;
        bool ready = gap == 0;
        if (gap > 0)
        {
            /// Skip the already-cached gap on the live connection. The bytes
            /// cross the wire (charged as over-read); only the source request
            /// is saved. A short skip means the source hit EOF inside the gap
            /// (unknown size) - the connection is spent, fall through to reopen.
            const size_t skipped = conn.connection->skipForward(gap, block_size);
            out_stats.add(Stats::BytesFromSource, skipped);
            out_stats.add(Stats::OverReadBytes, skipped);
            ready = skipped == gap;  // skipForward advanced the frontier to `offset`
        }

        if (ready)
        {
            LOG_TRACE(log, "readFromSource: live connection hit for {}, position={}", object.remote_path, offset);
            ProfileEvents::increment(ProfileEvents::LiveSourceBufferHits);

            Rope rope = conn.connection->readInto(std::move(blocks), logical_offset, log, interrupt);
            ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, rope.totalBytes());
            releaseLiveConnectionAtBound(conn);
            return rope;
        }
    }

    if (conn.connection)
    {
        LOG_TRACE(log, "readFromSource: closing live connection for {} (was at {}), need {}:{}",
            conn.connection->object_path, conn.connection->current_position, object.remote_path, offset);
        dropLiveConnection(conn, out_stats);
    }

    /// `keep_live` is decided by the caller (the per-plan `connection_lease`, threaded as
    /// `bool(connection_lease)` from the foreground or `job->leased` from a worker): a
    /// wide plan opens a connection kept live across windows; a narrow plan opens a
    /// bounded one-shot range read below. readFromSource never takes/releases the lease.
    if (keep_live)
    {
        auto opened = source->open(object);
        if (opened)
        {
            if (offset > 0)
                opened->seek(offset, SEEK_SET);

            /// Bound the connection so it is read to a known end and returned to
            /// the pool reusable rather than abandoned open-ended. A transient
            /// (`readBigAt`) reads one block, which may over-read past its
            /// requested extent to fill a cache block - bound it to the bytes this
            /// call reads. A sequential reader with an advertised extent streams
            /// within `[.., extent)` across windows and drains at the extent -
            /// bound it there, but never short of this call's read so a cache-block
            /// over-read past the extent still completes. `offset`/blocks are
            /// physical (map-space) offsets.
            std::optional<size_t> read_until;
            if (opened->supportsRightBoundedReads())
            {
                if (is_transient)
                {
                    /// A `readBigAt` transient only runs on known-size sources.
                    if (!hasUnknownSize())
                        read_until = offset + want;
                }
                else if (read_extent_end)
                {
                    /// The advertised extent is a concrete position even when the
                    /// total size is unknown, so bound to it regardless - otherwise
                    /// the live connection (and its slot) would stay open-ended and
                    /// pinned after the consumer stops at the extent. Only the
                    /// object-end clamp needs a known size; an unknown-size object
                    /// has no end to clamp against, the extent is the only bound.
                    const size_t physical_extent_end = *read_extent_end + data_start_offset;
                    const size_t to_extent = physical_extent_end > logical_offset ? physical_extent_end - logical_offset : 0;
                    size_t bound_size = to_extent;
                    if (!hasUnknownSize())
                    {
                        const size_t to_object_end = object.bytes_size > offset ? object.bytes_size - offset : 0;
                        bound_size = std::min(to_extent, to_object_end);
                    }
                    read_until = offset + std::max(want, bound_size);
                }
                if (read_until)
                    opened->setReadUntilPosition(*read_until);
            }

            conn.connection.emplace(Connection{
                .current_position = offset,
                .read_until = read_until,
                .buffer = std::move(opened),
                .object_path = object.remote_path,
            });
            out_stats.add(Stats::SourceRequests);

            Rope rope = conn.connection->readInto(std::move(blocks), logical_offset, log, interrupt);

            ProfileEvents::increment(ProfileEvents::LiveSourceBufferCreated);
            ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, rope.totalBytes());
            LOG_TRACE(log, "readFromSource: opened live connection for {}, read {} bytes, position={}",
                object.remote_path, rope.totalBytes(), conn.connection->current_position);
            releaseLiveConnectionAtBound(conn);
            return rope;
        }
    }

    /// No slot available — open a one-shot connection without storing it as
    /// `connection`. Dropped when this function returns.
    ProfileEvents::increment(ProfileEvents::LiveSourceBufferFallbacks);

    auto opened = source->open(object);
    if (offset > 0)
        opened->seek(offset, SEEK_SET);

    /// No slot kept: bound the one-shot read so its connection is fully consumed
    /// and reusable by the pool, rather than abandoning an open-ended GET. The read
    /// consumes exactly `want` bytes, so bound to `offset + want` whenever the end is
    /// concrete — a known object size, or a finite advertised extent
    /// (`read_extent_end`) even when the size is unknown. Only a truly unbounded
    /// source (unknown size AND no advertised extent) is left open-ended.
    const bool stateless_bounded = opened->supportsRightBoundedReads() && want > 0
        && (!hasUnknownSize() || read_extent_end.has_value());
    if (stateless_bounded)
        opened->setReadUntilPosition(offset + want);

    auto & buf = *opened;
    out_stats.add(Stats::SourceRequests);

    Rope rope;
    size_t total_read = 0;
    bool hit_eof = false;

    for (auto & block : blocks)
    {
        /// The interrupt point (see `Connection::readInto`): stop between blocks.
        if (interrupt && interrupt->load(std::memory_order_relaxed))
            break;

        size_t chunk = block->size();
        size_t got = readIntoBlock(buf, block->data(), chunk);

        LOG_DEBUG(log, "readFromSource: stateless block offset={}, chunk={}, got={}, first_byte=0x{:02x}",
            offset + total_read, chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
        {
            hit_eof = true;
            break;
        }

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    /// A one-shot GET dropped before it was fully consumed is not reusable: an
    /// unbounded one (unknown size AND no advertised extent) that did not reach
    /// EOF, or a bounded one shortened by an interrupt (`total_read < want`).
    if (!hit_eof && (!stateless_bounded || total_read < want))
        out_stats.add(Stats::IncompleteConnections);

    return rope;
}

Rope ReaderExecutor::readPhysicalWindow(ByteRange physical_window, ConnState & conn,
    const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats)
{
    LOG_TRACE(log, "readPhysicalWindow [{}, {})", physical_window.offset, physical_window.end());

    /// Foreground SYNCHRONOUS assembler: `initDecryption` (header) and the two sync gap
    /// reads in `readNextWindow`. `fetchAndBackfillGaps` re-credits grown committed
    /// prefixes, serves late hits, reads the still-missing ranges from the source, and
    /// pushes them into the plan's held write buffers. A prefetch worker never comes
    /// here: it runs the narrow `fetchGapsFromSource` over the plan gap the foreground
    /// bounded at submit, and the foreground backfills its bytes at consume.
    Rope result;
    /// Physical bytes already materialised in `result`. Keeps `result` disjoint:
    /// resident and source bytes only fill what is not yet covered. The cache writes
    /// happen AFTER assembly, into the plan's held write buffers - so a short/zero
    /// landing never holes `result` (`[CF-contiguity]`).
    IntervalSet covered;

    /// Widen the FETCH to the cache-aligned miss extent (the segment/block-aligned head
    /// below `physical_window.offset` and the tail past its end), mirroring the prefetch
    /// path's `fetchWindowAt` at submit: the source over-reads to fill the whole
    /// cache segment/block so the write buffers commit complete cells, and the alignment
    /// slack is counted as `OverReadBytes`. With an empty geometry (`initDecryption`) this
    /// is a no-op (`fetch_window == physical_window`). The result is sliced back to the
    /// REQUESTED `physical_window` by `finalizeAssembledWindow`, so the caller still gets
    /// only the requested bytes; the pin uses the aligned frontier.
    const ByteRange fetch_window = geometry.fetchWindowAt(physical_window);

    /// A still-uncommitted deferred fill overlapping this window must land first:
    /// the aligned head below can reach back into its cell (fetching those bytes
    /// again from the source would silently inflate R/O), and the sync push at
    /// the end needs its writers home.
    joinPutMachinesOverlapping(fetch_window, /*writers_too=*/true);

    /// With pools present the sync path defers its cache fill exactly like a
    /// machine collect: assemble only, then hand the writers + rope to a
    /// put-only machine below. Transients and pool-less executors keep the
    /// synchronous push.
    const bool defer_fill = runner != nullptr;

    /// Serve resident bytes over the ALIGNED window: a byte that is a miss on the tier
    /// driving the alignment but resident on a faster tier is covered here, so the gap
    /// read below never re-fetches it.
    serveResidentFromPlan(fetch_window, result, covered, geometry, out_stats);
    const bool fetched_from_source = fetchAndBackfillGaps(
        fetch_window, physical_window, result, covered, conn, eof_latch, geometry.pressure_level,
        /*push_to_writers=*/!defer_fill, out_stats);

    /// A cache-only window (no source read) leaves the live connection idle; keep
    /// it only if the next window bridges, else drop it (see the helper). The logical
    /// continuation point is the requested window end, not the aligned end.
    if (!fetched_from_source)
        maybeKeepLiveConnectionBefore(physical_window.end(), conn, eof_latch, out_stats);

    auto sliced = finalizeAssembledWindow(physical_window, fetch_window.end(),
        result, conn, eof_latch);

    /// The deferred write side (after finalize - the pin was just taken from the
    /// plan's writers while they were still home). A put-only machine: no fetch
    /// step ever ran, it exists only to borrow the writers and run the put.
    if (defer_fill && fetched_from_source)
    {
        auto pm = std::make_shared<FetchMachine>();
        pm->requested_range = physical_window;
        pm->physical_window = fetch_window;
        schedulePutStep(std::move(pm), result);
    }

    return sliced;
}

Rope ReaderExecutor::finalizeAssembledWindow(ByteRange slice_window, size_t pin_frontier, Rope & result,
    ConnState & conn, bool eof_latch) const
{
    /// Strategy A pin: re-point to the partial segment under `pin_frontier` - the frontier
    /// the read actually reached, which (with page-block alignment) can sit past
    /// `slice_window.end()`. This protects a still-being-filled cache segment from eviction
    /// and is independent of whether a live SOURCE connection is kept: a stateless one-shot
    /// gap read in a sequential scan backfills a partial segment too, and the next window
    /// needs it intact. A `readBigAt` transient is excluded - it reads its bounded extent
    /// once and is destroyed, so pinning the partial segment it leaves serves nothing.
    /// `writerPinAt` returns the first held write buffer's `pin` (a bare FileSegmentPtr
    /// the buffer already owns) that passes the 3-part guard, empty otherwise; clear the
    /// pin at EOF.
    if (!eof_latch && !is_transient)
    {
        conn.inflight_segment_pin = writerPinAt(pin_frontier);

        /// Test hook: pause here while the in-flight segment is pinned, so a test can
        /// drop/evict the cache and observe that the pinned segment survives. No-op
        /// unless enabled.
        if (conn.inflight_segment_pin)
            FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_window);
    }
    else
    {
        conn.inflight_segment_pin.reset();
    }

    auto sliced = result.slice(slice_window);

    /// Enforce the single-contiguous-run-from-the-window-start guarantee (may
    /// end early at EOF). A hole would misalign the caller's offsets.
    const auto & ivs = sliced.getIntervals();
    if (ivs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReaderExecutor: assembled result has {} disjoint intervals in window [{}, {}) - expected at most one contiguous run",
            ivs.size(), slice_window.offset, slice_window.end());
    if (!ivs.empty() && ivs[0].offset != slice_window.offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReaderExecutor: assembled result starts at {} but window begins at {} - missing prefix bytes",
            ivs[0].offset, slice_window.offset);
    return sliced;
}

/// Serve a clamped resident sub-range from a held `planResidencyView` view's hit read
/// buffers: find each `HitEntry` overlapping `clamped`, read the overlap from its
/// re-readable buffer (clamped to `readable()` so a partial prefix is never over-read),
/// and append the pieces. Returns the assembled (possibly short) Rope; the caller checks
/// `covers`. Records each `read` on the view for the deferred LRU bump.
Rope ReaderExecutor::readHitFromView(CacheView & view, ByteRange clamped)
{
    Rope out;
    for (const auto & hit : view.hits())
    {
        if (!hit.reader)
            continue;
        const size_t readable = hit.reader->readable();
        const size_t lo = std::max(hit.range.offset, clamped.offset);
        const size_t hi = std::min({hit.range.end(), clamped.end(), readable});
        if (lo >= hi)
            continue;
        out.append(hit.reader->read(ByteRange{lo, hi - lo}));
    }
    return out;
}

void ReaderExecutor::serveResidentFromPlan(
    ByteRange physical_window, Rope & result, IntervalSet & covered,
    const ReadPlanGeometry & geometry, Stats & out_stats)
{
    /// Foreground-only (reads `this->read_plan.bufs`). Does NOT re-plan: the foreground
    /// re-plans before calling (in readNextWindow). An uncovered window simply yields no
    /// resident bytes here - `recreditCommittedPrefixes` / `serveLateHits` / the source
    /// backfill handle the rest.

    /// Test hook: pause after residency is planned - the held read buffers pin the
    /// resident segments - but before they are read, so a test can drop/evict the cache
    /// and verify the pinned segments survive and a read still honors them. Gated on
    /// there being resident geometry (entries can also be miss-only write targets, which
    /// pin nothing). No-op in production.
    bool any_resident = false;
    for (const auto & entry : geometry.entries)
        if (!entry.resident.empty())
        {
            any_resident = true;
            break;
        }
    if (any_resident)
        FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_cache_status);

    /// Geometry is in cache-tier priority order, so the `covered` guard serves each
    /// byte from the fastest tier that holds it. Each entry's held read buffers live in
    /// the 1:1-positional foreground-private `read_plan.bufs[i].view`.
    for (size_t i = 0; i < geometry.entries.size(); ++i)
    {
        const auto & entry = geometry.entries[i];
        const bool is_page = entry.tier == CacheTier::PageCache;
        const Stats::Counter tier_counter = is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache;

        for (const auto & res : entry.resident)
        {
            const size_t lo = std::max(res.offset, physical_window.offset);
            const size_t hi = std::min(res.end(), physical_window.end());
            if (lo >= hi)
                continue;
            ByteRange clamped{lo, hi - lo};

            auto useful = covered.subtract(clamped);
            if (useful.empty())
                continue;

            /// The entry's held view lives in the 1:1-positional `read_plan.bufs`. If it
            /// is missing, fail safe: leave the bytes for `serveLateHits` / the source.
            if (i >= read_plan.bufs.size() || !read_plan.bufs[i].view)
                continue;

            out_stats.add(Stats::CacheGetRequests);
            StatTimer get_scope(out_stats, Stats::CacheGetMicroseconds);
            Rope resident_rope = readHitFromView(*read_plan.bufs[i].view, clamped);
            HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
            for (const auto & sub : useful)
            {
                /// The held read buffer pins resident segments, so a byte the plan
                /// reported resident MUST still be readable here. If not, the pin was
                /// not honored - fail loudly rather than drop bytes.
                if (!resident_rope.covers(sub))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ReaderExecutor: residency plan promised a hit at [{}, {}) but read() did not "
                        "return it - a pinned cache segment was not honored",
                        sub.offset, sub.end());
                result.append(resident_rope.extract(sub));
                covered.add(sub);
                out_stats.add(tier_counter, sub.size);
            }
        }
    }
}

void ReaderExecutor::recreditCommittedPrefixes(
    ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats)
{
    /// Before the source fetch, re-credit any committed prefix of a frozen miss that a
    /// concurrent reader (or this plan's own write) has grown since plan-build: serve it
    /// from the held write buffer's own `read` so only the truly-uncommitted tail drives
    /// the fetch + `setReadUntilPosition`. Disk: a grown PARTIALLY_DOWNLOADED prefix.
    /// Page: a self-populated complete block re-touched within the plan span
    /// (`[CF-partial-prefix]` / `[CF-reusable]`). Held write buffers are in tier-priority
    /// order, so the `covered` guard serves each byte from the fastest tier under the
    /// SAME shared `covered`.
    for (const auto & buf : read_plan.bufs)
    {
        if (!buf.provider)
            continue;
        const bool is_page = buf.provider->tier() == CacheTier::PageCache;
        const Stats::Counter tier_counter = is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache;
        for (const auto & w : buf.writers)
        {
            if (!w.writer)
                continue;
            /// The committed prefix this buffer can serve from its own held segment/cells,
            /// clamped to the window. Derive the committed sub-ranges of `w_clamped` as
            /// `w_clamped` minus the uncommitted gaps (`committed().subtract`), since
            /// `IntervalSet` exposes only `add`/`subtract`.
            const size_t w_lo = std::max(w.writer->range().offset, window.offset);
            const size_t w_hi = std::min(w.writer->range().end(), window.end());
            if (w_lo >= w_hi)
                continue;
            const ByteRange w_clamped{w_lo, w_hi - w_lo};
            IntervalSet uncommitted;
            for (const auto & gap : w.writer->committed().subtract(w_clamped))
                uncommitted.add(gap);
            for (const auto & committed_part : uncommitted.subtract(w_clamped))
            {
                auto useful = covered.subtract(committed_part);
                if (useful.empty())
                    continue;
                out_stats.add(Stats::CacheGetRequests);
                StatTimer get_scope(out_stats, Stats::CacheGetMicroseconds);
                for (const auto & sub : useful)
                {
                    Rope chunk = w.writer->read(sub);
                    if (!chunk.covers(sub))
                        continue;  /// raced shrink/detach - fall back to the source path
                    result.append(chunk.extract(sub));
                    covered.add(sub);
                    out_stats.add(tier_counter, sub.size);
                }
                HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                    static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
            }
        }
    }
}

void ReaderExecutor::serveLateHits(ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats)
{
    /// Late hits: a sibling reader / promotion populated a gap between plan-build and
    /// consume. Mirror the deleted `serveCacheTiersCollectingMisses` - all tiers, in
    /// priority order, under ONE shared `covered` - but READ-ONLY (`planResidencyView`,
    /// never a mutating `lookup`), and keep each view's deferred LRU-bump alive past the
    /// held write buffers' writes by moving it into `read_plan.deferred_lru_bumps`
    /// (`[CF-lru]`). Its writers are ignored: we already have, or are about to fetch, the
    /// source bytes.
    VectorWithMemoryTracking<ByteRange> remaining = covered.subtract(window);
    for (auto & cache : caches)
    {
        if (remaining.empty())
            break;
        VectorWithMemoryTracking<ByteRange> still_missing;
        const bool is_page = cache->tier() == CacheTier::PageCache;
        const Stats::Counter tier_counter = is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache;

        for (const auto & r : remaining)
        {
            /// Split by object boundaries so each probe carries a single `StoredObject`
            /// (the provider keys/translates per object); views report file-level ranges.
            auto pieces = offset_map.map(r);
            size_t piece_file_start = r.offset;
            for (const auto & pr : pieces)
            {
                const size_t object_file_offset = piece_file_start - pr.object_offset;
                ByteRange piece_range{piece_file_start, pr.size};

                auto view = cache->planResidencyView(pr.object, object_file_offset, piece_range);

                if (!view->hits().empty())
                    FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_cache_status);

                for (const auto & hit : view->hits())
                {
                    if (!hit.reader)
                        continue;
                    const size_t readable = hit.reader->readable();
                    const size_t lo = std::max(hit.range.offset, piece_range.offset);
                    const size_t hi = std::min({hit.range.end(), piece_range.end(), readable});
                    if (lo >= hi)
                        continue;
                    auto useful = covered.subtract(ByteRange{lo, hi - lo});
                    if (useful.empty())
                        continue;
                    out_stats.add(Stats::CacheGetRequests);
                    StatTimer get_scope(out_stats, Stats::CacheGetMicroseconds);
                    for (const auto & sub : useful)
                    {
                        Rope hit_rope = hit.reader->read(sub);
                        if (!hit_rope.covers(sub))
                            throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "ReaderExecutor: cache {} planResidencyView reported a late hit at "
                                "[{}, {}) but read() did not return it - a held FileSegment was not honored",
                                cache->name(), sub.offset, sub.end());
                        result.append(hit_rope.extract(sub));
                        covered.add(sub);
                        out_stats.add(tier_counter, sub.size);
                    }
                    HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                        static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
                }

                /// Whatever this tier still misses propagates down to the next tier.
                for (const auto & sub : covered.subtract(piece_range))
                    still_missing.push_back(sub);

                /// Keep the view alive for the plan's life so its deferred LRU-bump lands
                /// AFTER the held write buffers' writes (the bump fires in `~CacheView`).
                read_plan.deferred_lru_bumps.push_back(std::move(view));

                piece_file_start += pr.size;
            }
        }

        remaining = std::move(still_missing);
    }
}

bool ReaderExecutor::fetchAndBackfillGaps(
    ByteRange fetch_window,
    ByteRange requested_window,
    Rope & result,
    IntervalSet & covered,
    ConnState & conn,
    bool & eof_latch,
    MemoryPressureLevel pressure_level,
    bool push_to_writers,
    Stats & out_stats)
{
    /// Synchronous foreground gap path: serve any grown committed prefix and late cache hit
    /// FIRST (so a concurrently/self-cached gap is served from cache, not re-fetched), then
    /// read the still-missing gaps of the ALIGNED `fetch_window` from the source - merged
    /// into fewer requests by `min_bytes_for_seek` - into one `source_bytes` Rope, and hand
    /// it to the shared `assembleAndWriteBack` (append + over-read + cache fill).
    recreditCommittedPrefixes(fetch_window, result, covered, out_stats);
    serveLateHits(fetch_window, result, covered, out_stats);
    VectorWithMemoryTracking<ByteRange> remaining = covered.subtract(fetch_window);

    /// Merge close-together gaps into fewer source requests. A merge may bridge already-
    /// covered bytes; `assembleAndWriteBack` appends only the still-uncovered sub-ranges.
    auto fetch_ranges = mergeRanges(remaining, min_bytes_for_seek);
    if (fetch_ranges.size() < remaining.size())
        LOG_TRACE(log, "fetchAndBackfillGaps: merged {} gaps into {} fetch ranges (min_gap={})",
            remaining.size(), fetch_ranges.size(), min_bytes_for_seek);

    /// Block size for the source-read tiles, from the per-plan cached pressure level.
    const size_t window_block_size = effectiveBlockSize(pressure_level);

    Rope source_bytes;
    for (const auto & fr : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(fr);
        size_t logical_pos = fr.offset;
        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "fetchAndBackfillGaps: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);

            /// Split at the REQUESTED window edges so user-data bytes and segment-aligned
            /// head/tail-extension bytes land in separate `OwnedRopeBuffer`s (released
            /// independently).
            VectorWithMemoryTracking<size_t> splits;
            const size_t pr_lo = logical_pos;
            const size_t pr_hi = logical_pos + pr.size;
            if (requested_window.offset > pr_lo && requested_window.offset < pr_hi)
                splits.push_back(requested_window.offset - pr_lo);
            if (requested_window.end() > pr_lo && requested_window.end() < pr_hi)
                splits.push_back(requested_window.end() - pr_lo);
            std::sort(splits.begin(), splits.end());

            auto blocks = allocateBlocks(pr.size, window_block_size, splits);
            StatTimer src_scope(out_stats, Stats::SourceReadMicroseconds);
            /// Keep the connection live iff the current plan holds the lease (a wide plan);
            /// a narrow tail plan reads a one-shot.
            Rope sr = readFromSource(pr.object, pr.object_offset, std::move(blocks), logical_pos,
                /*keep_live=*/static_cast<bool>(connection_lease), conn, /*interrupt=*/nullptr, out_stats);
            HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
                static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
            const size_t actual = sr.totalBytes();
            out_stats.add(Stats::BytesFromSource, actual);
            /// Size-known short reads are fatal (the map promised those bytes).
            /// Size-unknown short reads are how EOF is learned - latch it.
            if (actual != pr.size)
            {
                if (!offset_map.hasUnknownSize())
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                        pr.object.remote_path, pr.object_offset, pr.size, actual);
                eof_latch = true;
            }
            source_bytes.append(std::move(sr));
            logical_pos += pr.size;
        }
    }

    assembleAndWriteBack(fetch_window, requested_window, source_bytes, result, covered, push_to_writers, out_stats);
    return !fetch_ranges.empty();
}

Rope ReaderExecutor::fetchGapsFromSource(ByteRange physical_window, bool from_prefetch, bool keep_live, ConnState & conn,
    bool & eof_latch, MemoryPressureLevel pressure_level, const std::atomic<bool> * interrupt, Stats & out_stats)
{
    /// PURE source fetch: read the WHOLE window from the source as one contiguous
    /// physical run (short at EOF or at an interrupt point). No cache
    /// `lookup`/`get`/`put`, no plan - this is all a machine fetch step runs (it
    /// cannot touch shared cache/plan state), and the foreground reuses it before
    /// its own `backfillBytes`. The window is already clamped to one plan gap by
    /// the caller, so it never straddles a resident run; the cache backfill of
    /// these bytes is `backfillBytes`'s job.
    Rope result;
    if (physical_window.size == 0)
        return result;

    /// Block size for the source-read tiles, from the per-plan cached pressure level
    /// (a worker passes `job->pressure_level`, the foreground `read_plan.geometry()`'s).
    const size_t window_block_size = effectiveBlockSize(pressure_level);

    auto physical_ranges = offset_map.map(physical_window);
    size_t file_pos = physical_window.offset;
    for (const auto & pr : physical_ranges)
    {
        LOG_TRACE(log, "fetchGapsFromSource: source read object={}, offset={}, size={}",
            pr.object.remote_path, pr.object_offset, pr.size);

        /// No head/tail-extension splits: the window IS the fetch range (the cache
        /// `getOrSet` that would segment-align a miss runs later, in `backfillBytes`).
        auto blocks = allocateBlocks(pr.size, window_block_size, {});
        StatTimer src_scope(out_stats, Stats::SourceReadMicroseconds);
        Rope source_rope = readFromSource(pr.object, pr.object_offset, std::move(blocks), file_pos,
            keep_live, conn, interrupt, out_stats);
        HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
            static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
        const size_t actual = source_rope.totalBytes();
        out_stats.add(Stats::BytesFromSource, actual);
        if (from_prefetch)
            out_stats.add(Stats::PrefetchIssuedSourceBytes, actual);
        result.append(std::move(source_rope));
        file_pos += pr.size;

        /// An interrupt-short return is checked FIRST: it must neither latch EOF
        /// (the bytes exist - the remainder is read by the normal dispatch) nor
        /// throw the size-known short-read error. The partial result is the
        /// machine's wrap-up product.
        if (interrupt && interrupt->load(std::memory_order_relaxed))
            break;

        /// Size-known short reads are fatal (the map promised those bytes). Size-unknown
        /// short reads are how EOF is learned - latch it and stop (no later piece exists).
        if (actual != pr.size)
        {
            if (!offset_map.hasUnknownSize())
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                    "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                    pr.object.remote_path, pr.object_offset, pr.size, actual);
            eof_latch = true;
            break;
        }
    }
    return result;
}

void ReaderExecutor::backfillBytes(
    ByteRange physical_window, ByteRange requested_window, const Rope & source_bytes,
    Rope & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats)
{
    /// The prefetch-CONSUME gap path: the worker already fetched the whole aligned
    /// `physical_window` into `source_bytes` (it does no cache work). Serve any grown
    /// committed prefix and late cache hit first (BEFORE the worker's bytes, so a
    /// concurrently-cached gap is served from cache, not the redundant source copy), then
    /// hand the bytes to the shared `assembleAndWriteBack` - the same tail the sync path
    /// uses, so over-read counts identically.
    recreditCommittedPrefixes(physical_window, result, covered, out_stats);
    serveLateHits(physical_window, result, covered, out_stats);
    assembleAndWriteBack(physical_window, requested_window, source_bytes, result, covered, push_to_writers, out_stats);
}

void ReaderExecutor::assembleAndWriteBack(
    ByteRange fetch_window, ByteRange requested_window,
    const Rope & source_bytes, Rope & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats)
{
    /// Append the source bytes for the still-uncovered gaps of `fetch_window`, in offset
    /// order (assembly truth is the SOURCE Rope, `[CF-contiguity]`). CLAMP every append to
    /// what `source_bytes` ACTUALLY delivered: a size-unknown EOF read returns fewer bytes
    /// than the window, and a cold-segment miss head can sit BEFORE the window - those head
    /// bytes were never fetched, so they stay a hole here and the held write buffer's
    /// append-at-`cwo` skips them.
    const ByteRange delivered = source_bytes.range();
    size_t served_requested = 0;
    for (const auto & miss : covered.subtract(fetch_window))
    {
        const size_t lo = std::max(miss.offset, delivered.offset);
        const size_t hi = std::min(miss.end(), delivered.end());
        if (lo >= hi)
            continue;
        for (const auto & sub : covered.subtract(ByteRange{lo, hi - lo}))
        {
            result.append(source_bytes.slice(sub));
            covered.add(sub);
            const size_t rlo = std::max(sub.offset, requested_window.offset);
            const size_t rhi = std::min(sub.end(), requested_window.end());
            if (rhi > rlo)
                served_requested += rhi - rlo;
        }
    }

    /// Over-read - the single rule for both gap paths: source bytes that did NOT serve the
    /// REQUESTED window. That is the alignment slack fetched only to fill a cache cell,
    /// redundant copies of late-hit ranges another reader cached since planning, and any
    /// bridged sub-`min_bytes_for_seek` hole (`[CF-overread]`).
    out_stats.add(Stats::OverReadBytes, source_bytes.totalBytes() - served_requested);

    if (push_to_writers)
        pushAssembledToWriteBuffers(fetch_window, result, out_stats);
}

void ReaderExecutor::pushRopeToWriters(VectorWithMemoryTracking<MissEntry> & writers, ByteRange window,
    const Rope & rope, Stats::Counter bytes_counter, const std::atomic<bool> * interrupt, Stats & out_stats)
{
    for (auto & w : writers)
    {
        /// The put step's interrupt point: stop between writers, leaving the
        /// remaining ones untouched for the caller's abandon path.
        if (interrupt && interrupt->load(std::memory_order_relaxed))
            break;

        chassert(w.writer);
        /// Clamp the write target to the window's served portion and the buffer's own
        /// aligned range; the buffer further skips already-committed bytes internally
        /// (committed-set idempotency), so an out-of-order/overlapping slice from an
        /// interleaved promotion never double-counts.
        const size_t lo = std::max(w.writer->range().offset, window.offset);
        const size_t hi = std::min(w.writer->range().end(), window.end());
        if (lo >= hi)
            continue;
        auto slice = rope.slice(ByteRange{lo, hi - lo});
        if (slice.empty())
            continue;
        out_stats.add(Stats::CachePopulateRequests);
        StatTimer put_scope(out_stats, Stats::CachePopulateMicroseconds);
        out_stats.add(bytes_counter, w.writer->write(std::move(slice)));
        HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
            static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
    }
}

void ReaderExecutor::pushAssembledToWriteBuffers(ByteRange physical_window, const Rope & result, Stats & out_stats)
{
    /// Push the assembled `result`'s miss bytes into the plan's held write buffers,
    /// fire-and-forget: `result` is already assembled from the source Rope + hit readers,
    /// so a short/zero `write` landing affects only `BytesPushedToCacheSync`, never
    /// `result` (`[CF-contiguity]`). Writes only into the authoritative `BufEntry::writers`
    /// (`chassert(writer)`), never the view's null-writer misses (`[CF-mutate]`). `result`
    /// is disjoint, so each slice has at most one node per byte (it may be short at EOF).
    /// This is the SYNCHRONOUS write side (the no-pool/sync paths); a machine collect
    /// defers the same work to a put step (`schedulePutStep`).
    for (auto & buf : read_plan.bufs)
        pushRopeToWriters(buf.writers, physical_window, result, Stats::BytesPushedToCacheSync, /*interrupt=*/nullptr, out_stats);
}

void ReaderExecutor::schedulePutStep(std::shared_ptr<FetchMachine> m, const Rope & assembled)
{
    /// Over the cap: the NEW fill is skipped - droppable by the invariant
    /// (mandatory work never queues behind it). The writers stay home, the
    /// segments stay partial; a later window may still fill them.
    if (put_machines.size() >= MAX_PUT_MACHINES)
    {
        stats.add(Stats::PutAbandoned);
        return;
    }

    /// An earlier put may still be borrowing writers that span this window too
    /// (one fs segment / page run covers many windows) - join it so each writer
    /// has exactly one owner.
    joinPutMachinesOverlapping(m->physical_window, /*writers_too=*/true);

    /// BORROW this window's writers from the plan into the machine: the put step
    /// owns them exclusively while it writes; the reap returns them home
    /// (`writer_origins`) so the next window's fill - and the plan teardown's
    /// finalize - find them where they have always lived. Runs AFTER
    /// `finalizeAssembledWindow`, so the in-flight pin was taken first.
    for (size_t i = 0; i < read_plan.bufs.size(); ++i)
    {
        auto & buf = read_plan.bufs[i];
        auto kept = std::stable_partition(buf.writers.begin(), buf.writers.end(),
            [&](const MissEntry & w)
            {
                return !(w.writer && w.range.offset < m->physical_window.end()
                         && m->physical_window.offset < w.range.end());
            });
        for (auto it = kept; it != buf.writers.end(); ++it)
        {
            m->writers.push_back(std::move(*it));
            m->writer_origins.push_back(i);
        }
        buf.writers.erase(kept, buf.writers.end());
    }
    if (m->writers.empty())
        return;  /// nothing to fill for this window

    m->fill_rope = assembled;
    /// The machine is being re-armed for a second step: a takeover collect set
    /// `interrupt_requested` to stop the FETCH - the put must not inherit it.
    m->interrupt_requested.store(false);
    m->current_step.reset();
    m->put_wait.restart();
    m->run_step = [this, self = m.get()]
    {
        self->stats.add(Stats::PutWaitMicroseconds, self->put_wait.elapsedMicroseconds());
        const size_t fill_end = self->fill_rope.empty()
            ? self->physical_window.offset
            : std::min(self->physical_window.end(), self->fill_rope.range().end());
        pushRopeToWriters(self->writers, self->physical_window, self->fill_rope,
            self->put_bytes_counter, &self->interrupt_requested, self->stats);
        /// Pin the partial segment under the just-written frontier until the
        /// reap (see `fill_pin`): the foreground's finalize pinned BEFORE this
        /// fill landed, so a fresh segment was not pinnable there.
        for (const auto & w : self->writers)
        {
            if (w.writer && fill_end >= w.writer->range().offset && fill_end < w.writer->range().end())
                if (auto pin = w.writer->pin(fill_end))
                {
                    self->fill_pin = std::move(pin);
                    break;
                }
        }
        /// The writers are NOT released here - the reap returns them home (a
        /// writer spans many windows and the next one needs it). Only the rope
        /// is dropped: the fill is committed (or abandoned on interrupt).
        self->fill_rope = {};
        return self->interrupt_requested.load() ? StepResult::Interrupted : StepResult::Done;
    };

    if (runner->schedule(m, StepKind::Put))
        stats.add(Stats::PutScheduled);
    else
        stats.add(Stats::PutPoolFull);  /// parked; the sweep grants one reschedule
    put_machines.push_back(std::move(m));
}

void ReaderExecutor::schedulePromoteStep(CacheTier from_tier, ByteRange range, const Rope & bytes, Stats & out_stats)
{
    /// Pool-less executors promote synchronously, as always.
    if (!runner)
    {
        maybePromote(from_tier, range, bytes, out_stats);
        return;
    }

    /// STRICTLY optional: over the cap means skip, not park - a warm serve must
    /// never wait on promote bookkeeping.
    if (put_machines.size() >= MAX_PUT_MACHINES)
    {
        out_stats.add(Stats::PromoteSkipped);
        return;
    }

    auto pm = std::make_shared<FetchMachine>();
    pm->requested_range = range;
    pm->physical_window = range;

    /// Borrow the faster-tier writers overlapping `range` that are HOME - chain
    /// order, breaking at the serving tier (`[CF-promote]`, decided here on the
    /// foreground; the machine gets a flat list). A writer on loan to a fill is
    /// simply not here - that part of the promote is skipped, not waited for.
    for (size_t i = 0; i < read_plan.bufs.size(); ++i)
    {
        auto & buf = read_plan.bufs[i];
        if (!buf.provider)
            continue;
        if (buf.provider->tier() == from_tier)
            break;
        auto kept = std::stable_partition(buf.writers.begin(), buf.writers.end(),
            [&](const MissEntry & w)
            {
                return !(w.writer && w.range.offset < range.end() && range.offset < w.range.end());
            });
        for (auto it = kept; it != buf.writers.end(); ++it)
        {
            pm->writers.push_back(std::move(*it));
            pm->writer_origins.push_back(i);
        }
        buf.writers.erase(kept, buf.writers.end());
    }
    if (pm->writers.empty())
        return;  /// nothing faster populatable for this run (or all on loan)

    pm->fill_rope = bytes;
    pm->put_wait.restart();
    pm->put_bytes_counter = Stats::BytesPromoted;
    pm->run_step = [this, self = pm.get()]
    {
        self->stats.add(Stats::PutWaitMicroseconds, self->put_wait.elapsedMicroseconds());
        pushRopeToWriters(self->writers, self->physical_window, self->fill_rope,
            self->put_bytes_counter, &self->interrupt_requested, self->stats);
        self->fill_rope = {};
        return self->interrupt_requested.load() ? StepResult::Interrupted : StepResult::Done;
    };

    if (runner->schedule(pm, StepKind::Put))
    {
        stats.add(Stats::PutScheduled);
        put_machines.push_back(std::move(pm));
    }
    else
    {
        /// Skip, no park: return the borrowed writers home right away.
        out_stats.add(Stats::PromoteSkipped);
        reapPutMachine(*pm);
    }
}

void ReaderExecutor::reapPutMachine(FetchMachine & m)
{
    /// Return the borrowed writers home so the next window's fill and the plan
    /// teardown's finalize find them where they have always lived. Valid by
    /// construction: every reap precedes the plan teardown, so the recorded
    /// bufs indices still address the borrowing plan.
    chassert(m.writers.size() == m.writer_origins.size());
    for (size_t i = 0; i < m.writers.size(); ++i)
    {
        chassert(m.writer_origins[i] < read_plan.bufs.size());
        read_plan.bufs[m.writer_origins[i]].writers.push_back(std::move(m.writers[i]));
    }
    m.writers.clear();
    m.writer_origins.clear();

    /// A failed put is logged, never thrown - a read must not fail because
    /// cache population failed.
    if (m.failure)
    {
        stats.add(Stats::PutFailed);
        tryLogException(m.failure, log, "Deferred cache fill failed", LogsLevel::debug);
    }
    stats += m.stats;
}

void ReaderExecutor::sweepPutMachines(bool wait)
{
    if (put_machines.empty())
        return;
    for (auto it = put_machines.begin(); it != put_machines.end();)
    {
        auto & m = **it;
        switch (m.state.load())
        {
            case MachineState::Scheduled:
            case MachineState::Running:
            {
                if (!wait)
                {
                    ++it;
                    continue;
                }
                /// Plan rebuild / destruction: let the bounded write finish
                /// (one window of local I/O) rather than drop bytes in hand.
                runner->waitReleased(m);
                break;
            }
            case MachineState::ParkedPoolFull:
            {
                /// The ladder: one reschedule, then abandon (the fill is skipped;
                /// the writers still return home). A rebuild/teardown sweep
                /// abandons outright - the plan it would fill is going away.
                if (!wait && !m.put_rescheduled)
                {
                    m.put_rescheduled = true;
                    if (runner->schedule(*it, StepKind::Put))
                    {
                        stats.add(Stats::PutScheduled);
                        ++it;
                        continue;
                    }
                }
                stats.add(Stats::PutAbandoned);
                break;
            }
            case MachineState::Constructed:
            case MachineState::AwaitCollect:
            case MachineState::Interrupted:
            case MachineState::Done:
            case MachineState::Cancelled:
            case MachineState::Failed:
                break;
        }

        reapPutMachine(m);
        it = put_machines.erase(it);
    }
}

void ReaderExecutor::joinPutMachinesOverlapping(ByteRange window, bool writers_too)
{
    if (put_machines.empty())
        return;
    for (auto it = put_machines.begin(); it != put_machines.end();)
    {
        auto & m = **it;
        /// The machine touches `window` through its own aligned window OR - for
        /// callers that need the writers home - any borrowed writer's range
        /// (writers span many windows).
        bool overlaps = m.physical_window.offset < window.end()
            && window.offset < m.physical_window.end();
        if (writers_too)
        {
            for (const auto & w : m.writers)
            {
                if (overlaps)
                    break;
                overlaps = w.range.offset < window.end() && window.offset < w.range.end();
            }
        }
        if (!overlaps)
        {
            ++it;
            continue;
        }
        if (m.state.load() == MachineState::ParkedPoolFull)
        {
            /// A parked fill whose ranges the foreground is about to need: write it
            /// HERE rather than skip - skipping would re-fetch the bytes from the
            /// source AND drop the fill (double loss). The one case a deferred
            /// write runs on the client thread, bounded by one window.
            pushRopeToWriters(m.writers, m.physical_window, m.fill_rope,
                Stats::BytesPushedToCacheSync, /*interrupt=*/nullptr, m.stats);
            m.fill_rope = {};
        }
        else
        {
            runner->waitReleased(m);
        }
        reapPutMachine(m);
        it = put_machines.erase(it);
    }
}

void ReaderExecutor::maybePromote(CacheTier from_tier, ByteRange range, const Rope & bytes, Stats & out_stats)
{
    /// The POOL-LESS promote body (`schedulePromoteStep` defers the same write to a
    /// put-only machine when pools are present).
    /// Walk the plan's held write buffers in chain order (provider-grouped fastest-first).
    /// Everything before `from_tier` is faster and missed `range` (else it would have
    /// served it), so write `bytes` up into each such tier's held write buffers. BREAK at
    /// the first `BufEntry` whose `provider->tier() == from_tier` - the tier-equality stop
    /// keeps the "first FilesystemCache buffer ends promotion" rule two fs layers rely on
    /// (`[CF-promote]`), and stops anything slower. `bytes`/`range` are file-level
    /// (physical) coordinates (pre-decryption shift), the space `write` expects. The
    /// write buffer's committed-set makes out-of-order/sub-block promote slices idempotent
    /// (no `status()` re-query), and a bypass tier has no write buffers (so it is skipped
    /// for free).
    for (auto & buf : read_plan.bufs)
    {
        if (!buf.provider)
            continue;
        if (buf.provider->tier() == from_tier)
            break;

        for (auto & w : buf.writers)
        {
            chassert(w.writer);
            const size_t lo = std::max({w.writer->range().offset, range.offset});
            const size_t hi = std::min({w.writer->range().end(), range.end()});
            if (lo >= hi)
                continue;
            const ByteRange sub{lo, hi - lo};
            auto slice = bytes.slice(sub);
            if (slice.empty())
                continue;
            out_stats.add(Stats::CachePopulateRequests);
            StatTimer put_scope(out_stats, Stats::CachePopulateMicroseconds);
            out_stats.add(Stats::BytesPromoted, w.writer->write(std::move(slice)));
            HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
                static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
        }
    }
}

ByteRange ReaderExecutor::boundedPlanSpan(size_t physical_start) const
{
    size_t want = plan_look_ahead_window;

    /// Clamp to the physical file end when the size is known. An unknown-size source
    /// plans the full look-ahead and discovers EOF via short reads.
    if (!offset_map.hasUnknownSize())
    {
        const size_t physical_end = offset_map.totalSize();
        if (physical_start >= physical_end)
            return ByteRange{physical_start, 0};
        want = std::min(want, physical_end - physical_start);
    }
    else if (!read_extent_end)
    {
        /// Unknown-size source with no advertised extent: planning the full look-ahead
        /// would produce an enormous aligned-miss region from the last cached byte to the
        /// look-ahead end (bytes that may not exist), and `openWriteBuffers` would `getOrSet`
        /// millions of tiny segments for them (the legacy per-window `lookup` never did).
        /// Cap to one window - just the bytes about to be fetched; EOF is learned via the
        /// short read. A finite extent (below) bounds it precisely when set.
        want = window_size;
    }

    /// Clamp to the advertised read extent so the plan never pins segments past the
    /// region the reader will actually consume.
    if (read_extent_end)
    {
        const size_t physical_extent_end = *read_extent_end + data_start_offset;
        if (physical_start >= physical_extent_end)
            return ByteRange{physical_start, 0};
        want = std::min(want, physical_extent_end - physical_start);
    }

    return ByteRange{physical_start, want};
}

void ReaderExecutor::extractResidentRuns(const CacheView & view, ByteRange plan_range, GeometryEntry & geom_entry)
{
    for (const auto & hit : view.hits())
    {
        /// Hits are segment-aligned and may extend past the plan span; clamp so
        /// streaming never reads outside `[plan_start, plan_end)`.
        const size_t lo = std::max(hit.range.offset, plan_range.offset);
        const size_t hi = std::min(hit.range.end(), plan_range.end());
        if (lo < hi)
            geom_entry.resident.push_back(ByteRange{lo, hi - lo});
    }
}

void ReaderExecutor::extractMissesAndOpenWriters(
    ICacheProvider & cache, const CacheView & view,
    const StoredObject & object, size_t object_file_offset,
    const IntervalSet & upper_hits, GeometryEntry & geom_entry, BufEntry & buf_entry)
{
    /// A bypass tier is never written, so it has no fetch/write target.
    if (!cache.populatesOnMiss())
        return;

    /// The cache-aligned gaps this tier lacks, UNCLAMPED to the plan span (only
    /// object-end-clamped inside the provider), so the aligned extent drives both the
    /// fetch and the over-read bound (`[CF-overread]`). PRUNE any cell fully covered by a
    /// faster tier (`upper_hits`): the data already lives upstream, so this tier needs no
    /// writer for it. Open the held write buffers over the survivors now
    /// (`[CF-plan-rebuild]`): one `getOrSet` per range, owned for the plan's life, so
    /// promotion/backfill only ever write into already-open buffers.
    VectorWithMemoryTracking<ByteRange> aligned_miss;
    for (const auto & miss : view.misses())
    {
        if (upper_hits.subtract(miss.range).empty())
            continue;  /// fully covered by a faster tier - prune
        geom_entry.aligned_miss.push_back(miss.range);
        aligned_miss.push_back(miss.range);
    }
    if (!aligned_miss.empty())
        buf_entry.writers = cache.openWriteBuffers(object, object_file_offset, aligned_miss);
}

void ReaderExecutor::planResidencyWindow(size_t physical_start)
{
    /// Machine-check the threading invariant: the held read/write buffers are
    /// foreground-private and must never be torn down / rebuilt while a prefetch worker
    /// is in flight (the worker co-owns only the immutable geometry). Deferred fills
    /// are joined first for the same reason - and so a segment is never aliased by a
    /// machine-held writer and a fresh `openWriteBuffers` of the next plan
    /// (`[CF-plan-rebuild]`).
    chassert(!machine);
    sweepPutMachines(/*wait=*/true);
    chassert(put_machines.empty());

    /// Reset the in-flight segment pin BEFORE tearing down the held buffers
    /// (`[CF-plan-rebuild]`): the pin aliases a held write buffer's own bare segment ref,
    /// so dropping it first makes `~DiskCacheWriter` the LAST owner and
    /// `FileSegment::complete` effective (otherwise a PARTIALLY_DOWNLOADED segment would
    /// stay un-shrunk and the next `openWriteBuffers` would alias the same segment in two
    /// buffers). The pin is re-established through the NEW buffer on the next
    /// `finalizeAssembledWindow`. Reset only the foreground cluster's pin -
    /// planResidencyWindow runs only with no prefetch in flight, so the worker's cluster
    /// is not live here.
    foreground_connection_state.inflight_segment_pin.reset();

    /// Release the PREVIOUS plan's held buffers FIRST: each held write buffer's
    /// destructor finalizes its segments (`FileSegment::complete`) and each `~CacheView`
    /// runs the deferred LRU-bump - AFTER those writes, since the bump is sequenced last
    /// in the view dtor. Foreground-timed (planResidencyWindow runs only after the
    /// in-flight prefetch is joined), so never concurrent with a worker.
    read_plan = {};

    /// Always publish a geometry (empty on the early-out paths below) so the query
    /// methods' callers never dereference a null snapshot: an empty geometry has
    /// `plan_end == plan_start`, so `covers` returns false and the caller re-plans.
    auto geom = std::make_shared<ReadPlanGeometry>();
    geom->plan_start = physical_start;
    geom->plan_end = physical_start;
    /// Sample memory pressure ONCE here, per plan. Every read within this plan (cache
    /// and remote, foreground and the prefetch worker via `job->pressure_level`) sizes
    /// off this cached level instead of re-querying the global monitor per call.
    geom->pressure_level = memoryPressureMonitor().currentLevel();

    /// TRIM: the plan span, bounded to the file end and the read extent. An empty
    /// span (the start already at/past a bound) publishes an empty plan.
    const ByteRange plan_range = boundedPlanSpan(physical_start);
    if (plan_range.size == 0)
    {
        ReadPlan empty;
        empty.geometry_snapshot = std::move(geom);  /// empty plan; covers()==false
        read_plan = std::move(empty);
        return;
    }
    geom->plan_end = plan_range.end();
    ReadPlan plan;

    /// One read-only residency probe (`planResidencyView`) per cache tier per object-piece,
    /// each translated by the two extract helpers into a 1:1 `GeometryEntry`/`BufEntry` pair
    /// (pushed BOTH-or-NEITHER, so `geometry()->entries` and `bufs` stay positionally
    /// aligned — `residentAt`'s entry index maps into `bufs`). `caches` is fastest-first, so
    /// `upper_hits` (the running union of already-processed, faster tiers' hits) lets a
    /// slower tier PRUNE the miss cells a faster tier already holds. The streaming `covered`
    /// guard in `readPhysicalWindow` re-establishes the same priority when serving.
    IntervalSet upper_hits;
    for (auto & cache : caches)
    {
        auto pieces = offset_map.map(plan_range);
        size_t piece_file_start = plan_range.offset;
        for (const auto & pr : pieces)
        {
            const size_t object_file_offset = piece_file_start - pr.object_offset;
            const ByteRange piece_range{piece_file_start, pr.size};

            auto view = cache->planResidencyView(pr.object, object_file_offset, piece_range);

            GeometryEntry geom_entry;
            geom_entry.tier = cache->tier();
            geom_entry.head_align = cache->fetchHeadAlignment();
            geom_entry.tail_align = cache->fetchTailAlignment();
            BufEntry buf_entry;
            buf_entry.provider = cache.get();
            buf_entry.object = pr.object;
            buf_entry.object_file_offset = object_file_offset;

            extractResidentRuns(*view, plan_range, geom_entry);
            extractMissesAndOpenWriters(*cache, *view, pr.object, object_file_offset, upper_hits, geom_entry, buf_entry);

            /// Fold this tier's hits into `upper_hits` so the next (slower) tier prunes
            /// against them. Read BEFORE the move below. Same-tier hits/misses are disjoint,
            /// so this never prunes a later piece of the same tier.
            for (const auto & r : geom_entry.resident)
                upper_hits.add(r);

            /// Drop records that are neither resident nor a populatable gap — nothing to
            /// read or write. Otherwise keep the view (its hit read buffers pin the
            /// resident segments) alongside the writers.
            if (!geom_entry.resident.empty() || !geom_entry.aligned_miss.empty())
            {
                buf_entry.view = std::move(view);
                geom->entries.push_back(std::move(geom_entry));
                plan.bufs.push_back(std::move(buf_entry));
            }

            piece_file_start += pr.size;
        }
    }

    chassert(geom->entries.size() == plan.bufs.size());

    /// Publish atomically: `geometry()` and `bufs` are one object (`read_plan`), so a
    /// reader can never see new geometry against a stale buffer vector. Assigning
    /// `read_plan` finalizes the previous plan's write buffers and runs its deferred
    /// LRU bumps.
    plan.geometry_snapshot = std::move(geom);
    read_plan = std::move(plan);

    LOG_TRACE(log, "planResidencyWindow: planned [{}, {}), {} entries",
        read_plan.geometry()->plan_start, read_plan.geometry()->plan_end, read_plan.geometry()->entries.size());
}

std::unique_ptr<ReaderExecutor> ReaderExecutor::makeTransientForReadAt(size_t start_position, size_t read_size) const
{
    /// `buffer_limit` is shared so the transient's live connection counts
    /// against the server-wide budget. `prefetch_pool` and
    /// `reader_executor_log` are intentionally NOT propagated: a one-shot
    /// `readBigAt` can't amortise prefetch latency (and would steal slots
    /// from a concurrent sequential reader), and per-call log rows would
    /// spam `system.reader_executor_log`.
    auto t = std::make_unique<ReaderExecutor>(
        source, stored_objects, caches,
        window_size, min_bytes_for_seek, block_size, log_file_path, max_tail_for_drain,
        plan_look_ahead_window);  /// plans over its one-shot range (clamped to the read extent)

    t->buffer_limit = buffer_limit;
    t->live_connection_min_read_bytes = live_connection_min_read_bytes;

#if USE_SSL
    t->decryption_layers = decryption_layers;
    t->decryption_headers = decryption_headers;
    t->decryption_initialized = decryption_initialized;
#endif
    t->data_start_offset = data_start_offset;
    t->read_extent_end = start_position + read_size;
    t->is_transient = true;
    t->seek(start_position);
    return t;
}

void ReaderExecutor::mergeTransientStats(const ReaderExecutor & transient)
{
    /// `readBigAt` fans out concurrently over one parent; serialize the roll-up. The
    /// transient already emitted its ProfileEvents at the read site (in this query's
    /// thread group), so this only accumulates into the parent's report aggregate.
    std::lock_guard lock(transient_stats_mutex);
    stats += transient.stats;
}

}
