#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/HistogramMetrics.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/getRandomASCIIString.h>
#include <base/getThreadId.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
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
    extern const Event ReaderExecutorPrefetchHits;
    extern const Event ReaderExecutorPrefetchCancelled;
    extern const Event ReaderExecutorPrefetchPoolFull;
    extern const Event ReaderExecutorPrefetchDiscardedRunning;
    extern const Event ReaderExecutorPrefetchDiscardWaitMicroseconds;
    extern const Event ReaderExecutorPrefetchIssuedSourceBytes;
    extern const Event ReaderExecutorPrefetchIssuedCacheBytes;
    extern const Event ReaderExecutorPrefetchWastedSourceBytes;
    extern const Event ReaderExecutorPrefetchWastedCacheBytes;
    extern const Event ReaderExecutorBufferSlotAcquired;
    extern const Event ReaderExecutorBufferSlotFailed;
}

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorActive;
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

namespace
{

/// Disjoint, sorted set of ByteRanges. Used by readPhysicalWindow to track
/// which logical bytes are already in `result` so that:
///   - cache hits are appended only for not-yet-covered subranges,
///   - source-fetched bytes are appended only for not-yet-covered subranges.
/// This makes `result` always disjoint by construction, so any `slice` over
/// it is also disjoint — which is what `cache->put` requires.
class IntervalSet
{
public:
    void add(ByteRange r)
    {
        if (r.size == 0)
            return;

        size_t new_start = r.offset;
        size_t new_end = r.end();

        auto erase_from = intervals.begin();
        while (erase_from != intervals.end() && erase_from->end() < new_start)
            ++erase_from;

        auto it = erase_from;
        while (it != intervals.end() && it->offset <= new_end)
        {
            new_start = std::min(new_start, it->offset);
            new_end = std::max(new_end, it->end());
            ++it;
        }

        auto insert_pos = intervals.erase(erase_from, it);
        intervals.insert(insert_pos, ByteRange{new_start, new_end - new_start});
    }

    /// Returns r minus all intervals in the set, as a list of disjoint
    /// sub-ranges in increasing-offset order.
    VectorWithMemoryTracking<ByteRange> subtract(ByteRange r) const
    {
        VectorWithMemoryTracking<ByteRange> out;
        if (r.size == 0)
            return out;
        size_t cur = r.offset;
        size_t end = r.end();
        for (const auto & i : intervals)
        {
            if (i.end() <= cur)
                continue;
            if (i.offset >= end)
                break;
            if (i.offset > cur)
                out.push_back({cur, i.offset - cur});
            cur = std::max(cur, i.end());
            if (cur >= end)
                break;
        }
        if (cur < end)
            out.push_back({cur, end - cur});
        return out;
    }

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<ISourceReader> source_,
    const StoredObjects & objects,
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches_,
    size_t window_size_,
    size_t min_bytes_for_seek_,
    size_t block_size_,
    String log_file_path_,
    size_t max_tail_for_drain_)
    : source(std::move(source_))
    , stored_objects(objects)
    , caches(std::move(caches_))
    , log_file_path(std::move(log_file_path_))
    , window_size(window_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , block_size(block_size_)
    , max_tail_for_drain(max_tail_for_drain_)
{
    if (window_size == 0 || block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "reader_executor_window_size and reader_executor_block_size must be > 0, "
            "got window_size={}, block_size={}", window_size, block_size);

    /// `build` can throw (e.g. an `UnknownSize` object in a multi-object
    /// pipeline). Bump the live-instance gauge only after it succeeds: a ctor
    /// that throws skips `~ReaderExecutor`, so an earlier bump would leak.
    offset_map.build(stored_objects);
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorActive);
    creator_query_id = String(CurrentThread::getQueryId());
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, block_size={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, block_size, caches.size());
}

ReaderExecutor::~ReaderExecutor()
{
    /// Cleanup, not a seek-away: `UNNEEDED` is not logged (matches legacy). The
    /// abandon slot is pre-reserved at prefetch-submit time, so stashing the
    /// in-flight handle here never allocates - safe from this `noexcept` destructor.
    discardPrefetch(FilesystemPrefetchState::UNNEEDED);
    drainAbandonedPrefetches(/*wait_finished=*/true);
    CurrentMetrics::sub(CurrentMetrics::ReaderExecutorActive);

    /// A transient `readBigAt` executor rolls its stats into the parent via
    /// mergeTransientStats; emitting ProfileEvents / a reader_executor_log row
    /// here too would double-count. The parent's destructor reports the aggregate.
    if (is_transient)
        return;

    /// A live connection still open here was never drained to its bound (else
    /// releaseLiveBufferAtBound would have reset it): an incomplete connection.
    accountLiveBufferDrop(/*at_eof=*/false);

    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromPageCache, stats.bytes_from_page_cache);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromFilesystemCache, stats.bytes_from_filesystem_cache);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromSource, stats.bytes_from_source);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesPushedToCacheSync, stats.bytes_pushed_to_cache_sync);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesPushedToCacheAsync, stats.bytes_pushed_to_cache_async);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetRequests, stats.cache_get_requests);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateRequests, stats.cache_populate_requests);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceRequests, stats.source_requests);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorIncompleteConnections, stats.incomplete_connections);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadBytes, stats.over_read_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorRequestedBytes, stats.bytes_requested);

    /// Modeled cost in microseconds; same weights documented at the ProfileEvents declaration (ms -> us).
    /// Bandwidth is charged on bytes_from_source (useful source payload + over-read), not over-read alone.
    ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds,
        30000 * stats.source_requests
        + 5000 * stats.incomplete_connections
        + 20000 * stats.bytes_from_source / (1024 * 1024)
        + 100 * stats.cache_populate_requests
        + 50 * stats.cache_get_requests);

    ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetMicroseconds, stats.cache_get_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateMicroseconds, stats.cache_populate_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceReadMicroseconds, stats.source_read_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorDecryptMicroseconds, stats.decrypt_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWaitMicroseconds, stats.prefetch_wait_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorSyncReadMicroseconds, stats.sync_read_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchHits, stats.prefetch_hits);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchCancelled, stats.prefetch_cancelled);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchPoolFull, stats.prefetch_pool_full);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardedRunning, stats.prefetch_discarded_running);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardWaitMicroseconds, stats.prefetch_discard_wait_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchIssuedSourceBytes, stats.prefetch_issued_source_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchIssuedCacheBytes, stats.prefetch_issued_cache_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWastedSourceBytes, stats.prefetch_wasted_source_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWastedCacheBytes, stats.prefetch_wasted_cache_bytes);

    LOG_DEBUG(log,
        "Destroyed: from_page_cache={} from_filesystem_cache={} from_source={} "
        "pushed_to_cache_sync={} pushed_to_cache_async={} "
        "get_reqs={} populate_reqs={} src_reqs={} "
        "get_us={} populate_us={} src_us={} decrypt_us={} "
        "prefetch_wait_us={} sync_read_us={} "
        "prefetch_hits={} prefetch_cancelled={} prefetch_pool_full={} "
        "prefetch_discarded_running={} prefetch_discard_wait_us={} "
        "prefetch_issued_source_bytes={} prefetch_issued_cache_bytes={} "
        "prefetch_wasted_source_bytes={} prefetch_wasted_cache_bytes={} "
        "incomplete_connections={} over_read_bytes={}",
        stats.bytes_from_page_cache, stats.bytes_from_filesystem_cache, stats.bytes_from_source,
        stats.bytes_pushed_to_cache_sync, stats.bytes_pushed_to_cache_async,
        stats.cache_get_requests, stats.cache_populate_requests, stats.source_requests,
        stats.cache_get_us, stats.cache_populate_us,
        stats.source_read_us, stats.decrypt_us,
        stats.prefetch_wait_us, stats.sync_read_us,
        stats.prefetch_hits, stats.prefetch_cancelled, stats.prefetch_pool_full,
        stats.prefetch_discarded_running, stats.prefetch_discard_wait_us,
        stats.prefetch_issued_source_bytes, stats.prefetch_issued_cache_bytes,
        stats.prefetch_wasted_source_bytes, stats.prefetch_wasted_cache_bytes,
        stats.incomplete_connections, stats.over_read_bytes);

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
        elem.bytes_from_page_cache = stats.bytes_from_page_cache;
        elem.bytes_from_filesystem_cache = stats.bytes_from_filesystem_cache;
        elem.bytes_from_source = stats.bytes_from_source;
        elem.bytes_pushed_to_cache_sync = stats.bytes_pushed_to_cache_sync;
        elem.bytes_pushed_to_cache_async = stats.bytes_pushed_to_cache_async;
        elem.cache_get_requests = stats.cache_get_requests;
        elem.cache_populate_requests = stats.cache_populate_requests;
        elem.source_requests = stats.source_requests;
        elem.incomplete_connections = stats.incomplete_connections;
        elem.over_read_bytes = stats.over_read_bytes;
        elem.cache_get_us = stats.cache_get_us;
        elem.cache_populate_us = stats.cache_populate_us;
        elem.source_read_us = stats.source_read_us;
        elem.decrypt_us = stats.decrypt_us;
        elem.prefetch_wait_us = stats.prefetch_wait_us;
        elem.sync_read_us = stats.sync_read_us;
        elem.prefetch_hits = stats.prefetch_hits;
        elem.prefetch_cancelled = stats.prefetch_cancelled;
        elem.prefetch_pool_full = stats.prefetch_pool_full;
        elem.prefetch_discarded_running = stats.prefetch_discarded_running;
        elem.prefetch_discard_wait_us = stats.prefetch_discard_wait_us;
        elem.prefetch_issued_source_bytes = stats.prefetch_issued_source_bytes;
        elem.prefetch_issued_cache_bytes = stats.prefetch_issued_cache_bytes;
        elem.prefetch_wasted_source_bytes = stats.prefetch_wasted_source_bytes;
        elem.prefetch_wasted_cache_bytes = stats.prefetch_wasted_cache_bytes;

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

void ReaderExecutor::setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool)
{
    prefetch_pool = std::move(pool);
}

void ReaderExecutor::setBufferLimit(std::shared_ptr<SourceBufferLimit> limit)
{
    buffer_limit = std::move(limit);
}

void ReaderExecutor::setReaderExecutorLog(std::shared_ptr<ReaderExecutorLog> log_)
{
    reader_executor_log = std::move(log_);
}

void ReaderExecutor::setFilesystemReadPrefetchesLog(std::shared_ptr<FilesystemReadPrefetchesLog> log_)
{
    prefetches_log = std::move(log_);
    if (prefetches_log)
        prefetch_reader_id = getRandomASCIIString(8);
}

void ReaderExecutor::emitPrefetchLog(FilesystemPrefetchState state, Int64 size)
{
    if (!prefetches_log)
        return;

    FilesystemReadPrefetchesLogElement elem;
    elem.event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    elem.query_id = creator_query_id;
    /// Report the object actually requested from storage, in object-local
    /// coordinates. `prefetch_range.offset` is the executor-logical offset; add
    /// `data_start_offset` to reach the physical file offset (encrypted reads
    /// shift by the header), then resolve the `StoredObject` and subtract its file
    /// start, so gather reads name the mapped object rather than the executor-wide
    /// first-object path/logical offset. (A window may span into a later object;
    /// the row reflects the object under its start, matching legacy per-buffer
    /// prefetch logging.)
    const size_t physical_offset = prefetch_range.offset + data_start_offset;
    size_t object_file_offset = 0;
    const StoredObject * prefetch_object = offset_map.findObjectAt(physical_offset, &object_file_offset);
    elem.path = prefetch_object ? prefetch_object->remote_path : log_file_path;
    elem.offset = prefetch_object ? (physical_offset - object_file_offset) : prefetch_range.offset;
    elem.size = size;
    elem.prefetch_submit_time = prefetch_submit_time;
    elem.execution_watch = prefetch_execution_watch;
    /// The executor's prefetch pool has no per-prefetch priority concept.
    elem.priority = Priority{};
    elem.state = state;
    elem.thread_id = getThreadId();
    elem.reader_id = prefetch_reader_id;
    prefetches_log->add(std::move(elem));
}

std::optional<SourceBufferSlot> ReaderExecutor::acquireSlotCounted(const StoredObject & object)
{
    auto slot = buffer_limit->tryAcquire(buffer_limit, object.remote_path, object.local_path, String(CurrentThread::getQueryId()));
    if (slot)
        ProfileEvents::increment(ProfileEvents::ReaderExecutorBufferSlotAcquired);
    else
        ProfileEvents::increment(ProfileEvents::ReaderExecutorBufferSlotFailed);
    return slot;
}

void ReaderExecutor::ensurePreAcquiredSlot()
{
    if (live_buffer || pre_acquired_slot || !buffer_limit)
        return;

    const size_t physical_position = position + data_start_offset;
    const StoredObject * object = offset_map.findObjectAt(physical_position);
    if (!object)
        return;

    auto slot = acquireSlotCounted(*object);
    if (slot)
    {
        LOG_TRACE(log, "ensurePreAcquiredSlot: got slot for {}", object->remote_path);
        pre_acquired_slot.emplace(std::move(*slot));
    }
}

void ReaderExecutor::releaseStalePreAcquiredSlot(const String & target_path)
{
    if (pre_acquired_slot && pre_acquired_slot->objectPath() != target_path)
        pre_acquired_slot.reset();
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
WindowAndBlock sizesAtCurrentPressure(size_t base_window, size_t base_block)
{
    const size_t level = static_cast<size_t>(memoryPressureMonitor().currentLevel());
    static constexpr size_t FLOOR = 128ULL << 10;
    const size_t window = std::min(std::max(base_window / WINDOW_REDUCTION[level], FLOOR), base_window);
    size_t block = std::min(std::max(base_block / BLOCK_REDUCTION[level], FLOOR), base_block);
    block = std::min(block, window);
    return {window, block};
}

}

size_t ReaderExecutor::effectiveBlockSize() const
{
    return sizesAtCurrentPressure(window_size, block_size).block_bytes;
}

size_t ReaderExecutor::effectiveWindowSize() const
{
    const auto sizes = sizesAtCurrentPressure(window_size, block_size);
    /// Only the live path streams one block at a time, reusing the open
    /// connection across windows. Stateless reads - local files and remote
    /// reads with live connections disabled - keep the full (pressure-scaled)
    /// window so each one-shot open amortises its setup over a window, not a
    /// block.
    if (live_buffer || pre_acquired_slot)
        return sizes.block_bytes;
    return sizes.window_bytes;
}

size_t ReaderExecutor::effectivePrefetchWindowSize() const
{
    const size_t level = static_cast<size_t>(memoryPressureMonitor().currentLevel());
    if (!PREFETCH_ENABLED[level])
        return 0;
    /// Prefetch reads the same window as a synchronous read; under High/Critical it
    /// is suppressed entirely (above) rather than shrunk.
    return effectiveWindowSize();
}

size_t ReaderExecutor::clampToExtent(size_t win_size) const
{
    if (!read_extent_end)
        return win_size;
    const size_t remaining = *read_extent_end > position ? *read_extent_end - position : 0;
    return std::min(win_size, remaining);
}

void ReaderExecutor::releaseLiveBufferAtBound()
{
    if (live_buffer && live_buffer->read_until && live_buffer->current_position >= *live_buffer->read_until)
    {
        live_buffer.reset();
        inflight_segment_pin.reset();
    }
}

void ReaderExecutor::accountLiveBufferDrop(bool at_eof)
{
    if (!live_buffer)
        return;
    /// Reusable iff the response was fully consumed: read to its right bound, or
    /// to EOF. An open-ended buffer dropped before EOF, or a bounded one dropped
    /// before its bound, is abandoned mid-response and not pool-reusable.
    bool drained = false;
    if (at_eof)
        drained = true;
    else if (live_buffer->read_until)
        drained = live_buffer->current_position >= *live_buffer->read_until;
    if (!drained)
        ++stats.incomplete_connections;
}

void ReaderExecutor::maybeDrainLiveTail()
{
    /// Called just before dropping a live connection that is not at its bound. If
    /// only a small tail remains, read it out so the connection completes and is
    /// returned to the pool reusable (no incomplete connection) rather than reset.
    /// The drained bytes cross the wire - charged as over-read. Worth it only for a
    /// tail below the I-weight/bandwidth breakeven; bounded by `max_tail_for_drain`.
    if (!live_buffer || !live_buffer->read_until)
        return;
    const size_t bound = *live_buffer->read_until;
    if (live_buffer->current_position >= bound)
        return; /// already drained; releaseLiveBufferAtBound covers this
    const size_t tail = bound - live_buffer->current_position;
    if (tail > max_tail_for_drain)
        return;
    const size_t skipped = skipLiveBufferForward(tail);
    stats.bytes_from_source += skipped;
    stats.over_read_bytes += skipped;
    live_buffer->current_position += skipped;
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

    /// Drain any in-flight prefetch before changing the extent: the prefetch
    /// worker reads `read_extent_end` to bound its source connection, so mutating
    /// it underneath the worker would race, and a prefetch issued for the old
    /// range must not be served for the new one. No-op when no prefetch is in
    /// flight (the common per-mark-range boundary, where prefetch is clamped to
    /// the extent), so it is free on the hot path. Mirrors the legacy
    /// `AsynchronousBoundedReadBuffer::setReadUntilPosition` contract.
    discardPrefetch(FilesystemPrefetchState::CANCELLED_WITH_RANGE_CHANGE);
    read_extent_end = logical_end;
}

void ReaderExecutor::maybeTriggerPrefetch()
{
    if (!prefetch_pool || prefetch_handle || atEnd())
        return;

    drainAbandonedPrefetches();

    size_t logical_size = totalSize();

    ensurePreAcquiredSlot();

    const size_t prefetch_window = effectivePrefetchWindowSize();
    if (prefetch_window == 0)
    {
        /// Read-ahead suppressed under High/Critical memory pressure. Release the
        /// slot reserved above (same reason as the submit-failure path below) so a
        /// suppressed prefetch doesn't pin an idle `max_remote_read_connections`
        /// slot; the next readNextWindow re-acquires for its synchronous read.
        pre_acquired_slot.reset();
        return;
    }

    size_t next_size = offset_map.hasUnknownSize()
        ? prefetch_window
        : std::min(prefetch_window, logical_size - position);
    /// Keep prefetch within the advertised extent: never read ahead past what the
    /// consumer said it will read. At the extent boundary there is nothing left to
    /// prefetch - release the slot reserved above (as the suppressed-window path).
    next_size = clampToExtent(next_size);
    if (next_size == 0)
    {
        pre_acquired_slot.reset();
        return;
    }
    ByteRange next_physical_window{position + data_start_offset, next_size};
    size_t next_logical_offset = position;

    LOG_TRACE(log, "Prefetch: submitting physical [{}, {})", next_physical_window.offset, next_physical_window.end());

    /// Prefetch-log timing. Reset `execution_watch` before submit so a
    /// never-run (cancelled-while-queued) prefetch logs no execution timing;
    /// the worker fills it only if it actually runs `readPhysicalWindow`.
    prefetch_submit_time = std::chrono::system_clock::now();
    prefetch_execution_watch.reset();

    /// Snapshot the issued counters BEFORE submit (the worker may start the moment
    /// submit returns), so a later discard attributes exactly this prefetch's
    /// source/cache bytes - the delta the worker adds - to wasted.
    prefetch_issued_source_at_submit = stats.prefetch_issued_source_bytes;
    prefetch_issued_cache_at_submit = stats.prefetch_issued_cache_bytes;

    /// Reserve the stash slot up front so a later discard of this prefetch (seek or
    /// the readNextWindow cancel path) can move the handle into `abandoned_prefetches`
    /// WITHOUT allocating. A `push_back` realloc there could throw; on the cancel path
    /// that drops the handle before the worker is joined (it still runs against this
    /// `ReaderExecutor` - use-after-free). Capacity is retained across drains, so this
    /// allocates only on the first prefetch; reserving here keeps it off the hot
    /// discard paths.
    abandoned_prefetches.reserve(abandoned_prefetches.size() + 1);

    auto handle = prefetch_pool->submit([this, next_physical_window]()
    {
        Stopwatch watch;
        auto rope = readWindowLogical(next_physical_window, /*from_prefetch=*/true);
        watch.stop();
        prefetch_execution_watch = watch;
        return rope;
    });

    if (!handle)
    {
        LOG_TRACE(log, "Prefetch: pool queue full, will fetch synchronously on next read");
        ++stats.prefetch_pool_full;
        /// No prefetch task will consume the slot `ensurePreAcquiredSlot` reserved
        /// above; release it so a full prefetch queue doesn't pin idle
        /// `max_remote_read_connections` slots across many readers. The next
        /// readNextWindow re-acquires before its synchronous read.
        pre_acquired_slot.reset();
        return;
    }

    prefetch_handle = std::move(handle);
    /// Track prefetch_range in logical coordinates — same space as `position`
    /// and as the decrypted rope returned by the handle.
    prefetch_range = ByteRange{next_logical_offset, next_size};
}

void ReaderExecutor::discardPrefetch(FilesystemPrefetchState reason)
{
    drainAbandonedPrefetches();

    auto local_handle = std::move(prefetch_handle);
    if (!local_handle)
        return;

    LOG_TRACE(log, "Prefetch: discarding [{}, {})", prefetch_range.offset, prefetch_range.end());

    if (local_handle->tryCancel())
    {
        /// Cancelled before the worker ran - count it like the readNextWindow
        /// cancel path (but not destructor `UNNEEDED` cleanup) so
        /// `ReaderExecutorPrefetchCancelled` / `reader_executor_log.prefetch_cancelled`
        /// includes seek-cancelled prefetches.
        if (reason != FilesystemPrefetchState::UNNEEDED)
            ++stats.prefetch_cancelled;
        /// Still queued: the worker will take the cancellation path and never
        /// touch our state. Stash it for the destructor to join.
        abandoned_prefetches.push_back(std::move(local_handle));
    }
    else
    {
        /// Already running and mutating our state via the captured `this`; block
        /// until it finishes so the caller can safely overwrite it. Work wasted.
        ++stats.prefetch_discarded_running;
        StopwatchAccumulator wait_scope(stats.prefetch_discard_wait_us);
        try
        {
            auto rope = local_handle->get();
            /// The worker's reads since submit (the delta of the issued counters)
            /// are exactly this discarded prefetch's bytes; attribute them to
            /// wasted, split source vs cache (sum == rope.totalBytes()).
            stats.prefetch_wasted_source_bytes
                += stats.prefetch_issued_source_bytes - prefetch_issued_source_at_submit;
            stats.prefetch_wasted_cache_bytes
                += stats.prefetch_issued_cache_bytes - prefetch_issued_cache_at_submit;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Discarded prefetch task threw", LogsLevel::debug);
        }
    }

    /// Log the dropped prefetch (seek-away). Destructor cleanup passes
    /// `UNNEEDED`, which the legacy path never logs — skip it too.
    if (reason != FilesystemPrefetchState::UNNEEDED)
        emitPrefetchLog(reason, static_cast<Int64>(prefetch_range.size));
}

void ReaderExecutor::drainAbandonedPrefetches(bool wait_finished)
{
    abandoned_prefetches.erase(
        std::remove_if(abandoned_prefetches.begin(), abandoned_prefetches.end(),
            [this, wait_finished](std::shared_ptr<PrefetchHandle> & h)
            {
                if (!wait_finished && !h->isFinished())
                    return false;
                try
                {
                    std::ignore = h->get();
                }
                catch (...)
                {
                    /// Cancellation throws `PrefetchHandle: task was cancelled`
                    /// — every abandoned-on-cancel handle reaches here. Debug
                    /// level keeps the error log clean.
                    tryLogCurrentException(log, "Abandoned prefetch task threw", LogsLevel::debug);
                }
                return true;
            }),
        abandoned_prefetches.end());
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

    Rope header_rope = readPhysicalWindow(ByteRange{0, data_start_offset}, /*from_prefetch=*/false);

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

Rope ReaderExecutor::readWindowLogical(ByteRange physical_window, bool from_prefetch)
{
    Rope rope = readPhysicalWindow(physical_window, from_prefetch);
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

    StopwatchAccumulator decrypt_scope(stats.decrypt_us);

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
    size_t logical_size = totalSize();
    Rope rope;

    if (prefetch_handle)
    {
        auto local_handle = std::move(prefetch_handle);

        if (local_handle->tryCancel())
        {
            LOG_TRACE(log, "readNextWindow: prefetch was queued, cancelling and reading from position {}", position);
            ++stats.prefetch_cancelled;

            /// Stash BEFORE the synchronous read: the worker attaches a
            /// `ThreadGroupSwitcher` before checking cancellation, so
            /// ~ReaderExecutor must join it before our state is freed. If the
            /// read below throws, the handle would otherwise be dropped on the
            /// unwind and the destructor would never wait (see `discardPrefetch`).
            abandoned_prefetches.push_back(std::move(local_handle));

            ensurePreAcquiredSlot();
            size_t win_size = offset_map.hasUnknownSize()
                ? effectiveWindowSize()
                : std::min(effectiveWindowSize(), logical_size - position);
            win_size = clampToExtent(win_size);
            ByteRange physical_window{position + data_start_offset, win_size};
            StopwatchAccumulator sync_scope(stats.sync_read_us);
            rope = readWindowLogical(physical_window, /*from_prefetch=*/false);
            HistogramMetrics::ReaderExecutorSyncReadLatency.observe(
                static_cast<HistogramMetrics::Value>(sync_scope.elapsedMicroseconds()));
        }
        else
        {
            /// If a seek landed inside the prefetched window, trim the prefix
            /// below so `rope.range().offset` matches `position`.
            LOG_TRACE(log, "readNextWindow: waiting on prefetched [{}, {})", prefetch_range.offset, prefetch_range.end());
            StopwatchAccumulator wait_scope(stats.prefetch_wait_us);
            rope = local_handle->get();
            ++stats.prefetch_hits;
            HistogramMetrics::ReaderExecutorPrefetchWaitLatency.observe(
                static_cast<HistogramMetrics::Value>(wait_scope.elapsedMicroseconds()));
            /// Log before the seek-trim below so `size` is the full prefetched
            /// payload (what the prefetch actually delivered).
            emitPrefetchLog(FilesystemPrefetchState::USED, static_cast<Int64>(rope.totalBytes()));

            if (!rope.empty() && position > rope.range().offset)
            {
                size_t end = rope.range().end();
                rope = rope.slice(ByteRange{position, end - position});
            }
        }
    }
    else
    {
        if (atEnd())
        {
            LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
            /// Release per-stream resources at EOF instead of waiting for
            /// the caller to drop the `PipelineReadBuffer`. A subsequent
            /// seek-back will re-open and re-acquire.
            accountLiveBufferDrop(/*at_eof=*/true);
            live_buffer.reset();
            inflight_segment_pin.reset();
            pre_acquired_slot.reset();
            return {};
        }

        ensurePreAcquiredSlot();
        size_t win_size = clampToExtent(std::min(effectiveWindowSize(), logical_size - position));
        ByteRange physical_window{position + data_start_offset, win_size};
        LOG_TRACE(log, "readNextWindow: synchronous read physical [{}, {}), logical [{}, {})",
            physical_window.offset, physical_window.end(), position, position + win_size);
        StopwatchAccumulator sync_scope(stats.sync_read_us);
        rope = readWindowLogical(physical_window, /*from_prefetch=*/false);
        HistogramMetrics::ReaderExecutorSyncReadLatency.observe(
            static_cast<HistogramMetrics::Value>(sync_scope.elapsedMicroseconds()));
    }

    stats.bytes_requested += rope.range().size;
    position += rope.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        rope.range().size, rope.getNodes().size(), position);

    /// Unknown-size EOF is latched by a short read here, not the pre-read gate,
    /// and the caller stops on the empty rope without a follow-up call — so
    /// release the live connection + slot now rather than leaking it.
    if (reached_eof)
    {
        accountLiveBufferDrop(/*at_eof=*/true);
        live_buffer.reset();
        inflight_segment_pin.reset();
        pre_acquired_slot.reset();
    }

    maybeTriggerPrefetch();

    return rope;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_DEBUG(log, "seek to {}, current position={}", new_position, position);

    if (prefetch_handle
        && new_position >= prefetch_range.offset
        && new_position < prefetch_range.end())
    {
        LOG_TRACE(log, "seek: target within prefetch [{}, {}), keeping prefetch",
            prefetch_range.offset, prefetch_range.end());
        position = new_position;
        return;
    }

    discardPrefetch(FilesystemPrefetchState::CANCELLED_WITH_SEEK);

    const size_t new_physical = new_position + data_start_offset;
    size_t new_obj_file_offset = 0;
    const StoredObject * new_obj = offset_map.findObjectAt(new_physical, &new_obj_file_offset);

    /// `pre_acquired_slot` is keyed to the old object. Drop it when the new
    /// position lands in a different (or no) object — pairs with the
    /// path-mismatch reset in `readFromSource`.
    releaseStalePreAcquiredSlot(new_obj ? new_obj->remote_path : String{});

    /// Decide the live buffer's fate across the seek. Keep it for a forward seek
    /// small enough to bridge within its right bound: the next `readFromSource`
    /// skips the seeked-over gap on the open GET instead of reopening (the same
    /// rule and `min_bytes_for_seek` bound used there). A backward seek, a
    /// different object, or a gap past that bound closes it. A cache-hit path
    /// skips `readFromSource`'s check, so this is also where a stale connection +
    /// slot would otherwise leak until EOF/destruction.
    if (live_buffer)
    {
        const bool same_obj = new_obj && live_buffer->slot.objectPath() == new_obj->remote_path;
        const size_t new_local = same_obj ? new_physical - new_obj_file_offset : 0;
        const bool keep = same_obj
            && new_local >= live_buffer->current_position
            && new_local - live_buffer->current_position <= min_bytes_for_seek
            && (!live_buffer->read_until || new_local <= *live_buffer->read_until);
        if (!keep)
        {
            LOG_TRACE(log, "seek: live buffer for {} (at {}) no longer matches target, closing",
                live_buffer->slot.objectPath(), live_buffer->current_position);
            maybeDrainLiveTail();
            accountLiveBufferDrop(/*at_eof=*/false);
            live_buffer.reset();
            inflight_segment_pin.reset();
        }
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

Rope ReaderExecutor::readFromLiveBufferIntoRope(
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
{
    chassert(live_buffer);
    auto & buf = *live_buffer->buffer;

    Rope rope;
    size_t total_read = 0;

    for (auto & block : blocks)
    {
        size_t chunk = block->size();
        size_t got = readIntoBlock(buf, block->data(), chunk);

        LOG_DEBUG(log, "readFromLiveBufferIntoRope: block {}, chunk={}, got={}, first_byte=0x{:02x}",
            rope.getNodes().size(), chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
            break;

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    return rope;
}

size_t ReaderExecutor::skipLiveBufferForward(size_t gap)
{
    chassert(live_buffer);
    auto & buf = *live_buffer->buffer;

    /// Discard `gap` bytes from the open source read so the connection's
    /// frontier advances over an already-cached gap. Uses a scratch block
    /// because the source is in external-buffer mode (mirrors `readIntoBlock`);
    /// the bytes are transferred and thrown away - only the source request is
    /// saved. Returns bytes actually skipped (< `gap` only if the source hit EOF).
    const size_t scratch_size = std::min(gap, block_size);
    auto scratch = std::make_shared<OwnedRopeBuffer>(scratch_size);

    size_t skipped = 0;
    while (skipped < gap)
    {
        const size_t chunk = std::min(gap - skipped, scratch_size);
        const size_t got = readIntoBlock(buf, scratch->data(), chunk);
        if (got == 0)
            break;
        skipped += got;
    }
    return skipped;
}

Rope ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
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
    if (live_buffer
        && live_buffer->slot.objectPath() == object.remote_path
        && offset >= live_buffer->current_position
        && offset - live_buffer->current_position <= min_bytes_for_seek
        && (!live_buffer->read_until || offset + want <= *live_buffer->read_until))
    {
        const size_t gap = offset - live_buffer->current_position;
        bool ready = gap == 0;
        if (gap > 0)
        {
            /// Skip the already-cached gap on the live connection. The bytes
            /// cross the wire (charged as over-read); only the source request
            /// is saved. A short skip means the source hit EOF inside the gap
            /// (unknown size) - the connection is spent, fall through to reopen.
            const size_t skipped = skipLiveBufferForward(gap);
            stats.bytes_from_source += skipped;
            stats.over_read_bytes += skipped;
            if (skipped == gap)
            {
                live_buffer->current_position = offset;
                ready = true;
            }
        }

        if (ready)
        {
            LOG_TRACE(log, "readFromSource: live buffer hit for {}, position={}", object.remote_path, offset);
            ProfileEvents::increment(ProfileEvents::LiveSourceBufferHits);

            Rope rope = readFromLiveBufferIntoRope(std::move(blocks), logical_offset);
            size_t total_read = rope.totalBytes();

            ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
            live_buffer->current_position += total_read;
            live_buffer->slot.updatePosition(live_buffer->current_position);
            releaseLiveBufferAtBound();
            return rope;
        }
    }

    if (live_buffer)
    {
        LOG_TRACE(log, "readFromSource: closing live buffer for {} (was at {}), need {}:{}",
            live_buffer->slot.objectPath(), live_buffer->current_position, object.remote_path, offset);
        maybeDrainLiveTail();
        accountLiveBufferDrop(/*at_eof=*/false);
        live_buffer.reset();
        inflight_segment_pin.reset();
    }

    /// The slot is decided by the caller (ensurePreAcquiredSlot): a held slot
    /// means an open-ended live buffer continued across windows; no slot means a
    /// bounded one-shot range read below. readFromSource never acquires one
    /// itself, so the window size (effectiveWindowSize) and the read shape agree.
    releaseStalePreAcquiredSlot(object.remote_path); // drop a slot pre-acquired for another object

    std::optional<SourceBufferSlot> slot;
    if (pre_acquired_slot && pre_acquired_slot->objectPath() == object.remote_path)
    {
        slot.emplace(std::move(*pre_acquired_slot));
        pre_acquired_slot.reset();
    }

    if (slot)
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

            live_buffer.emplace(LiveBuffer{
                .current_position = offset,
                .read_until = read_until,
                .buffer = std::move(opened),
                .slot = std::move(*slot),
            });
            ++stats.source_requests;

            Rope rope = readFromLiveBufferIntoRope(std::move(blocks), logical_offset);
            size_t total_read = rope.totalBytes();

            live_buffer->current_position += total_read;
            live_buffer->slot.updatePosition(live_buffer->current_position);

            ProfileEvents::increment(ProfileEvents::LiveSourceBufferCreated);
            ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
            LOG_TRACE(log, "readFromSource: opened live buffer for {}, read {} bytes, position={}",
                object.remote_path, total_read, live_buffer->current_position);
            releaseLiveBufferAtBound();
            return rope;
        }
    }

    /// No slot available — open a one-shot connection without storing it as
    /// `live_buffer`. Dropped when this function returns.
    ProfileEvents::increment(ProfileEvents::LiveSourceBufferFallbacks);

    auto opened = source->open(object);
    if (offset > 0)
        opened->seek(offset, SEEK_SET);

    /// No slot kept: bound the one-shot read so its connection is fully consumed
    /// and reusable by the pool, rather than abandoning an open-ended GET.
    const bool stateless_bounded = !hasUnknownSize() && opened->supportsRightBoundedReads() && want > 0;
    if (stateless_bounded)
        opened->setReadUntilPosition(offset + want);

    auto & buf = *opened;
    ++stats.source_requests;

    Rope rope;
    size_t total_read = 0;
    bool hit_eof = false;

    for (auto & block : blocks)
    {
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

    /// An unbounded one-shot GET (unknown-size source: the bound above was
    /// skipped) that did not reach EOF is dropped mid-response — not reusable.
    if (!stateless_bounded && !hit_eof)
        ++stats.incomplete_connections;

    return rope;
}

/// Assemble the bytes for `physical_window` from the cache chain, then the
/// source. Walks caches fastest-first, appending each tier's hits (clamped to
/// the request, deduped via `covered` so `result` stays disjoint even across
/// overlapping tiers) and propagating misses down; merges leftover misses into
/// fewer source reads; writes fetched bytes back into the missed segments.
/// While a live connection streams sequentially, pins the segment it will
/// continue into so a mid-read eviction can't reset it (Strategy A).
/// `from_prefetch` routes cache-populate bytes to the sync vs. async counter.
/// Returns one contiguous run from the window start (a hole would shift the
/// caller's offset interpretation).
Rope ReaderExecutor::readPhysicalWindow(ByteRange physical_window, bool from_prefetch)
{
    LOG_TRACE(log, "readPhysicalWindow [{}, {}) from_prefetch={}",
        physical_window.offset, physical_window.end(), from_prefetch);

    Rope result;
    IntervalSet covered;   /// disjoint logical bytes already materialised in `result`
    /// Both vectors live to scope exit so `~ICacheHandle` (the deferred LRU
    /// bump) runs AFTER every `put` below — see `~DiskCacheHandle`.
    VectorWithMemoryTracking<std::unique_ptr<ICacheHandle>> miss_handles;
    VectorWithMemoryTracking<std::unique_ptr<ICacheHandle>> hit_only_handles;

    VectorWithMemoryTracking<ByteRange> remaining = covered.subtract(physical_window);
    for (auto & cache : caches)
    {
        VectorWithMemoryTracking<ByteRange> still_missing;

        /// Attribute every byte this tier serves to the matching counter.
        size_t & hit_bytes = cache->tier() == CacheTier::PageCache
            ? stats.bytes_from_page_cache
            : stats.bytes_from_filesystem_cache;

        for (const auto & r : remaining)
        {
            /// Split `r` by object boundaries so each `cache->lookup` carries
            /// a single `StoredObject` and the provider can derive a per-object
            /// key / origin. Handles still report ranges in file-level
            /// coordinates — the per-object translation lives inside the provider.
            auto pieces = offset_map.map(r);
            size_t piece_file_start = r.offset;
            for (const auto & pr : pieces)
            {
                const size_t object_file_offset = piece_file_start - pr.object_offset;
                ByteRange piece_range{piece_file_start, pr.size};

                auto handle = cache->lookup(pr.object, object_file_offset, piece_range);
                auto status = handle->status();
                bool any_hit_done = false;

                /// Test hook: pause after a hit is classified but before `get`
                /// reads it, so a test can drop the cache in that window and
                /// verify the hit is still honored. No-op in production.
                if (!status.hit_ranges.empty())
                    FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_cache_status);

                for (const auto & hit : status.hit_ranges)
                {
                    LOG_TRACE(log, "readPhysicalWindow: cache {} hit [{}, {})",
                        cache->name(), hit.offset, hit.end());

                    /// `hit` is the cache's segment range; `piece_range` is
                    /// the actual request. Clamp `get` to the intersection to
                    /// avoid allocating buffer memory for segment bytes outside
                    /// the request. Misses stay segment-sized so the next layer
                    /// (or the source) can fully populate this cache via `put`.
                    size_t lo = std::max(hit.offset, piece_range.offset);
                    size_t hi = std::min(hit.end(), piece_range.end());
                    if (lo >= hi)
                        continue;
                    ByteRange clamped{lo, hi - lo};

                    auto useful = covered.subtract(clamped);
                    if (useful.empty())
                        continue;
                    ++stats.cache_get_requests;
                    StopwatchAccumulator get_scope(stats.cache_get_us);
                    Rope hit_rope = handle->get(clamped);
                    HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                        static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
                    for (const auto & sub : useful)
                    {
                        /// `status` promised this sub-range as a hit; `get` must
                        /// have returned it. If not, a held FileSegment was lost
                        /// between the two calls (the holder is supposed to keep
                        /// it non-releasable against eviction and DROP) — fail
                        /// loudly rather than silently mark uncovered bytes done.
                        if (!hit_rope.covers(sub))
                            throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "ReaderExecutor: cache {} status() reported a hit at [{}, {}) but get() did not "
                                "return it - a held FileSegment was not honored across status()/get()",
                                cache->name(), sub.offset, sub.end());
                        result.append(hit_rope.extract(sub));
                        covered.add(sub);
                        hit_bytes += sub.size;
                        if (from_prefetch)
                            stats.prefetch_issued_cache_bytes += sub.size;
                    }
                    any_hit_done = true;
                }

                for (const auto & miss : status.miss_ranges)
                {
                    LOG_TRACE(log, "readPhysicalWindow: cache {} miss [{}, {})",
                        cache->name(), miss.offset, miss.end());
                    still_missing.push_back(miss);
                }

                if (!status.miss_ranges.empty())
                    miss_handles.push_back(std::move(handle));
                else if (any_hit_done)
                    hit_only_handles.push_back(std::move(handle));

                piece_file_start += pr.size;
            }
        }

        remaining = std::move(still_missing);
        if (remaining.empty())
            break;
    }

    /// Merge close-together miss ranges into fewer source requests. The merge
    /// may bridge across already-covered hits — the latency saving outweighs
    /// the small extra bandwidth, and the overlap is dropped below when only
    /// the uncovered portion of each source range is appended to `result`.
    auto fetch_ranges = mergeRanges(remaining, min_bytes_for_seek);

    if (fetch_ranges.size() < remaining.size())
        LOG_TRACE(log, "readPhysicalWindow: merged {} miss ranges into {} fetch ranges (min_gap={})",
            remaining.size(), fetch_ranges.size(), min_bytes_for_seek);

    /// Sample the pressure-scaled block size ONCE per window, not per fetch
    /// range — `effectiveBlockSize` queries the memory-pressure monitor, and on
    /// a wide-table read thousands of concurrent executors would otherwise hit
    /// it per block. The value is stable across this call.
    const size_t window_block_size = effectiveBlockSize();

    for (const auto & fr : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(fr);
        size_t logical_pos = fr.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readPhysicalWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);

            /// Split at the user-window edges so user-data bytes (within
            /// the window) and head-extension bytes (segment-aligned miss
            /// heads before the window) live in separate `OwnedRopeBuffer`s.
            VectorWithMemoryTracking<size_t> splits;
            const size_t pr_lo = logical_pos;
            const size_t pr_hi = logical_pos + pr.size;
            if (physical_window.offset > pr_lo && physical_window.offset < pr_hi)
                splits.push_back(physical_window.offset - pr_lo);
            if (physical_window.end() > pr_lo && physical_window.end() < pr_hi)
                splits.push_back(physical_window.end() - pr_lo);
            std::sort(splits.begin(), splits.end());

            auto blocks = allocateBlocks(pr.size, window_block_size, splits);
            StopwatchAccumulator src_scope(stats.source_read_us);
            Rope source_rope = readFromSource(pr.object, pr.object_offset, std::move(blocks), logical_pos);
            HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
                static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
            size_t actual = source_rope.totalBytes();
            stats.bytes_from_source += actual;
            if (from_prefetch)
                stats.prefetch_issued_source_bytes += actual;
            /// Size-known short reads are fatal (the map promised those bytes;
            /// silently shrinking would shift later logical offsets). Size-unknown
            /// short reads are the only way to learn EOF — latch `reached_eof`.
            if (actual != pr.size)
            {
                if (!offset_map.hasUnknownSize())
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                        pr.object.remote_path, pr.object_offset, pr.size, actual);
                reached_eof = true;
            }

            /// Use `actual` so the recorded range tracks what the source
            /// actually delivered — diverges from `pr.size` only at EOF.
            ByteRange pr_range{logical_pos, actual};
            /// Over-read: fetched bytes that do not serve the requested window —
            /// head/tail alignment slack outside `physical_window`, plus bytes
            /// inside it already covered by an earlier hit (a mergeRanges bridged
            /// gap). Measured before `covered.add` below mutates the set.
            {
                const size_t lo = std::max(pr_range.offset, physical_window.offset);
                const size_t hi = std::min(pr_range.end(), physical_window.end());
                size_t needed = 0;
                if (hi > lo)
                    for (const auto & sub : covered.subtract(ByteRange{lo, hi - lo}))
                        needed += sub.size;
                stats.over_read_bytes += actual - needed;
            }
            auto uncovered = covered.subtract(pr_range);
            for (const auto & sub : uncovered)
            {
                result.append(source_rope.extract(sub));
                covered.add(sub);
            }

            logical_pos += pr.size;
        }
    }

    /// `result` is disjoint by construction (every append went through the
    /// `covered` guard), so each slice handed to `put` has at most one node
    /// per byte. The slice may be shorter than `miss.size` at EOF.
    for (auto & handle : miss_handles)
    {
        auto status = handle->status();
        for (const auto & miss : status.miss_ranges)
        {
            auto slice = result.slice(miss);
            if (slice.empty())
                continue;
            ++stats.cache_populate_requests;
            StopwatchAccumulator put_scope(stats.cache_populate_us);
            size_t & pushed_bytes = from_prefetch
                ? stats.bytes_pushed_to_cache_async
                : stats.bytes_pushed_to_cache_sync;
            pushed_bytes += handle->put(miss, std::move(slice));
            HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
                static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
        }
    }

    /// Cache-only window (no source read): the un-advanced live buffer fell
    /// behind the read frontier. Keep it while the next window start is still a
    /// bridgeable forward gap (<= min_bytes_for_seek) within its bound - the
    /// next miss will skip the cached gap and reuse the connection. Close it
    /// once it has fallen too far behind to bridge, so its slot isn't held idle.
    if (live_buffer && !reached_eof && fetch_ranges.empty())
    {
        const size_t next_physical = physical_window.end();
        size_t next_obj_file_offset = 0;
        const StoredObject * next_obj = offset_map.findObjectAt(next_physical, &next_obj_file_offset);
        const bool same_object = next_obj
            && live_buffer->slot.objectPath() == next_obj->remote_path;
        const size_t next_local = same_object ? next_physical - next_obj_file_offset : 0;
        const bool bridgeable = same_object
            && next_local >= live_buffer->current_position
            && next_local - live_buffer->current_position <= min_bytes_for_seek
            && (!live_buffer->read_until || next_local <= *live_buffer->read_until);
        if (!bridgeable)
        {
            maybeDrainLiveTail();
            accountLiveBufferDrop(/*at_eof=*/false);
            live_buffer.reset();
        }
    }

    /// Strategy A pin: re-point to the partial segment under the new frontier
    /// (dropping any previous pin); clear it when there's nothing partial.
    if (live_buffer && !reached_eof)
    {
        ICacheHandle::CacheSegmentPin pin;
        for (const auto & handle : miss_handles)
        {
            pin = handle->pinSegmentAt(physical_window.end());
            if (pin)
                break;
        }
        inflight_segment_pin = std::move(pin);

        /// Test hook: pause here while the in-flight segment is pinned and the
        /// live connection is open, so a test can drop/evict the cache and
        /// observe that the pinned segment survives. No-op unless enabled.
        if (inflight_segment_pin)
            FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_window);
    }
    else
    {
        inflight_segment_pin.reset();
    }

    /// Release a slot that never opened a connection (warm-cache window), else
    /// it lingers as a phantom `max_remote_read_connections` holder. (A promoted
    /// read already emptied `pre_acquired_slot` into `live_buffer.slot`.)
    if (pre_acquired_slot && !live_buffer)
        pre_acquired_slot.reset();

    auto sliced = result.slice(physical_window);

    /// Enforce the single-contiguous-run-from-the-window-start guarantee (may
    /// end early at EOF). A hole would misalign the caller's offsets.
    const auto & ivs = sliced.getIntervals();
    if (ivs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReaderExecutor::readPhysicalWindow: assembled result has {} disjoint intervals in window [{}, {}) - expected at most one contiguous run",
            ivs.size(), physical_window.offset, physical_window.end());
    if (!ivs.empty() && ivs[0].offset != physical_window.offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReaderExecutor::readPhysicalWindow: assembled result starts at {} but window begins at {} - missing prefix bytes",
            ivs[0].offset, physical_window.offset);
    return sliced;
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
        window_size, min_bytes_for_seek, block_size, log_file_path, max_tail_for_drain);

    t->buffer_limit = buffer_limit;

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
    /// `readBigAt` fans out concurrently over one parent; serialize the roll-up.
    std::lock_guard lock(transient_stats_mutex);
    stats += transient.stats;
}

}
