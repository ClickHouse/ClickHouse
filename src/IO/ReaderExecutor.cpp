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
#include <limits>

#include "config.h"

namespace ProfileEvents
{
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
    extern const Event LongConnectionOpened;
    extern const Event LongConnectionHits;
    extern const Event LongConnectionFallbacks;
    extern const Event LongConnectionBytes;
    extern const Event ReaderExecutorObservations;
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
#endif

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
        case LongConnectionOpened:      ProfileEvents::increment(ProfileEvents::LongConnectionOpened, value); break;
        case LongConnectionHits:        ProfileEvents::increment(ProfileEvents::LongConnectionHits, value); break;
        case LongConnectionFallbacks:   ProfileEvents::increment(ProfileEvents::LongConnectionFallbacks, value); break;
        case LongConnectionBytes:       ProfileEvents::increment(ProfileEvents::LongConnectionBytes, value); break;
        case Observations:              ProfileEvents::increment(ProfileEvents::ReaderExecutorObservations, value); break;
        case NumCounters:               break;
    }
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

/// The cooperative stop probe. The policy lives at the call sites: a LIVE
/// connection stops at the next block (it is saved with the machine and
/// continues from its frontier later - nothing is forfeited); a one-shot GET
/// is never cut mid-response (its request would be forfeited and the remainder
/// would pay a fresh one) - stateless fetches stop only BETWEEN connections.
static bool stopRequested(const MachineBase * stop)
{
    return stop && stop->interrupt_requested.load(std::memory_order_relaxed);
}

ReaderExecutor::Stats & ReaderExecutor::Stats::operator+=(const Stats & o)
{
    for (size_t i = 0; i < NumCounters; ++i)
        values[i] += o.values[i];
    return *this;
}

ReaderExecutor::StatTimer::StatTimer(Stats & stats_, Stats::Counter counter_)
    : target(stats_)
    , counter(counter_)
{
}

ReaderExecutor::StatTimer::~StatTimer()
{
    target.add(counter, watch.elapsedMicroseconds());
}

ReaderExecutor::FetchMachine::FetchMachine()
    : inflight_gauge(CurrentMetrics::ReaderExecutorPrefetchInFlight)
{
}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<IFileBasedSourceReader> source_,
    const StoredObjects & objects,
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches_,
    Options options)
    : source(std::move(source_))
    , stored_objects(objects)
    , caches(std::move(caches_))
    , log_file_path(std::move(options.log_file_path))
    , window_size(options.window_size)
    , min_bytes_for_seek(options.min_bytes_for_seek)
    , block_size(options.block_size)
    , max_tail_for_drain(options.max_tail_for_drain)
    , decrypt_ahead(options.decrypt_ahead)
    , plan_look_ahead_window(std::max(options.plan_look_ahead_window, options.window_size))
    , lookahead_window(options.lookahead_window)
    , prefetch_pool(std::move(options.prefetch_pool))
    , runner(prefetch_pool ? std::make_unique<FetchMachineRunner>(prefetch_pool) : nullptr)
    , long_connection_limit(std::move(options.long_connection_limit))
    , reader_executor_log(std::move(options.reader_executor_log))
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

    /// Keep the estimator's continuity gap in lockstep with the executor's seek
    /// bound, so a bridged gap feeds the same whether modeled as a read or a seek.
    ContinuityTracker::Options continuity_options;
    continuity_options.near_gap = min_bytes_for_seek;
    continuity_tracker = ContinuityTracker(continuity_options);

    /// The cache-read (served-pattern) estimator drives the residency lookup span.
    /// `near_gap == 0`: unlike the source estimator it must NOT bridge gaps - a hole
    /// in the served pattern breaks the held-reader run.
    ContinuityTracker::Options lookup_options;
    lookup_options.near_gap = 0;
    lookup_continuity = ContinuityTracker(lookup_options);
}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<IFileBasedSourceReader> source_,
    const StoredObjects & objects,
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches_)
    : ReaderExecutor(std::move(source_), objects, std::move(caches_), Options{})
{
}

ReaderExecutor::~ReaderExecutor()
{
    /// Cleanup, not a seek-away (not counted as a cancellation). The abandon slot is
    /// pre-reserved at machine-launch time, so stashing the in-flight machine here never
    /// allocates - safe from this `noexcept` destructor.
    cancelMachine(/*cancelled=*/false);
    drainAbandonedMachines(/*wait_finished=*/true);

    /// Account and release a still-held long connection abandoned at teardown.
    /// Never drain here - a source read can throw and this destructor is noexcept.
    if (long_conn)
    {
        accountLongDrop(long_conn, /*at_eof=*/false, stats);
        long_conn.reset();
    }

    /// A transient `readBigAt` executor rolls its stats into the parent via
    /// mergeTransientStats; emitting ProfileEvents / a reader_executor_log row
    /// here too would double-count. The parent's destructor reports the aggregate.
    if (is_transient)
        return;

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

/// One window of bytes, or empty at EOF. An in-flight prefetch is consumed
/// FIRST, before the EOF gate: an unknown-size worker can latch `reached_eof`
/// while still returning the file's final bytes, so gating first would drop
/// them. With no prefetch, read synchronously (or return empty at EOF).
/// Releases the live connection + slot once EOF is latched, then reads one
/// window ahead.
ChainedBuffers ReaderExecutor::readNextWindow()
{
    /// Total foreground time in the read call (planning, cache reads, source reads,
    /// prefetch waits) - the executor's direct contribution to query read latency.
    StatTimer work_timer(stats, Stats::WorkMicroseconds);

    const size_t logical_size = totalSize();

    /// EOF return - but a machine launched before EOF can have its worker latch
    /// `reached_eof` via a short read on an unknown-size source while still holding
    /// the final bytes. Defer the EOF return until that machine is collected in the
    /// gap branch below; only return here once nothing is in flight.
    if (atEnd() && !machine)
    {
        LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
        /// Drop the in-flight fill pin at EOF instead of waiting for the caller to
        /// drop the `PipelineReadBuffer`; a subsequent seek-back re-establishes it.
        inflight_segment_pin.reset();
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

    ChainedBuffers chain;

    /// Plan-first: build/refresh the geometry, then let the interpreter serve the
    /// cursor step - a resident run streamed from the held cache handle, or an
    /// in-flight / synchronous gap fetch.
    if (to_read > 0)
    {
        /// Re-plan only when the cursor leaves the planned span. The margin is the
        /// BASE `window_size` (a constant), so deciding whether to plan never queries
        /// memory pressure; the plan span and every read are clamped to `plan_end`,
        /// and the per-plan pressure level is sampled once inside `observeAndSchedule`.
        /// NB: never re-plan while a machine is in flight. A read-ahead is launched only
        /// at a gap cursor, so this cursor IS a gap and must be collected via the gap
        /// branch below. A re-plan here would re-probe residency and could see the
        /// worker's just-fetched gap as RESIDENT, wrongly taking the resident
        /// fast-path while `machine` is still set (the invariant the
        /// `observeAndSchedule` `chassert(!machine)` guards).
        if (!machine
            && (!read_plan.geometry()
                || position_phys < read_plan.geometry()->plan_start
                || (position_phys + window_size > read_plan.geometry()->plan_end && !planReachesEnd())))
        {
            observeAndSchedule(position_phys);
            reconstructCursor();
        }
    }

    /// Reset before the serve; `interpretStep` sets it only when it serves a window of
    /// decrypt-ahead bytes (already plaintext), so the boundary below skips decryption.
    served_window_is_plaintext = false;
    chain = interpretStep(position_phys, to_read);

    stats.add(Stats::RequestedBytes, chain.range().size);
    position += chain.range().size;
    /// Feed every forward serve - hit OR miss - to the look-ahead estimator. It sizes the
    /// plan / look-ahead window, which must grow on a sequential COLD read too (a cold scan
    /// has no hits, so a hit-only feed left the window pinned to one mark range). The window
    /// extends PAST `read_extent_end` on purpose: we plan and fetch ahead (whole cache
    /// segments) while the SERVE stays bounded to the extent by `clampToExtent`. `onServe`'s
    /// own contiguity test keeps a non-sequential (scattered) read from looking sequential.
    if (chain.range().size)
        lookup_continuity.onServe(position_phys, chain.range().size);
    advanceCursor();
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        chain.range().size, chain.getNodes().size(), position);

    /// Unknown-size EOF is latched by a short read here, not the pre-read gate,
    /// and the caller stops on the empty chain without a follow-up call - so drop
    /// the in-flight fill pin now rather than leaking it.
    if (reached_eof)
        inflight_segment_pin.reset();

    maybeLaunchAhead();

    /// A decrypt-ahead window is already plaintext (decrypted on the worker); every
    /// other window (cache hit, synchronous read) is ciphertext and decrypts here.
    if (served_window_is_plaintext)
        return chain;
    return decryptWindow(std::move(chain));
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
        reconstructCursor();  /// jumped within the surviving plan
        return;
    }

    cancelMachine(/*cancelled=*/true);

    const size_t new_physical = new_position + data_start_offset;
    /// Feed the seek to the continuity estimator and rewind the plan-feed watermark,
    /// so the post-seek plan re-feeds its predicted reads from here.
    continuity_tracker.onSeek(new_physical);
    lookup_continuity.onSeek(new_physical);
    continuity_fed_end = new_physical;

    /// A seek away from the current frontier strands the in-flight fill segment;
    /// drop its pin (the next window re-establishes it).
    inflight_segment_pin.reset();

    position = new_position;
    reached_eof = false;
    /// A jumped position invalidates the schedule-driven serve state: jobs banked AHEAD of
    /// the old cursor would, after the jump, leave `ready_bytes` disjoint from the new
    /// cursor (a foreground read below the ahead-bank makes a gappy chain). Drop the plan so
    /// the next launch re-plans + rebuilds `retrieve_status` from the new position. (The
    /// fast path above keeps the plan only for a forward seek into the in-flight window,
    /// where the bank stays contiguous.)
    read_plan = {};

    maybeLaunchAhead();
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

    /// An ADVANCE (or clear to EOF) does NOT invalidate an in-flight read-ahead: the
    /// long connection is opened PAST the current extent (`longConnectionBound` =
    /// `max(extent, reach)`), so it already covers the larger bound and keeps
    /// streaming - one GET spans the mark ranges. Cancelling here would reset that GET
    /// at every per-mark-range bound advance, forcing a fresh GET (and its S3
    /// first-byte) per range - the populate-path GET amplification. The machine reads
    /// against its immutable launch-time `extent_snapshot`, so updating the live bound
    /// cannot race it, and serving stays bounded by `clampToExtent` on the live
    /// `read_extent_end`. Only a backward SHRINK (which MergeTree never issues - see the
    /// assert above) would strand an over-reading prefetch past the new bound, so detach
    /// the machine just then.
    const bool advance_or_clear = !logical_end || (read_extent_end && *logical_end >= *read_extent_end);
    if (!advance_or_clear)
        cancelMachine(/*cancelled=*/true);
    read_extent_end = logical_end;
}

std::unique_ptr<ReaderExecutor> ReaderExecutor::makeTransientForReadAt(size_t start_position, size_t read_size) const
{
    /// `prefetch_pool` and `reader_executor_log` are intentionally NOT propagated:
    /// a one-shot `readBigAt` can't amortise prefetch latency, and per-call log rows
    /// would spam `system.reader_executor_log`. (Fills/promotes always run inline.)
    /// `long_connection_limit` is shared (dormant until the long-connection rework).
    Options transient_options;
    transient_options.window_size = window_size;
    transient_options.min_bytes_for_seek = min_bytes_for_seek;
    transient_options.block_size = block_size;
    transient_options.log_file_path = log_file_path;
    transient_options.max_tail_for_drain = max_tail_for_drain;
    transient_options.plan_look_ahead_window = plan_look_ahead_window;
    transient_options.long_connection_limit = long_connection_limit;
    auto t = std::make_unique<ReaderExecutor>(source, stored_objects, caches, std::move(transient_options));

#if USE_SSL
    t->decryptor = decryptor;
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

void ReaderExecutor::addDecryptionLayer(
    [[maybe_unused]] String path,
    [[maybe_unused]] size_t buffer_size,
    [[maybe_unused]] KeyFinderFunc key_finder)
{
#if USE_SSL
    decryptor.addLayer(std::move(path), buffer_size, std::move(key_finder));
    data_start_offset = decryptor.headerBytes();
    LOG_DEBUG(log, "Added decryption layer, data_start_offset={}", data_start_offset);
#endif
}

void ReaderExecutor::initDecryption()
{
#if USE_SSL
    if (decryptor.initialized() || decryptor.empty())
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

    LOG_DEBUG(log, "initDecryption: reading headers ({} bytes)", data_start_offset);

    /// No plan built yet at init time: pass an empty geometry so the header is read
    /// purely via the source/gap path. `serveLateHits` still serves a header byte already
    /// cached by a sibling reader (a read-only `planResidencyView` probe), but with no
    /// held write buffers the header itself is not populated here - it is read once and is
    /// tiny.
    CoverageMap init_geometry;
    ChainedBuffers header_chain = readPhysicalWindow(ByteRange{0, data_start_offset},
        init_geometry, reached_eof, stats);

    /// Under size-unknown sources `readPhysicalWindow` latches `reached_eof`
    /// on short returns instead of throwing, so an empty chain means
    /// "empty object" (same as the size-known empty branch above) and a
    /// partial chain means corrupted/truncated.
    if (offset_map.hasUnknownSize() && header_chain.totalBytes() == 0)
    {
        LOG_DEBUG(log, "initDecryption: unknown-size source returned 0 bytes (empty object), skipping");
        return;
    }
    if (header_chain.totalBytes() != data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source returned {} header bytes, expected {} (corrupted/truncated)",
            header_chain.totalBytes(), data_start_offset);

    decryptor.parseHeaders(header_chain);
#endif
}

void ReaderExecutor::decryptInPlace(
    [[maybe_unused]] char * data, [[maybe_unused]] size_t size, [[maybe_unused]] size_t logical_offset)
{
#if USE_SSL
    if (decryptor.empty() || size == 0)
        return;

    chassert(!decryptor.empty());
    StatTimer decrypt_scope(stats, Stats::DecryptMicroseconds);
    decryptor.decrypt(data, size, logical_offset);
#endif
}

ChainedBuffers ReaderExecutor::decryptWindow(ChainedBuffers && cipher)
{
    /// Without encryption (or without SSL) this short-circuits and the served
    /// window is returned untouched - zero-copy for the plaintext path.
    if (!needsDecryption() || cipher.empty())
        return std::move(cipher);

    ChainedBuffers plain;
    for (const auto & node : cipher.getNodes())
    {
        auto block = std::make_shared<OwnedChainedBuffer>(node.size);
        std::memcpy(block->data(), node.data(), node.size);
        decryptInPlace(block->data(), node.size, node.logical_offset);
        plain.append(ChainedBufferNode{block, 0, node.size, node.logical_offset});
    }
    return plain;
}

ChainedBuffers ReaderExecutor::decryptFetchedAhead([[maybe_unused]] const ChainedBuffers & cipher, [[maybe_unused]] Stats & timing_stats) const
{
    ChainedBuffers plain;
#if USE_SSL
    StatTimer decrypt_scope(timing_stats, Stats::DecryptMicroseconds);
    for (const auto & node : cipher.getNodes())
    {
        auto block = std::make_shared<OwnedChainedBuffer>(node.size);
        std::memcpy(block->data(), node.data(), node.size);
        /// `fetched` carries PHYSICAL offsets; the keystream is on logical payload
        /// offsets, so shift by the header size. Keep the PHYSICAL label on the copy
        /// so the collect path (slice / shift / finalize) treats it like `fetched`.
        decryptor.decrypt(block->data(), node.size, node.logical_offset - data_start_offset);
        plain.append(ChainedBufferNode{block, 0, node.size, node.logical_offset});
    }
#endif
    return plain;
}

size_t ReaderExecutor::totalSize() const
{
    size_t physical = offset_map.totalSize();
    return physical > data_start_offset ? physical - data_start_offset : 0;
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

        /// Strict `<`: a gap exactly `min_gap` wide is NOT bridged - reopening past it
        /// costs about the same as over-reading it, and if it is resident in a faster
        /// tier it is filled down from there rather than re-fetched.
        if (gap < min_gap)
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

ChainedBuffers ReaderExecutor::serveCacheBlock(size_t position_phys, size_t to_read)
{
    /// Stream the contiguous resident run straight from the plan's held (pinning) cache
    /// readers - no per-window discovery, no source. Serve each tier's range from its own
    /// reader, advancing the cursor so the appended runs stay disjoint; stop at the first
    /// gap (the next call serves it). A machine for a downstream gap may be in flight here
    /// (the resident/prefetch overlap): this path touches ONLY the caches and the (empty,
    /// moved-to-the-machine) foreground connection cluster, never the worker's machine.
    ChainedBuffers chain;

    /// Test hook: pause after the plan classifies this run as a hit but before the read, so
    /// a test can drop/evict the cache in that window and verify the plan-pinned segment
    /// survives. No-op in production.
    FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_cache_status);

    /// Serve a BLOCK at a time (not a full window): a cache hit has no remote open to
    /// amortise over a window, so block-sizing just bounds the in-flight ChainedBuffers memory per
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
        ChainedBuffers chunk = readHitFromView(*read_plan.bufs[run.entry].view, ByteRange{pos, serve_end - pos});
        const size_t got = chunk.range().size;
        if (got == 0)
            break;
        stats.add(Stats::CacheGetRequests);
        const bool is_page = run.tier == CacheTier::PageCache;
        stats.add(is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache, got);
        /// Promote this run up into any faster tier that misses it (no-op when served from
        /// the fastest tier or nothing faster populates), inline on the serve thread.
        maybePromote(run.tier, ByteRange{pos, got}, chunk, stats);
        chain.append(std::move(chunk));
        pos += got;
        if (pos < serve_end)
            break;
    }
    HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
        static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));

    if (data_start_offset)
        chain.shift(-static_cast<ssize_t>(data_start_offset));
    LOG_TRACE(log, "serveCacheBlock: streamed resident [{}, {}) from cache",
        position_phys, position_phys + chain.range().size);
    return chain;
}

bool ReaderExecutor::tryCollectMachine(ChainedBuffers & chain, bool & is_plaintext)
{
    is_plaintext = false;
    /// The worker may own the connection mid-read, so the revoke/release handoff
    /// must complete before any source touch.
    auto m = std::move(machine);
    /// The foreground holds no long connection while a machine is in flight - it was
    /// moved into the machine at launch; we reclaim it below.
    chassert(!long_conn);

    if (runner->tryCancelQueued(*m))
    {
        /// The worker never ran - the carried long connection is pristine; reclaim it
        /// so the synchronous read can continue it.
        long_conn = takeLong(m->long_conn);
        /// Still queued: revoke and let the caller read synchronously. Stash the
        /// machine - the pool's no-op pickup attaches a `ThreadGroupSwitcher`
        /// before checking cancellation, so ~ReaderExecutor must join it before
        /// our state is freed (a throw on the unwind would otherwise drop it
        /// un-joined; see `cancelMachine`).
        LOG_TRACE(log, "tryCollectMachine: prefetch was queued, cancelling and reading from position {}", position);
        stats.add(Stats::PrefetchCancelled);
        abandoned_machines.push_back(std::move(m));
        return false;
    }

    /// Started/finished: collect the worker's raw PHYSICAL gap bytes, then fold the
    /// machine-local source I/O into `this->stats`. Collect WAITS at the barrier -
    /// no takeover: a one-shot fetch has nothing to take over (the GET is read to
    /// its bound, and splitting it would forfeit the request). Interruption remains
    /// the CANCEL mechanism, where the remainder is never fetched at all.
    LOG_TRACE(log, "tryCollectMachine: waiting on prefetched [{}, {})", m->requested_range.offset, m->requested_range.end());
    StatTimer wait_scope(stats, Stats::PrefetchWaitMicroseconds);
    runner->waitReleased(*m);

    /// The fetch step failed: mandatory work, so the read fails. Keep the machine's
    /// issued-I/O counters before rethrowing - the bytes crossed the wire.
    if (m->failure)
    {
        stats += m->stats;
        std::rethrow_exception(m->failure);
    }

    /// The worker released the machine - reclaim the carried long connection (now
    /// advanced) so the next launch re-carries it (one GET across the run). Safe: the
    /// release edge has passed, so the worker no longer touches the payload.
    long_conn = takeLong(m->long_conn);

    const bool interrupted = m->state.load() == MachineState::Interrupted;
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
            ChainedBuffers assembled;
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
    /// back to the REQUESTED window and shift to logical. A partial chain is
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
    ChainedBuffers result;
    IntervalSet covered;
    backfillBytes(m->physical_window, requested_phys, m->fetched, result, covered,
        /*push_to_writers=*/false, stats);

    /// Decrypt-ahead: when the worker produced a plaintext copy that fully covers the
    /// served window, assemble the SERVED chain from it so the serve boundary skips
    /// `decryptWindow`; the cache-fill below still writes the ciphertext `result`. Fall
    /// back to serving ciphertext when the plaintext copy does not cover the window (a
    /// late cache hit can extend `result` past the fetched prefix the worker decrypted).
    ChainedBuffers served_plain;
    const bool serve_plaintext = !m->plaintext_fetched.empty()
        && m->plaintext_fetched.range().offset <= slice_window.offset
        && m->plaintext_fetched.range().end() >= slice_window.end();
    if (serve_plaintext)
    {
        IntervalSet covered_plain;
        Stats discard;  /// over-read already accounted on `result`'s backfill above
        assembleAndWriteBack(m->physical_window, requested_phys, m->plaintext_fetched,
            served_plain, covered_plain, /*push_to_writers=*/false, discard);
    }
    chain = finalizeAssembledWindow(slice_window, pin_frontier, serve_plaintext ? served_plain : result, reached_eof);
    is_plaintext = serve_plaintext;
    /// The deferred write side of this window: the put step takes the writers and
    /// the assembled chain to the background. After `finalizeAssembledWindow` - the
    /// pin was just taken from the plan's writers while they were still here.
    schedulePutStep(std::move(m), result);
    if (data_start_offset)
        chain.shift(-static_cast<ssize_t>(data_start_offset));

    /// A seek landed inside the fetched window: trim the prefix so `chain` starts at `position`.
    if (!chain.empty() && position > chain.range().offset)
    {
        const size_t end = chain.range().end();
        chain = chain.slice(ByteRange{position, end - position});
    }
    return true;
}

ChainedBuffers ReaderExecutor::syncGapRead(ByteRange physical_window)
{
    LOG_TRACE(log, "syncGapRead: synchronous gap read physical [{}, {})",
        physical_window.offset, physical_window.end());
    StatTimer sync_scope(stats, Stats::SyncReadMicroseconds);
    ChainedBuffers chain = readWindowLogical(physical_window, *read_plan.geometry(), reached_eof, stats);
    HistogramMetrics::ReaderExecutorSyncReadLatency.observe(
        static_cast<HistogramMetrics::Value>(sync_scope.elapsedMicroseconds()));
    return chain;
}

ChainedBuffers ReaderExecutor::readPhysicalWindow(ByteRange physical_window,
    const CoverageMap & geometry, bool & eof_latch, Stats & out_stats)
{
    LOG_TRACE(log, "readPhysicalWindow [{}, {})", physical_window.offset, physical_window.end());

    /// Foreground SYNCHRONOUS assembler: `initDecryption` (header) and the two sync gap
    /// reads in `readNextWindow`. `fetchAndBackfillGaps` re-credits grown committed
    /// prefixes, serves late hits, reads the still-missing ranges from the source, and
    /// pushes them into the plan's held write buffers. A prefetch worker never comes
    /// here: it runs the narrow `fetchGapsFromSource` over the plan gap the foreground
    /// bounded at submit, and the foreground backfills its bytes at consume.
    ChainedBuffers result;
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

    /// Serve resident bytes over the ALIGNED window: a byte that is a miss on the tier
    /// driving the alignment but resident on a faster tier is covered here, so the gap
    /// read below never re-fetches it.
    serveResidentFromPlan(fetch_window, result, covered, geometry, out_stats);
    /// The gap fill writes the cache inline on this read thread: the elected segment
    /// downloader must be the thread that writes+completes it, so the fill is never deferred.
    fetchAndBackfillGaps(
        fetch_window, physical_window, result, covered, eof_latch, geometry.pressure_level,
        /*push_to_writers=*/true, out_stats);

    return finalizeAssembledWindow(physical_window, fetch_window.end(), result, eof_latch);
}

ChainedBuffers ReaderExecutor::readWindowLogical(ByteRange physical_window,
    const CoverageMap & geometry, bool & eof_latch, Stats & out_stats)
{
    ChainedBuffers chain = readPhysicalWindow(physical_window, geometry, eof_latch, out_stats);
    /// Physical offsets include the encryption header prefix; the consumer works
    /// in logical (post-header) offsets. Shift once here. No-op when not encrypted.
    if (data_start_offset)
        chain.shift(-static_cast<ssize_t>(data_start_offset));
    return chain;
}

void ReaderExecutor::serveResidentFromPlan(
    ByteRange physical_window, ChainedBuffers & result, IntervalSet & covered,
    const CoverageMap & geometry, Stats & out_stats)
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
            ChainedBuffers resident_chain = readHitFromView(*read_plan.bufs[i].view, clamped);
            HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
            for (const auto & sub : useful)
            {
                /// The held read buffer pins resident segments, so a byte the plan
                /// reported resident MUST still be readable here. If not, the pin was
                /// not honored - fail loudly rather than drop bytes.
                if (!resident_chain.covers(sub))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ReaderExecutor: residency plan promised a hit at [{}, {}) but read() did not "
                        "return it - a pinned cache segment was not honored",
                        sub.offset, sub.end());
                result.append(resident_chain.extract(sub));
                covered.add(sub);
                out_stats.add(tier_counter, sub.size);
            }
        }
    }
}

/// Serve a clamped resident sub-range from a held `planResidencyView` view's hit read
/// buffers: find each `HitEntry` overlapping `clamped`, read the overlap from its
/// re-readable buffer (clamped to `readable()` so a partial prefix is never over-read),
/// and append the pieces. Returns the assembled (possibly short) ChainedBuffers; the caller checks
/// `covers`. Records each `read` on the view for the deferred LRU bump.
ChainedBuffers ReaderExecutor::readHitFromView(CacheView & view, ByteRange clamped)
{
    ChainedBuffers out;
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

void ReaderExecutor::serveLateHits(ByteRange window, ChainedBuffers & result, IntervalSet & covered, Stats & out_stats)
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
                        ChainedBuffers hit_chain = hit.reader->read(sub);
                        if (!hit_chain.covers(sub))
                            throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "ReaderExecutor: cache {} planResidencyView reported a late hit at "
                                "[{}, {}) but read() did not return it - a held FileSegment was not honored",
                                cache->name(), sub.offset, sub.end());
                        result.append(hit_chain.extract(sub));
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
    ChainedBuffers & result,
    IntervalSet & covered,
    bool & eof_latch,
    MemoryPressureLevel pressure_level,
    bool push_to_writers,
    Stats & out_stats)
{
    /// Synchronous foreground gap path: serve any grown committed prefix and late cache hit
    /// FIRST (so a concurrently/self-cached gap is served from cache, not re-fetched), then
    /// read the still-missing gaps of the ALIGNED `fetch_window` from the source and hand
    /// them to the shared `assembleAndWriteBack` (append + over-read + cache fill).
    recreditCommittedPrefixes(fetch_window, result, covered, out_stats);
    serveLateHits(fetch_window, result, covered, out_stats);
    VectorWithMemoryTracking<ByteRange> remaining = covered.subtract(fetch_window);

    /// Block size for the source-read tiles, from the per-plan cached pressure level.
    const size_t window_block_size = effectiveBlockSize(pressure_level);

    /// Read `ranges` from the source into `out_src` (merged by `min_bytes_for_seek` into one
    /// contiguous physical run each), in sub-spans bounded by a held long connection's
    /// `read_until`: each held GET drains EXACTLY to its bound (a clean release, not an
    /// incomplete drop) and the next sub-span reopens a fresh long connection - one GET per
    /// reach span. The long connection coalesces contiguous sub-spans across windows.
    auto fetch_into = [&](const VectorWithMemoryTracking<ByteRange> & ranges, ChainedBuffers & out_src)
    {
        for (const auto & fr : ranges)
        {
            auto physical_ranges = offset_map.map(fr);
            size_t logical_pos = fr.offset;
            for (const auto & pr : physical_ranges)
            {
                LOG_TRACE(log, "fetchAndBackfillGaps: source read object={}, offset={}, size={}",
                    pr.object.remote_path, pr.object_offset, pr.size);

                const size_t pr_obj_end = pr.object_offset + pr.size;
                size_t sub_obj_off = pr.object_offset;
                size_t sub_logical = logical_pos;
                while (sub_obj_off < pr_obj_end)
                {
                    /// W3: open/keep a long connection at the sub-span start when the run is long.
                    openLongIfWarranted(pr.object, sub_obj_off, sub_logical, out_stats);
                    size_t sub_end = pr_obj_end;
                    if (long_conn && long_conn->servesObject(pr.object.remote_path)
                        && long_conn->read_until > sub_obj_off && long_conn->read_until < pr_obj_end)
                        sub_end = long_conn->read_until;
                    const size_t sub_size = sub_end - sub_obj_off;

                    /// Split at the REQUESTED window edges so user-data bytes and segment-aligned
                    /// head/tail-extension bytes land in separate `OwnedChainedBuffer`s (released
                    /// independently).
                    VectorWithMemoryTracking<size_t> splits;
                    if (requested_window.offset > sub_logical && requested_window.offset < sub_logical + sub_size)
                        splits.push_back(requested_window.offset - sub_logical);
                    if (requested_window.end() > sub_logical && requested_window.end() < sub_logical + sub_size)
                        splits.push_back(requested_window.end() - sub_logical);
                    std::sort(splits.begin(), splits.end());

                    auto blocks = allocateBlocks(sub_size, window_block_size, splits);
                    StatTimer src_scope(out_stats, Stats::SourceReadMicroseconds);
                    ChainedBuffers sr = readFromSource(pr.object, sub_obj_off, std::move(blocks), sub_logical,
                        read_extent_end, &long_conn, /*stop=*/nullptr, out_stats);
                    HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
                        static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
                    const size_t actual = sr.totalBytes();
                    out_stats.add(Stats::BytesFromSource, actual);
                    out_src.append(std::move(sr));
                    /// Size-known short reads are fatal (the map promised those bytes).
                    /// Size-unknown short reads are how EOF is learned - latch it and stop.
                    if (actual != sub_size)
                    {
                        if (!offset_map.hasUnknownSize())
                            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                                "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                                pr.object.remote_path, sub_obj_off, sub_size, actual);
                        eof_latch = true;
                        break;
                    }
                    sub_obj_off = sub_end;
                    sub_logical += sub_size;
                }
                logical_pos += pr.size;
            }
        }
    };

    /// Deferred-write path (a cache-filler pool offloads the cache write to another thread):
    /// no per-segment downloader coordination here - the elected downloader must be the
    /// thread that writes+completes the segment, which is not this one when the write is
    /// deferred. Fetch the gaps plainly; the put step writes them later.
    if (!push_to_writers)
    {
        ChainedBuffers source_bytes;
        fetch_into(mergeRanges(remaining, min_bytes_for_seek), source_bytes);
        assembleAndWriteBack(fetch_window, requested_window, source_bytes, result, covered, push_to_writers, out_stats);
        return !remaining.empty();
    }

    /// Inline-write path: per-segment downloader arbitration. For each still-missing range,
    /// each overlapping write buffer elects the FileCache downloader of its segments: ones we
    /// win are fetched+written here, ones a sibling already leads are served from that
    /// sibling's cache fill instead of re-fetched - which dedups concurrent cold populate.
    /// Safe because we fetch AND complete each led segment on THIS thread (the downloader).
    VectorWithMemoryTracking<ByteRange> led_misses;
    VectorWithMemoryTracking<CacheWriter::SiblingLed> sibling_led;
    for (const auto & r : remaining)
    {
        bool elected = false;
        for (const auto & buf : read_plan.bufs)
        {
            if (!buf.provider)
                continue;
            for (const auto & w : buf.writers)
            {
                if (!w.writer)
                    continue;
                const size_t lo = std::max(r.offset, w.writer->range().offset);
                const size_t hi = std::min(r.end(), w.writer->range().end());
                if (lo >= hi)
                    continue;
                w.writer->electDownloaders(ByteRange{lo, hi - lo}, led_misses, sibling_led);
                elected = true;
            }
        }
        if (!elected)
            led_misses.push_back(r);  /// defensive: no writer → fetch as before
    }

    /// Coalesce the elected ranges into a DISJOINT set clamped to `fetch_window`: overlapping
    /// tier writers can elect the same bytes, and `mergeRanges` is a no-op at
    /// `min_bytes_for_seek == 0`, so without this the bytes would be fetched twice and the
    /// assembled chain would be non-disjoint. (Round-trip through `IntervalSet`: subtract the
    /// led set from the window to get the non-led parts, then subtract those to get the led.)
    IntervalSet led_set;
    for (const auto & r : led_misses)
        led_set.add(r);
    IntervalSet non_led;
    for (const auto & g : led_set.subtract(fetch_window))
        non_led.add(g);
    VectorWithMemoryTracking<ByteRange> led_disjoint = non_led.subtract(fetch_window);

    /// Pass 1: fetch+write the segments WE lead. Committing them unblocks sibling waiters.
    ChainedBuffers source_bytes;
    fetch_into(mergeRanges(led_disjoint, min_bytes_for_seek), source_bytes);
    assembleAndWriteBack(fetch_window, requested_window, source_bytes, result, covered, push_to_writers, out_stats);

    /// Serve sibling-led segments from cache AFTER our led writes (the write-before-wait
    /// order avoids a cross-thread deadlock); the wait blocks until the sibling commits.
    for (const auto & sl : sibling_led)
    {
        for (const auto & u : covered.subtract(sl.sub))
        {
            ChainedBuffers c = sl.writer->waitAndReadSiblingLed(u);
            if (!c.covers(u))
                continue;  /// tolerate a short commit; the loser-tail fallback below fetches it
            result.append(c.extract(u));
            covered.add(u);
            out_stats.add(Stats::BytesFromFilesystemCache, u.size);
        }
    }

    /// Loser-tail fallback: bytes a sibling leader committed short of what we need are still
    /// uncovered; fetch them from source (the leader reset the segment downloader on its
    /// `completePart`, so our write re-elects and wins the remainder). Rare when the leader
    /// fills the whole segment; required for correctness when it does not.
    auto remaining_tail = covered.subtract(fetch_window);
    if (!remaining_tail.empty())
    {
        ChainedBuffers tail_src;
        fetch_into(mergeRanges(remaining_tail, min_bytes_for_seek), tail_src);
        assembleAndWriteBack(fetch_window, requested_window, tail_src, result, covered, push_to_writers, out_stats);
    }

    return !led_misses.empty() || !remaining_tail.empty();
}

ChainedBuffers ReaderExecutor::fetchGapsFromSource(ByteRange physical_window, bool from_prefetch,
    bool & eof_latch, MemoryPressureLevel pressure_level, std::optional<size_t> read_extent,
    std::optional<LongConnection> * lc, const MachineBase * stop, Stats & out_stats)
{
    /// PURE source fetch: read the WHOLE window from the source as one contiguous
    /// physical run (short at EOF or at an interrupt point). No cache
    /// `lookup`/`get`/`put`, no plan - this is all a machine fetch step runs (it
    /// cannot touch shared cache/plan state), and the foreground reuses it before
    /// its own `backfillBytes`. The window is already clamped to one plan gap by
    /// the caller, so it never straddles a resident run; the cache backfill of
    /// these bytes is `backfillBytes`'s job.
    ChainedBuffers result;
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
        ChainedBuffers source_chain = readFromSource(pr.object, pr.object_offset, std::move(blocks), file_pos,
            read_extent, lc, stop, out_stats);
        HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
            static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
        const size_t actual = source_chain.totalBytes();
        out_stats.add(Stats::BytesFromSource, actual);
        if (from_prefetch)
            out_stats.add(Stats::PrefetchIssuedSourceBytes, actual);
        result.append(std::move(source_chain));
        file_pos += pr.size;

        /// The BETWEEN-CONNECTIONS stop point (and the post-hoc classifier for a
        /// live stop-short return): checked FIRST so a stop-short neither latches
        /// EOF (the bytes exist - the remainder is read by the normal dispatch)
        /// nor throws the size-known short-read error. For stateless fetches this
        /// is the ONLY stop point - the previous range's GET fully completed and
        /// the next one has not been opened, so stopping here is free.
        if (stopRequested(stop))
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
    ByteRange physical_window, ByteRange requested_window, const ChainedBuffers & source_bytes,
    ChainedBuffers & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats)
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
    const ChainedBuffers & source_bytes, ChainedBuffers & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats)
{
    /// Append the source bytes for the still-uncovered gaps of `fetch_window`, in offset
    /// order (assembly truth is the SOURCE ChainedBuffers, `[CF-contiguity]`). CLAMP every append to
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

ChainedBuffers ReaderExecutor::finalizeAssembledWindow(ByteRange slice_window, size_t pin_frontier, ChainedBuffers & result, bool eof_latch)
{
    /// Strategy A pin: re-point to the partial segment under `pin_frontier` - the frontier
    /// the read actually reached, which (with page-block alignment) can sit past
    /// `slice_window.end()`. This protects a still-being-filled cache segment from eviction
    /// across windows: a one-shot gap read in a sequential scan backfills a partial segment
    /// and the next window needs it intact. A `readBigAt` transient is excluded - it reads
    /// its bounded extent once and is destroyed, so pinning the partial segment it leaves
    /// serves nothing. `writerPinAt` returns the first held write buffer's `pin` (a bare
    /// FileSegmentPtr the buffer already owns) that passes the 3-part guard, empty
    /// otherwise; clear the pin at EOF.
    if (!eof_latch && !is_transient)
    {
        inflight_segment_pin = writerPinAt(pin_frontier);

        /// Test hook: pause here while the in-flight segment is pinned, so a test can
        /// drop/evict the cache and observe that the pinned segment survives. No-op
        /// unless enabled.
        if (inflight_segment_pin)
            FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_window);
    }
    else
    {
        inflight_segment_pin.reset();
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

void ReaderExecutor::pushAssembledToWriteBuffers(ByteRange physical_window, const ChainedBuffers & result, Stats & out_stats)
{
    /// Push the assembled `result`'s miss bytes into the plan's held write buffers,
    /// fire-and-forget: `result` is already assembled from the source ChainedBuffers + hit readers,
    /// so a short/zero `write` landing affects only `BytesPushedToCacheSync`, never
    /// `result` (`[CF-contiguity]`). Writes only into the authoritative `BufEntry::writers`
    /// (`chassert(writer)`), never the view's null-writer misses (`[CF-mutate]`). `result`
    /// is disjoint, so each slice has at most one node per byte (it may be short at EOF).
    /// This is the SYNCHRONOUS write side (the no-pool/sync paths); a machine collect
    /// defers the same work to a put step (`schedulePutStep`). Both honour the plan
    /// schedule's fill targets, so slack never reaches a faster tier.
    for (size_t i = 0; i < read_plan.bufs.size(); ++i)
        for (auto & w : read_plan.bufs[i].writers)
            if (w.writer && isScheduledFillTarget(physical_window, i, w.range))
                writeSliceToWriter(w.writer.get(), physical_window, result, Stats::BytesPushedToCacheSync, out_stats);
}

bool ReaderExecutor::isScheduledFillTarget(ByteRange window, size_t entry, ByteRange cell) const
{
    for (const auto & r : read_plan.schedule.retrieves)
    {
        if (!(r.range.offset < window.end() && window.offset < r.range.end()))
            continue;  /// retrieve does not cover this window
        for (const auto & t : r.into)
            if (t.entry == entry && t.cell.offset == cell.offset && t.cell.size == cell.size)
                return true;
    }
    return false;
}

void ReaderExecutor::writeSliceToWriter(CacheWriter * writer, ByteRange window, const ChainedBuffers & chain,
    Stats::Counter bytes_counter, Stats & out_stats)
{
    chassert(writer);
    /// Clamp the write target to the window's served portion and the buffer's own
    /// aligned range; the buffer further skips already-committed bytes internally
    /// (committed-set idempotency), so an out-of-order/overlapping slice from an
    /// interleaved promotion never double-counts.
    const size_t lo = std::max(writer->range().offset, window.offset);
    const size_t hi = std::min(writer->range().end(), window.end());
    if (lo >= hi)
        return;

    /// Write only the sub-ranges the chain actually COVERS. Under per-segment downloader
    /// coordination the assembled chain holds only the bytes THIS thread fetched (its led
    /// segments) plus cache hits - a sibling-led byte is written by the sibling, not by us -
    /// so the chain can cover `[lo, hi)` only partially. Slicing the whole `[lo, hi)` and
    /// writing it would hand the writer a non-covering chain (its `copyTo` asserts `covers`).
    /// A fully-covered window (the deferred/uncoordinated path) has no gaps, yielding the
    /// single sub-range `[lo, hi)`, so the behaviour is unchanged for it.
    const ByteRange target{lo, hi - lo};
    IntervalSet uncovered;
    for (const auto & gap : chain.gaps(target))
        uncovered.add(gap);
    for (const auto & covered_sub : uncovered.subtract(target))
    {
        auto slice = chain.slice(covered_sub);
        if (slice.empty())
            continue;
        out_stats.add(Stats::CachePopulateRequests);
        StatTimer put_scope(out_stats, Stats::CachePopulateMicroseconds);
        out_stats.add(bytes_counter, writer->write(std::move(slice)));
        HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
            static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
    }
}

void ReaderExecutor::pushChainToWriters(const VectorWithMemoryTracking<WriterView> & views, ByteRange window,
    const ChainedBuffers & chain, Stats::Counter bytes_counter, const std::atomic<bool> * interrupt, Stats & out_stats)
{
    for (const auto & view : views)
    {
        /// Cooperative stop point: stop between writers if interrupted, leaving the
        /// remaining ones untouched. (The inline fill never sets this - its
        /// `interrupt_requested` is cleared in `schedulePutStep`.)
        if (interrupt && interrupt->load(std::memory_order_relaxed))
            break;
        writeSliceToWriter(view.writer, window, chain, bytes_counter, out_stats);
    }
}

void ReaderExecutor::recreditCommittedPrefixes(
    ByteRange window, ChainedBuffers & result, IntervalSet & covered, Stats & out_stats)
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
                    ChainedBuffers chunk = w.writer->read(sub);
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

ChainedBuffers ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t logical_offset,
    std::optional<size_t> read_extent, std::optional<LongConnection> * lc,
    const MachineBase * stop, Stats & out_stats)
{
    /// One-shot source read: open a connection for this fetch range, bound it so it
    /// is fully consumed and returned to the pool reusable, read the blocks, and let
    /// it close on return. The HTTP pool still preserves the socket across reads; only
    /// the GET response stream is per-range - no stream is kept open across windows.
    size_t want = 0;
    for (const auto & block : blocks)
        want += block->size();

    /// Drain a held/carried long connection if it can serve this fetch contiguously
    /// within its bound. `lc` is the foreground's `long_conn` or the worker's machine
    /// payload, never the other's, so each thread drains only its own.
    ChainedBuffers head;  /// the prefix served from a held connection that drains to its bound mid-read
    if (lc && *lc)
    {
        if ((*lc)->servesObject(object.remote_path)
            && (*lc)->canContinue(offset, want, min_bytes_for_seek))
            return serveFromLong(*lc, offset, std::move(blocks), logical_offset, stop, out_stats);
        /// The read is forward-continuable from `offset` but CROSSES the channel bound. Serve the
        /// prefix up to `read_until` from the held connection - it drains exactly to its bound and
        /// releases clean - then read the remainder from a fresh GET below (the same request a
        /// reopen would cost, but the connection is no longer abandoned mid-run as an incomplete).
        /// Only split on a block boundary; if `read_until` does not land on one (rare - the reach
        /// is cache-aligned), or the connection cannot continue at all, drop and reopen.
        bool split = false;
        if ((*lc)->servesObject(object.remote_path)
            && offset >= (*lc)->current_position
            && (offset == (*lc)->current_position || offset - (*lc)->current_position < min_bytes_for_seek)
            && offset < (*lc)->read_until)
        {
            const size_t prefix_span = (*lc)->read_until - offset;
            size_t prefix_bytes = 0;
            size_t n = 0;
            while (n < blocks.size() && prefix_bytes + blocks[n]->size() <= prefix_span)
                prefix_bytes += blocks[n++]->size();
            if (prefix_bytes == prefix_span && n > 0)
            {
                VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> prefix;
                VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> suffix;
                for (size_t i = 0; i < blocks.size(); ++i)
                    (i < n ? prefix : suffix).push_back(std::move(blocks[i]));
                head = serveFromLong(*lc, offset, std::move(prefix), logical_offset, stop, out_stats);
                if (*lc)
                    return head;   /// EOF before the bound: the read ends here
                logical_offset += prefix_bytes;
                offset += prefix_span;   /// == read_until; continue with the suffix below
                want -= prefix_bytes;
                blocks = std::move(suffix);
                split = true;
            }
        }
        if (!split)
            dropLong(*lc, out_stats);
    }

    auto opened = source->open(object);
    if (offset > 0)
        opened->seek(offset, SEEK_SET);

    /// Bound the read so its connection is fully consumed and reusable by the pool,
    /// rather than abandoning an open-ended GET. The read consumes exactly `want`
    /// bytes, so bound to `offset + want` whenever the end is concrete - a known
    /// object size, or a finite advertised extent (`read_extent`) even when the size
    /// is unknown. Only a truly unbounded source (unknown size AND no advertised
    /// extent) is left open-ended.
    const bool stateless_bounded = opened->supportsRightBoundedReads() && want > 0
        && (!hasUnknownSize() || read_extent.has_value());
    if (stateless_bounded)
        opened->setReadUntilPosition(offset + want);

    auto & buf = *opened;
    out_stats.add(Stats::SourceRequests);

    ChainedBuffers chain = std::move(head);  /// the connection-served prefix, if the read was split at the bound
    size_t total_read = 0;
    bool hit_eof = false;

    for (auto & block : blocks)
    {
        /// No interrupt point: a one-shot GET, once issued, is read to its bound -
        /// cutting it mid-response would forfeit the request and make the remainder
        /// pay a fresh one. The stop lands BETWEEN connections (see
        /// `fetchGapsFromSource`), where nothing is in flight.
        size_t chunk = block->size();
        size_t got = readIntoBlock(buf, block->data(), chunk);

        if (got == 0)
        {
            hit_eof = true;
            break;
        }

        chain.append(ChainedBufferNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    /// A one-shot GET dropped before it was fully consumed is not pool-reusable:
    /// only the unbounded case (unknown size AND no advertised extent) that did not
    /// reach EOF can produce that, since bounded one-shots are read to their bound.
    /// Zero transfer means the lazy GET never started - nothing to count.
    if (!hit_eof && total_read > 0 && (!stateless_bounded || total_read < want))
        out_stats.add(Stats::IncompleteConnections);

    return chain;
}

VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> ReaderExecutor::allocateBlocks(
    size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits)
{
    chassert(block_size > 0);
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks;
    blocks.reserve((size + block_size - 1) / block_size + splits.size());

    size_t pos = 0;
    auto split_it = splits.begin();
    while (pos < size)
    {
        while (split_it != splits.end() && *split_it <= pos)
            ++split_it;

        const size_t boundary = (split_it != splits.end()) ? std::min(*split_it, size) : size;
        const size_t chunk = std::min(block_size, boundary - pos);
        blocks.push_back(std::make_shared<OwnedChainedBuffer>(chunk));
        pos += chunk;
    }
    return blocks;
}

// ─── Long connection ────────────────────────────────────────────────────────

ChainedBuffers ReaderExecutor::LongConnection::readInto(
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t logical_offset,
    const MachineBase * stop)
{
    ChainedBuffers chain;
    size_t total_read = 0;
    for (auto & block : blocks)
    {
        /// Stop BETWEEN blocks: a long connection stops freely - it stays put with
        /// its frontier and continues later, nothing forfeited.
        if (stopRequested(stop))
            break;
        const size_t got = readIntoBlock(*buffer, block->data(), block->size());
        if (got == 0)
            break;
        chain.append(ChainedBufferNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }
    current_position += total_read;
    return chain;
}

size_t ReaderExecutor::LongConnection::skipForward(size_t gap, size_t block_bytes)
{
    /// The source is in external-buffer mode, so discard through a scratch block
    /// (mirrors `readIntoBlock`): the bytes cross the wire (over-read) but the source
    /// request is saved. Short only at EOF.
    if (gap == 0)
        return 0;
    const size_t scratch_size = std::min(gap, block_bytes);
    auto scratch = std::make_shared<OwnedChainedBuffer>(scratch_size);
    size_t skipped = 0;
    while (skipped < gap)
    {
        const size_t got = readIntoBlock(*buffer, scratch->data(), std::min(gap - skipped, scratch_size));
        if (got == 0)
            break;
        skipped += got;
    }
    current_position += skipped;
    return skipped;
}

size_t ReaderExecutor::LongConnection::drainTail(size_t max_tail, size_t block_bytes)
{
    if (current_position >= read_until)
        return 0;
    const size_t tail = read_until - current_position;
    if (tail > max_tail)
        return 0;
    return skipForward(tail, block_bytes);
}

size_t ReaderExecutor::scheduleLookaheadReach(size_t phys_off) const
{
    /// How far a source connection opened at `phys_off` streams before a cached run forces a
    /// reopen: the plan's coverage walked forward, bridging resident runs strictly smaller than
    /// `min_bytes_for_seek` (the same strict-< rule `LongConnection::canContinue` applies on the
    /// open GET - the connection over-reads such a hole), stopping at the first run at/above the
    /// bound or the plan end. The single reach source for the connection bound: it reads only the
    /// plan geometry, so it is independent of how the schedule groups jobs.
    const auto & geom = read_plan.geometry();
    if (!geom)
        return phys_off;
    return geom->streamReach(phys_off, min_bytes_for_seek);
}

size_t ReaderExecutor::clampReach(size_t reach, size_t phys_off) const
{
    /// The estimator's reach is unclamped; bound it to the physical file end when the
    /// size is known (an unknown-size object has no end to clamp against).
    size_t end = phys_off + reach;
    if (!hasUnknownSize())
        end = std::min(end, totalSize() + data_start_offset);
    return end;
}

size_t ReaderExecutor::boundedReach(size_t phys_off) const
{
    /// The physical reach a long connection opened at `phys_off` actually gets, BEFORE any
    /// extent floor: the estimator's `predictedReach` clamped to the file end, then clamped
    /// DOWN at the next WIDE cached run the plan shows - a resident run at/above
    /// `min_bytes_for_seek` before `plan_end`, where the channel must stop (that region is
    /// served from cache / filled down, not over-read; holes strictly below the bound are
    /// bridged by `LongConnection::canContinue` on the open GET). A run cut by the plan
    /// boundary appears short here and is not a real stop, so the trajectory stays free to
    /// extend past the look-ahead. This is the SINGLE reach source shared by the open trigger
    /// (`shouldOpenLong`) and the channel bound (`longConnectionBound`), so the two can never
    /// disagree on how far the channel reaches. Reads only the tracker scalar + plan geometry.
    size_t reach = clampReach(continuity_tracker.predictedReach(), phys_off);
    const auto & geom = read_plan.geometry();
    if (geom)
    {
        const size_t wide = scheduleLookaheadReach(phys_off);
        const auto res = wide < geom->plan_end ? geom->residentAt(wide) : CoverageMap::Resident{};
        if (res.resident() && res.run_end - wide >= min_bytes_for_seek)
            reach = std::min(reach, wide);
    }
    return reach;
}

bool ReaderExecutor::shouldOpenLong(size_t phys_off) const
{
    /// Open a long connection when the estimator's predicted contiguous reach runs past
    /// the current read window - "a connection whose range exceeds the read window is
    /// long". Gated by the connection limit (the `reader_executor_use_long_connections`
    /// setting); suppressed under High/Critical pressure exactly where prefetch is.
    if (long_conn || !long_connection_limit)
        return false;
    const MemoryPressureLevel level
        = read_plan.geometry() ? read_plan.geometry()->pressure_level : MemoryPressureLevel::Normal;
    if (effectivePrefetchWindowSize(level) == 0)
        return false;
    /// Open when the forward reach runs past the current read extent - the right boundary
    /// where a short connection stops and the next read pays a fresh request. A long connection
    /// continues past it instead. The reach is `boundedReach` - the SAME value `longConnectionBound`
    /// sizes the channel with - so the trigger never opens a "long" channel the bound would then
    /// clamp back to the extent (a reverse/scattered pattern, or a run walled off by a near wide
    /// cached run, stays short). When no extent is advertised, fall back to one window.
    const size_t boundary = read_extent_end
        ? (*read_extent_end + data_start_offset)
        : (phys_off + effectiveWindowSize(level));
    return boundedReach(phys_off) > boundary;
}

size_t ReaderExecutor::longConnectionBound(const StoredObject & object, size_t object_offset, size_t phys_offset) const
{
    /// The channel bound, in object-local coordinates: the forward reach, floored at the
    /// current read extent and capped at the object end. The reach term lets a confirmed
    /// forward run extend the channel PAST the reader's current right boundary, so one GET
    /// spans several advancing mark ranges instead of reopening at each. The extent floor
    /// keeps a bounded read - one reverse chunk, or a run broken by a wide cached gap - from
    /// stranding the channel before its real end. The object end caps a GET to the single
    /// object it streams.
    ///
    /// The reach (`boundedReach`: `predictedReach` clamped at the next wide cached run) is the
    /// read's forward trajectory, which extrapolates past the current extent. It is the same
    /// value `shouldOpenLong` triggers on, so the GET drains cleanly at a wide cached run
    /// instead of being abandoned mid-run, and the trigger never opens a channel this bound
    /// would clamp back to the extent. Holes strictly below the bound are bridged by
    /// `LongConnection::canContinue` on the open GET.
    const size_t object_base = phys_offset - object_offset;
    const size_t object_end = hasUnknownSize()
        ? std::numeric_limits<size_t>::max()
        : object_base + object.bytes_size;
    const size_t extent = read_extent_end
        ? std::min<size_t>(*read_extent_end + data_start_offset, object_end)
        : object_end;
    const size_t reach = boundedReach(phys_offset);
    const size_t phys_bound = std::min(object_end, std::max(extent, reach));
    return phys_bound - object_base;
}

void ReaderExecutor::openLongIfWarranted(const StoredObject & object, size_t object_offset,
    size_t phys_offset, Stats & out_stats)
{
    /// Drop a held channel that cannot even START serving this fetch - wrong object, backward,
    /// a gap at/above `min_bytes_for_seek`, or already past its bound - before deciding to open,
    /// so a fresh channel covers the run from its first byte. A channel that CAN start serving
    /// is left for `readFromSource`, which serves up to the bound and reopens for any remainder;
    /// dropping it here would degrade the window to a one-shot and reopen only on the NEXT
    /// window, doubling the GET count of every cold run that follows a wide cached gap.
    if (long_conn && !(long_conn->servesObject(object.remote_path)
            && long_conn->canStartServing(phys_offset, min_bytes_for_seek)))
        dropLong(long_conn, out_stats);
    if (!shouldOpenLong(phys_offset))
        return;
    LongConnectionSlot slot = long_connection_limit->tryAcquire(long_connection_limit);
    if (!slot)
    {
        /// Wanted a long connection but the pool is at capacity - read a one-shot instead.
        out_stats.add(Stats::LongConnectionFallbacks);
        return;
    }
    openLong(long_conn, object, object_offset, longConnectionBound(object, object_offset, phys_offset),
        std::move(slot), out_stats);
}

void ReaderExecutor::openLong(std::optional<LongConnection> & conn, const StoredObject & object,
    size_t offset, size_t read_end, LongConnectionSlot slot, Stats & out_stats) const
{
    /// The foreground is the sole opener. Open a bounded GET over [offset, read_end) and
    /// store it; the first `readInto` issues the lazy request.
    auto opened = source->open(object);
    if (offset > 0)
        opened->seek(offset, SEEK_SET);
    if (opened->supportsRightBoundedReads())
        opened->setReadUntilPosition(read_end);

    conn.emplace(LongConnection{
        .buffer = std::move(opened),
        .object_path = object.remote_path,
        .opened_at = offset,
        .current_position = offset,
        .read_until = read_end,
        .slot = std::move(slot),
    });
    out_stats.add(Stats::SourceRequests);
    out_stats.add(Stats::LongConnectionOpened);
}

ChainedBuffers ReaderExecutor::serveFromLong(std::optional<LongConnection> & conn, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t logical_offset,
    const MachineBase * stop, Stats & out_stats) const
{
    /// Precondition: the caller has checked `servesObject` + `canContinue`.
    if (offset > conn->current_position)
    {
        /// Bridge the small forward gap by discarding it on the open stream: the
        /// bytes cross the wire (over-read) but the source request is saved.
        const size_t skipped = conn->skipForward(offset - conn->current_position, block_size);
        out_stats.add(Stats::BytesFromSource, skipped);
        out_stats.add(Stats::OverReadBytes, skipped);
    }
    /// The served bytes are counted as `BytesFromSource` by the caller (the returned
    /// chain), as on the one-shot path.
    ChainedBuffers chain = conn->readInto(std::move(blocks), logical_offset, stop);
    out_stats.add(Stats::LongConnectionHits);
    out_stats.add(Stats::LongConnectionBytes, chain.totalBytes());
    releaseLongAtBound(conn);
    return chain;
}

bool ReaderExecutor::maybeDrainLongTail(std::optional<LongConnection> & conn, Stats & out_stats) const
{
    if (!conn)
        return false;
    const size_t drained = conn->drainTail(max_tail_for_drain, block_size);
    out_stats.add(Stats::BytesFromSource, drained);
    out_stats.add(Stats::OverReadBytes, drained);
    return drained > 0 && !conn->atBound();
}

void ReaderExecutor::dropLong(std::optional<LongConnection> & conn, Stats & out_stats) const
{
    if (!conn)
        return;
    /// Drain a small tail so the connection returns to the pool reusable; the drain
    /// reports whether it ended short of the bound (EOF).
    const bool ended_at_eof = maybeDrainLongTail(conn, out_stats);
    accountLongDrop(conn, /*at_eof=*/ended_at_eof, out_stats);
    conn.reset();
}

void ReaderExecutor::accountLongDrop(const std::optional<LongConnection> & conn, bool at_eof, Stats & out_stats) const
{
    /// A connection dropped before it was fully consumed (not read to its bound or to
    /// EOF) is abandoned mid-response, not pool-reusable. One that never transferred
    /// is excluded: its lazy GET never started.
    if (conn && !conn->isComplete(at_eof) && conn->everTransferred())
        out_stats.add(Stats::IncompleteConnections);
}

void ReaderExecutor::releaseLongAtBound(std::optional<LongConnection> & conn) const
{
    if (conn && conn->atBound())
        conn.reset();
}

std::optional<ReaderExecutor::LongConnection> ReaderExecutor::takeLong(std::optional<LongConnection> & src)
{
    /// A plain `std::optional` move leaves the source engaged with a moved-from value,
    /// so reset it: the connection must be a single owner.
    std::optional<LongConnection> taken = std::move(src);
    src.reset();
    return taken;
}

void ReaderExecutor::collectFillTargets(FetchMachine & m)
{
    /// Record NON-OWNING views of the window's fill-target writers in the shared
    /// `read_plan.bufs`: the cells the plan SCHEDULE designates as fill targets for a retrieve
    /// overlapping this window (`buildSchedule`'s `into`). A cell holding the request is a
    /// target in every missing tier (promotion); a slack-only cell is a target ONLY in its
    /// owning lower tier - so a faster tier never receives un-requested slack bytes. Done at
    /// launch so the worker can write the led segments inline during its fetch; the writers
    /// stay in `read_plan.bufs` (the plan is stable while a machine is in flight).
    for (size_t i = 0; i < read_plan.bufs.size(); ++i)
    {
        auto & buf = read_plan.bufs[i];
        for (auto & w : buf.writers)
        {
            const bool overlaps_window = w.writer && w.range.offset < m.physical_window.end()
                && m.physical_window.offset < w.range.end();
            if (overlaps_window && isScheduledFillTarget(m.physical_window, i, w.range))
                m.writer_views.push_back({w.writer.get(), w.range});
        }
    }
}

void ReaderExecutor::schedulePutStep(std::shared_ptr<FetchMachine> m, const ChainedBuffers & assembled)
{
    /// `writer_views` were recorded at LAUNCH (`collectFillTargets`): NON-OWNING views of this
    /// window's fill-target writers in the shared `read_plan.bufs`, written in place on THIS
    /// read thread. Runs AFTER `finalizeAssembledWindow`, so the in-flight pin was taken first.
    if (m->writer_views.empty())
        return;  /// nothing to fill for this window

    /// Inline now: this fill writes synchronously on the read thread, so it credits the
    /// sync populate counter (the async counter is retired with the put lane).
    m->put_bytes_counter = Stats::BytesPushedToCacheSync;
    m->fill_chain = assembled;
    /// The machine is being re-armed for a second step: a takeover collect set
    /// `interrupt_requested` to stop the FETCH - the put must not inherit it.
    m->interrupt_requested.store(false);
    m->current_step.reset();
    m->put_wait.restart();
    m->run_step = [this, self = m.get()]
    {
        self->stats.add(Stats::PutWaitMicroseconds, self->put_wait.elapsedMicroseconds());
        const size_t fill_end = self->fill_chain.empty()
            ? self->physical_window.offset
            : std::min(self->physical_window.end(), self->fill_chain.range().end());
        pushChainToWriters(self->writer_views, self->physical_window, self->fill_chain,
            self->put_bytes_counter, &self->interrupt_requested, self->stats);
        /// Pin the partial segment under the just-written frontier until the
        /// reap (see `fill_pin`): the foreground's finalize pinned BEFORE this
        /// fill landed, so a fresh segment was not pinnable there.
        for (const auto & view : self->writer_views)
        {
            if (view.writer && fill_end >= view.writer->range().offset && fill_end < view.writer->range().end())
                if (auto pin = view.writer->pin(fill_end))
                {
                    self->fill_pin = std::move(pin);
                    break;
                }
        }
        self->fill_chain = {};
        return self->interrupt_requested.load() ? StepResult::Interrupted : StepResult::Done;
    };

    /// Run the fill inline on the read thread - no deferral. A failed fill is logged in
    /// `reapPutMachine`, never thrown: a read must not fail because cache population did.
    try
    {
        m->run_step();
    }
    catch (...)
    {
        m->failure = std::current_exception();
    }
    reapPutMachine(*m);
}

void ReaderExecutor::reapPutMachine(FetchMachine & m)
{
    /// The put wrote the shared `read_plan.bufs` writers in place (it held only
    /// non-owning views), so nothing comes home - just fold the pin, stats, and phase.

    /// Hand the put's Strategy-A pin to the foreground slot the pre-machine
    /// pin lived in: the fill landed, but its segment stays mid-stream until
    /// the scan passes it. Dropping the pin at reap let an eviction snap the
    /// next miss head back to the segment start - re-fetching aligned heads
    /// inflates R/O, worst on small-extent loads. Reaps run oldest-first, so
    /// the newest frontier wins; `finalizeAssembledWindow` keeps re-pointing /
    /// clearing it exactly as before.
    if (m.fill_pin)
        inflight_segment_pin = std::move(m.fill_pin);

    /// A failed put is logged, never thrown - a read must not fail because
    /// cache population failed.
    if (m.failure)
    {
        stats.add(Stats::PutFailed);
        tryLogException(m.failure, log, "Cache fill failed", LogsLevel::debug);
    }
    stats += m.stats;

    /// Ready -> Done: the job's bytes are now written into its `into[]` cells (this put
    /// committed). Only once the WHOLE job is fetched - a multi-window job reaps a put per
    /// window. `depsSatisfied` gates the launch of an offset-later same-cell write on this,
    /// so without it a deps-bearing job would never read ahead. The machine inherits
    /// `retrieve_index` from its launch; `schedulePutStep` keeps it for the fill.
    if (m.retrieve_index < read_plan.retrieve_status.size()
        && m.retrieve_index < read_plan.schedule.retrieves.size()
        && read_plan.retrieve_status[m.retrieve_index].fetched
            >= read_plan.schedule.retrieves[m.retrieve_index].range.size)
        read_plan.retrieve_status[m.retrieve_index].phase = RetrievePhase::Done;
}

void ReaderExecutor::maybePromote(CacheTier from_tier, ByteRange range, const ChainedBuffers & bytes, Stats & out_stats)
{
    /// Promote `bytes` up into the faster tiers that missed `range`, inline on the serve
    /// thread.
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

/// The index of the `schedule.steps` step whose `output` contains `pos_phys` (the live
/// serve cursor). Clamps to the last step past the materialized span (EOF / extent ceiling).
size_t ReaderExecutor::findStepContaining(size_t pos_phys) const
{
    const auto & steps = read_plan.schedule.steps;
    for (size_t i = 0; i < steps.size(); ++i)
        if (steps[i].output.offset <= pos_phys && pos_phys < steps[i].output.end())
            return i;
    /// Past the materialized span (EOF / extent ceiling): clamp to the last step.
    return steps.empty() ? 0 : steps.size() - 1;
}

void ReaderExecutor::reconstructCursor()
{
    if (!read_plan.geometry() || read_plan.schedule.steps.empty())
        return;
    read_plan.cursor = findStepContaining(position + data_start_offset);
}

void ReaderExecutor::advanceCursor()
{
    const size_t pos_phys = position + data_start_offset;
    auto & cursor = read_plan.cursor;
    const auto & steps = read_plan.schedule.steps;
    /// A single step (a whole resident run or a whole gap) can span several windows
    /// (`serveCacheBlock`/`coverWindow` sub-size by block/window), so advance to the
    /// step that now contains the position rather than incrementing once per window.
    while (cursor + 1 < steps.size() && steps[cursor].output.end() <= pos_phys)
        ++cursor;
}

// ─── Schedule-driven interpreter ──────────────────────────────────────────────
//
// `readNextWindow` runs the schedule's already-planned jobs instead of re-deriving
// the next gap from the coverage map. Two decoupled frontiers: the serve `cursor`
// (what the query reads) and the launch frontier (`retrieve_status[ri].fetched`,
// running one window ahead). ONE machine in flight - sequential serve is ordered, so a
// deeper read-ahead only trades memory for latency-hiding, and connection parallelism
// comes from multiple executors. Each remote job's collected bytes are banked in
// `retrieve_status[ri].ready_bytes` (LOGICAL coords, matching the I/O leaves' output, so
// banking needs no shift) and sliced per step; the long connection coalesces the GETs
// while each machine stays one window wide, so peak serve memory is ~one window per job.

bool ReaderExecutor::depsSatisfied(size_t ri) const
{
    /// An offset-earlier write into a shared append-only cell must be committed
    /// (its put reaped, phase Done) before this job's bytes land in the same cell.
    for (size_t dep : read_plan.schedule.retrieves[ri].deps)
        if (read_plan.retrieve_status[dep].phase != RetrievePhase::Done)
            return false;
    return true;
}

void ReaderExecutor::launchRetrieve(size_t ri)
{
    const auto & r = read_plan.schedule.retrieves[ri];
    auto & st = read_plan.retrieve_status[ri];
    const MemoryPressureLevel level = read_plan.geometry()->pressure_level;

    /// ONE window within the job range at its launch frontier - never `r.range` itself
    /// (a coalesced connection can be a whole column). The long connection keeps the GET
    /// open across these windows, so the job is still one GET.
    const size_t base = r.range.offset + st.fetched;
    const size_t chunk = std::min(r.range.end() - base, boundedReadSize(effectivePrefetchWindowSize(level)));
    if (chunk == 0)
        return;
    const ByteRange next_physical_window{base, chunk};

    auto m = std::make_shared<FetchMachine>();
    abandoned_machines.reserve(abandoned_machines.size() + 1);
    m->requested_range = ByteRange{base - data_start_offset, chunk};
    m->physical_window = next_physical_window;
    m->retrieve_index = ri;
    m->geometry = read_plan.geometry();
    m->extent_snapshot = read_extent_end;
    /// Record the fill-target writers now so the worker can write its led segments inline
    /// during the fetch step (the collect's `schedulePutStep` reuses these views).
    collectFillTargets(*m);

    /// The foreground is the sole opener; the aligned window's first physical range gives
    /// the object and its object-local offset. A no-op when not warranted / at capacity /
    /// a usable connection is already held. The channel bound comes from the runtime reach
    /// (`longConnectionBound`: `predictedReach` clamped at the next wide cached run), the same
    /// on the prefetch and foreground paths - the schedule no longer hands down a span.
    auto prefetch_ranges = offset_map.map(next_physical_window);
    if (!prefetch_ranges.empty())
        openLongIfWarranted(prefetch_ranges.front().object, prefetch_ranges.front().object_offset,
            next_physical_window.offset, stats);
    m->long_conn = takeLong(long_conn);

    m->run_step = [this, self = m.get()]
    {
        self->fetched = fetchGapsFromSource(
            self->physical_window, /*from_prefetch=*/true,
            self->reached_eof, self->geometry->pressure_level,
            self->extent_snapshot, &self->long_conn, self, self->stats);
        /// Decrypt-ahead: produce a plaintext copy on this worker thread so the serve
        /// boundary does not decrypt on the query thread. `fetched` stays ciphertext for
        /// the cache-fill. Reentrant - safe alongside a concurrent foreground decrypt.
        if (decrypt_ahead && needsDecryption() && !self->fetched.empty())
            self->plaintext_fetched = decryptFetchedAhead(self->fetched, self->stats);
        const size_t fetched_size = self->fetched.empty() ? 0 : self->fetched.range().size;
        const bool stopped_short = !self->reached_eof
            && fetched_size < self->physical_window.size
            && self->interrupt_requested.load();
        if (stopped_short)
        {
            self->stats.add(Stats::MachineInterrupted);
            return StepResult::Interrupted;
        }
        return StepResult::AwaitCollect;
    };

    if (!runner->schedule(m))
    {
        long_conn = takeLong(m->long_conn);
        stats.add(Stats::PrefetchPoolFull);
        return;
    }
    machine = std::move(m);
    st.phase = RetrievePhase::InFlight;
    st.machine = machine.get();
}

void ReaderExecutor::maybeLaunchAhead()
{
    if (!prefetch_pool || machine || atEnd())
        return;  /// one machine in flight (the cap for this stage)
    drainAbandonedMachines();

    const size_t position_phys = position + data_start_offset;
    const size_t probe = boundedReadSize(window_size);
    if (probe == 0)
        return;
    if (!read_plan.geometry() || !read_plan.geometry()->covers(ByteRange{position_phys, probe}))
    {
        observeAndSchedule(position_phys);
        reconstructCursor();
    }
    /// Fully cache-served plan: the look-ahead re-plan above has already pulled any
    /// upcoming cold region into the plan, so if there is still no `Source::Remote`
    /// retrieve there is nothing to prefetch - skip the rest of the bookkeeping.
    if (!read_plan.has_remote_retrieves)
        return;
    if (effectivePrefetchWindowSize(read_plan.geometry()->pressure_level) == 0)
        return;  /// read-ahead suppressed under High/Critical memory pressure

    auto & retrieves = read_plan.schedule.retrieves;
    auto & status = read_plan.retrieve_status;
    for (size_t ri = read_plan.launch_frontier; ri < retrieves.size(); ++ri)
    {
        const auto & r = retrieves[ri];
        /// Non-Remote jobs (fill/promote) are served from the cache side; a fully launched
        /// job is done. Advance the frontier past them so it never rescans.
        if (r.source != PlanSchedule::Source::Remote || status[ri].fetched >= r.range.size)
        {
            if (ri == read_plan.launch_frontier)
                ++read_plan.launch_frontier;
            continue;
        }
        if (!depsSatisfied(ri))
            return;  /// hold: an offset-earlier same-cell write is not committed yet
        launchRetrieve(ri);
        return;
    }
}

void ReaderExecutor::collectInFlightInto(size_t ri)
{
    auto & st = read_plan.retrieve_status[ri];
    /// The launch frontier advances by the requested window; short reads here are only
    /// EOF (no further launch) or a cancel/re-plan (which resets `fetched`), so the
    /// frontier never strands un-fetched bytes mid-plan.
    const size_t window = machine ? machine->physical_window.size : 0;
    ChainedBuffers collected;
    bool collected_is_plaintext = false;
    if (tryCollectMachine(collected, collected_is_plaintext))
    {
        /// `collected` is logical (the I/O leaf already shifted + sliced to `position`);
        /// `ready_bytes` is logical too, so bank it directly - no shift, no round-trip.
        if (!collected.empty())
        {
            /// `decrypt_ahead` is constant for an executor, so every collect for a given
            /// retrieve banks the same kind of bytes - the flag never flips mid-stream.
            chassert(st.ready_bytes.empty() || st.ready_bytes_is_plaintext == collected_is_plaintext);
            st.ready_bytes.append(std::move(collected));
            st.ready_bytes_is_plaintext = collected_is_plaintext;
        }
        st.fetched += window;
        st.phase = RetrievePhase::Ready;
        st.machine = nullptr;
    }
    else
    {
        /// Revoked while still queued: the foreground reads this window instead.
        st.phase = RetrievePhase::NotLaunched;
        st.machine = nullptr;
    }
}

ChainedBuffers ReaderExecutor::serveStepFromBanked(const PlanSchedule::Step & step, RetrieveStatus & st, size_t position_phys, size_t to_read) const
{
    /// All logical: `ready_bytes` and `position` are logical; the cursor step is physical,
    /// so shift its end by the header. No `ChainedBuffers` shift - only the bounds arithmetic.
    const size_t pos = position_phys - data_start_offset;
    const size_t step_end = step.output.end() - data_start_offset;
    const size_t end = std::min({step_end, pos + to_read, st.ready_bytes.range().end()});
    ChainedBuffers out = st.ready_bytes.slice(ByteRange{pos, end - pos});
    /// Release everything up to `end` (the served prefix + any skipped head) so the
    /// banked footprint stays ~one window.
    st.ready_bytes = st.ready_bytes.slice(ByteRange{end, st.ready_bytes.range().end() - end});
    return out;
}

ChainedBuffers ReaderExecutor::serveRetrieveForeground(size_t ri, size_t position_phys, size_t to_read)
{
    const auto & r = read_plan.schedule.retrieves[ri];
    auto & st = read_plan.retrieve_status[ri];
    const MemoryPressureLevel level = read_plan.geometry()->pressure_level;
    /// Read one window of THIS STEP only - never past the cursor step's end. A bridged
    /// retrieve can span several steps (an embedded faster-tier hit splits a gap into
    /// gap / hit / gap); reading to `r.range.end()` would bank past the hit and serve the
    /// hit + the next gap as one window, so the serve would no longer map 1:1 to the
    /// schedule. The long connection still coalesces ACROSS steps: it persists in
    /// `long_conn`, so the next gap step's source read continues it - skipping a small
    /// cached hole (`canContinue`) - or reopens past a hole at/above `min_bytes_for_seek`
    /// (the hole then filled down from the faster tier).
    const size_t step_end = read_plan.schedule.steps[read_plan.cursor].output.end();
    const size_t want = std::min({to_read, step_end - position_phys, boundedReadSize(effectiveWindowSize(level))});
    if (want == 0)
        return {};
    ChainedBuffers w = syncGapRead(ByteRange{position_phys, want});  /// returns logical, banked directly
    if (!w.empty())
        st.ready_bytes.append(std::move(w));
    /// The launch frontier is a high-water mark of bytes fetched from `r.range.offset`, NOT
    /// an accumulator: a foreground read at the cursor can land BELOW the frontier (the
    /// cursor trails an ahead launch), so `+= want` would over-count and make the next
    /// launch skip never-fetched bytes. Advance only if this read extends the frontier.
    st.fetched = std::max(st.fetched, (position_phys - r.range.offset) + want);
    st.phase = RetrievePhase::Ready;
    return serveStepFromBanked(read_plan.schedule.steps[read_plan.cursor], st, position_phys, to_read);
}

ChainedBuffers ReaderExecutor::serveRetrieveStep(const PlanSchedule::Step & step, size_t ri, size_t position_phys, size_t to_read)
{
    auto & st = read_plan.retrieve_status[ri];
    const size_t pos = position_phys - data_start_offset;  /// `ready_bytes` is logical
    /// Coverage-driven: a job can be partially banked AND still have a window in flight,
    /// so branch on "does `ready_bytes` cover the cursor?" rather than the phase alone.
    while (st.ready_bytes.empty()
        || pos < st.ready_bytes.range().offset
        || pos >= st.ready_bytes.range().end())
    {
        if (st.machine)
            collectInFlightInto(ri);  /// wait, bank one window, advance the frontier
        else
            return serveRetrieveForeground(ri, position_phys, to_read);  /// not prefetched: read it now
    }
    /// Banked bytes decrypted ahead on the worker are already plaintext, so the serve
    /// boundary must skip `decryptWindow` for this window.
    served_window_is_plaintext = st.ready_bytes_is_plaintext;
    return serveStepFromBanked(step, st, position_phys, to_read);
}

ChainedBuffers ReaderExecutor::serveHitStep(const PlanSchedule::Step & step, size_t position_phys, size_t to_read)
{
    /// A resident step: stream it from the held cache buffers, bounded to the step (the
    /// maximal cross-tier resident run) and to one block. Reuses the resident serve path.
    return serveCacheBlock(position_phys, std::min(to_read, step.output.end() - position_phys));
}

ChainedBuffers ReaderExecutor::handleExtentOrReplan(size_t position_phys, size_t to_read)
{
    /// The cursor ran past the materialized steps (or there is nothing to read). At a known
    /// end / the extent, this is EOF (empty chain). Otherwise re-plan from here and retry.
    if (to_read == 0 || atEnd())
        return {};
    if (!read_plan.geometry() || read_plan.cursor >= read_plan.schedule.steps.size())
    {
        observeAndSchedule(position_phys);
        reconstructCursor();
        if (read_plan.schedule.steps.empty())
            return {};
    }
    return interpretStep(position_phys, to_read);
}

ChainedBuffers ReaderExecutor::interpretStep(size_t position_phys, size_t to_read)
{
    if (to_read == 0 || !read_plan.geometry() || read_plan.schedule.steps.empty()
        || read_plan.cursor >= read_plan.schedule.steps.size())
        return handleExtentOrReplan(position_phys, to_read);

    const auto & step = read_plan.schedule.steps[read_plan.cursor];
    if (step.require_retrieve.has_value())
        return serveRetrieveStep(step, *step.require_retrieve, position_phys, to_read);
    return serveHitStep(step, position_phys, to_read);
}

void ReaderExecutor::observeAndSchedule(size_t physical_start)
{
    stats.add(Stats::Observations);
    /// Machine-check the threading invariant: the held read/write buffers are
    /// foreground-private and must never be torn down / rebuilt while a prefetch worker
    /// is in flight (the worker co-owns only the immutable geometry), so a segment is
    /// never aliased by a machine-held writer and a fresh `openWriteBuffers` of the next
    /// plan (`[CF-plan-rebuild]`). The cache fill is inline on the read thread, so there
    /// is nothing deferred to drain here.
    chassert(!machine);

    /// Reset the in-flight segment pin BEFORE tearing down the held buffers
    /// (`[CF-plan-rebuild]`): the pin aliases a held write buffer's own bare segment ref,
    /// so dropping it first makes `~DiskCacheWriter` the LAST owner and
    /// `FileSegment::complete` effective (otherwise a PARTIALLY_DOWNLOADED segment would
    /// stay un-shrunk and the next `openWriteBuffers` would alias the same segment in two
    /// buffers). The pin is re-established through the NEW buffer on the next
    /// `finalizeAssembledWindow`.
    inflight_segment_pin.reset();

    /// Release the PREVIOUS plan's held buffers FIRST: each held write buffer's
    /// destructor finalizes its segments (`FileSegment::complete`) and each `~CacheView`
    /// runs the deferred LRU-bump - AFTER those writes, since the bump is sequenced last
    /// in the view dtor. Foreground-timed (observeAndSchedule runs only after the
    /// in-flight prefetch is joined), so never concurrent with a worker.
    read_plan = {};

    /// Always publish a geometry (empty on the early-out paths below) so the query
    /// methods' callers never dereference a null snapshot: an empty geometry has
    /// `plan_end == plan_start`, so `covers` returns false and the caller re-plans.
    auto geom = std::make_shared<CoverageMap>();
    geom->plan_start = physical_start;
    geom->plan_end = physical_start;
    /// Sample memory pressure ONCE here, per plan. Every read within this plan (cache
    /// and remote, foreground and the prefetch worker via `job->pressure_level`) sizes
    /// off this cached level instead of re-querying the global monitor per call.
    geom->pressure_level = memoryPressureMonitor().currentLevel();

    /// TRIM: the plan span, bounded to the file end and the read extent. An empty
    /// span (the start already at/past a bound) publishes an empty plan.
    const ByteRange plan_range = boundedPlanSpan(physical_start, geom->pressure_level);
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
            geom_entry.whole_cell = cache->fillsWholeCell();
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

    /// Describe the plan's work once, here. The request for fill purposes is the
    /// whole plan span from the cursor: everything from `plan_start` forward is
    /// read by the scan (User), so only the alignment slack around it is
    /// FillOnly. `schedule.retrieves[*].into` then drives `schedulePutStep` so a
    /// faster tier never receives slack bytes (see `ReadPlan::schedule`).
    read_plan.schedule = buildSchedule(
        *read_plan.geometry(),
        ByteRange{plan_range.offset, plan_range.size},
        read_plan.geometry()->pressure_level,
        min_bytes_for_seek);

    /// Feed this plan's predicted source reads into the continuity estimator so its
    /// reach prediction (which sizes long source connections) stays current.
    feedScheduleToContinuity(read_plan.schedule);

    /// A plan with no `Source::Remote` retrieve is served entirely from cache; the
    /// prefetch look-ahead has nothing to launch.
    read_plan.has_remote_retrieves = std::any_of(
        read_plan.schedule.retrieves.begin(), read_plan.schedule.retrieves.end(),
        [](const auto & r) { return r.source == PlanSchedule::Source::Remote; });

    /// Allocate the per-job status sidecar 1:1 with the schedule's jobs. The
    /// schedule-driven processing loop branches on these phases instead of
    /// re-querying the coverage map. `cursor` is 0 from the fresh `ReadPlan` above.
    read_plan.retrieve_status.assign(read_plan.schedule.retrieves.size(), {});

    LOG_TRACE(log, "observeAndSchedule: planned [{}, {}), {} entries, {} retrieves",
        read_plan.geometry()->plan_start, read_plan.geometry()->plan_end,
        read_plan.geometry()->entries.size(), read_plan.schedule.retrieves.size());
}

void ReaderExecutor::feedScheduleToContinuity(const PlanSchedule & schedule)
{
    /// The predicted SOURCE reads are the `Source::Remote` retrieves; upper-tier
    /// reads and promotes open no source connection, so a wide upper hit between
    /// them correctly breaks the run. Feed in offset order, only past the
    /// watermark, so overlapping re-plans never double-feed.
    VectorWithMemoryTracking<ByteRange> source_reads;
    for (const auto & r : schedule.retrieves)
        if (r.source == PlanSchedule::Source::Remote)
            source_reads.push_back(r.range);
    std::sort(source_reads.begin(), source_reads.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });

    for (const auto & range : source_reads)
    {
        const size_t start = std::max(range.offset, continuity_fed_end);
        if (start >= range.end())
            continue;  /// already fed by an earlier (overlapping) plan
        continuity_tracker.onServe(start, range.end() - start);
        continuity_fed_end = range.end();
    }
}

bool ReaderExecutor::planReachesEnd() const
{
    return read_plan.geometry() && !offset_map.hasUnknownSize()
        && read_plan.geometry()->plan_end >= offset_map.totalSize();
}

ByteRange ReaderExecutor::boundedPlanSpan(size_t physical_start, MemoryPressureLevel level) const
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

    /// Decouple the residency LOOKUP span from the read-until bound. The held cache
    /// readers/view should live across mark ranges, not be rebuilt each time
    /// `read_extent_end` advances (per-mark-range churn that defeats reader reuse and
    /// is the warm-cache coordination cost). Cover at least the advertised extent (so
    /// the current task is served), then extend up to the forward-serve continuity
    /// prediction (`lookup_continuity`, fed by every forward serve - hit or miss), capped
    /// above by the look-ahead window / object end. The SERVE stays bounded by
    /// `read_extent_end` (`clampToExtent` / a machine's `extent_snapshot`), but the plan and
    /// fetch extend past it so a sequential read pre-fetches/caches ahead; a non-continuous
    /// read keeps `predictedReach` small, so the span falls back to the extent.
    if (read_extent_end)
    {
        const size_t physical_extent_end = *read_extent_end + data_start_offset;
        if (physical_start >= physical_extent_end)
            return ByteRange{physical_start, 0};
        const size_t extent_span = physical_extent_end - physical_start;
        /// Sequential (the run has already covered this extent): the read is streaming, so
        /// jump FLAT to the look-ahead cap - `predictedReach` only gates the decision, it does
        /// NOT size the span (sizing by it tracks the cursor and ramps too slowly to clear the
        /// rebuild margin). The cap is `lookahead_window`, halved per memory-pressure step and
        /// floored at `window_size`, so the held readers pin less under pressure. Not yet
        /// sequential: stay at the current extent (one mark range), as before.
        const size_t cap = std::max<size_t>(window_size, lookahead_window >> static_cast<unsigned>(level));
        const bool sequential = lookup_continuity.predictedReach() >= extent_span;
        const size_t reach = sequential ? cap : extent_span;
        want = std::min(want, std::max(extent_span, reach));
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

CacheWriter::CacheSegmentPin ReaderExecutor::writerPinAt(size_t frontier) const
{
    for (const auto & buf : read_plan.bufs)
        for (const auto & w : buf.writers)
            if (w.writer && frontier >= w.writer->range().offset && frontier < w.writer->range().end())
                if (auto pin = w.writer->pin(frontier))
                    return pin;
    return {};
}

void ReaderExecutor::cancelMachine(bool cancelled)
{
    drainAbandonedMachines();

    auto m = std::move(machine);
    if (!m)
        return;
    /// Clear the cancelled job's non-owning machine handle: it is live serve state. Without
    /// this, a later `serveRetrieveStep` would see a stale `st.machine` and collect a
    /// machine the foreground no longer owns (a null `shared_ptr` deref). The banked
    /// `ready_bytes`/`fetched` stay valid - the cursor has not moved (`setReadExtent`), or
    /// a seek re-plans and rebuilds them (see `seek`).
    if (m->retrieve_index < read_plan.retrieve_status.size())
        read_plan.retrieve_status[m->retrieve_index].machine = nullptr;
    /// The foreground holds no long connection while a machine is in flight (moved in
    /// at launch); a queued machine's pristine one is reclaimed below.
    chassert(!long_conn);

    LOG_TRACE(log, "Prefetch: discarding [{}, {})", m->requested_range.offset, m->requested_range.end());

    if (runner->tryCancelQueued(*m))
    {
        /// The worker never ran - reclaim the carried connection (pristine). A seek
        /// keeps it (the read funnel decides bridge-or-reopen later); the destructor
        /// accounts it if still held.
        long_conn = takeLong(m->long_conn);
        /// Revoked before the worker ran - count it like the readNextWindow
        /// revoke path (but not destructor cleanup, which passes `cancelled=false`) so
        /// `ReaderExecutorPrefetchCancelled` / `reader_executor_log.prefetch_cancelled`
        /// includes seek-cancelled read-aheads. Stats stay zero (worker never ran),
        /// so no merge.
        if (cancelled)
            stats.add(Stats::PrefetchCancelled);
        abandoned_machines.push_back(std::move(m));
    }
    else
    {
        /// Already running (or finished): SOFT cancel - flag the doomed work
        /// and stash the machine, with no foreground wait. The worker wraps at
        /// its next safe point; the sweep reaps it opportunistically and the
        /// destructor joins it hard. Its stats and wasted-bytes attribution are
        /// reconciled at the reap. The machine reads only its own snapshots
        /// (geometry, extent), so the foreground is free to re-plan or move the
        /// extent right away.
        stats.add(Stats::PrefetchDiscardedRunning);
        runner->requestInterrupt(*m);
        abandoned_machines.push_back(std::move(m));
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
                /// Join: cannot throw - a revoked handle resolves with a
                /// value, and step-body exceptions live in `m->failure`.
                m->current_step->get();
                if (m->failure)
                    tryLogException(m->failure, log, "Cancelled prefetch task threw", LogsLevel::debug);
                /// Reconcile the reaped machine: its fetch really happened, so
                /// merge the stats and attribute the issued bytes to wasted (the
                /// chain is never collected). A REVOKED machine no-ops every term:
                /// its stats are zero.
                stats += m->stats;
                stats.add(Stats::PrefetchWastedSourceBytes, m->stats.get(Stats::PrefetchIssuedSourceBytes));
                stats.add(Stats::PrefetchWastedCacheBytes, m->stats.get(Stats::PrefetchIssuedCacheBytes));
                /// Account the still-incomplete long connection and destroy it HERE, on
                /// the query-attached reaping thread, so its pool reset/expire events are
                /// attributed to this query: left to the machine's shared_ptr, the prefetch
                /// worker can win the last reference and free it after detaching, leaking
                /// `DiskConnectionsReset` off-query. Never drain (as `dropLong` does) - this
                /// is reachable from the noexcept destructor.
                accountLongDrop(m->long_conn, /*at_eof=*/m->reached_eof, stats);
                m->long_conn.reset();
                return true;
            }),
        abandoned_machines.end());
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

size_t ReaderExecutor::effectiveWindowSize(MemoryPressureLevel level) const
{
    /// Every source read is a one-shot, so each open amortises its setup over a full
    /// (pressure-scaled) window rather than a block.
    return sizesAtPressure(level, window_size, block_size).window_bytes;
}

size_t ReaderExecutor::effectiveBlockSize(MemoryPressureLevel level) const
{
    return sizesAtPressure(level, window_size, block_size).block_bytes;
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

}
