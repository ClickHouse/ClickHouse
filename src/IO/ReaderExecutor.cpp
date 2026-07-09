#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/FetchMachineRunner.h>
#include <IO/LocalFetchMachineRunner.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/HistogramMetrics.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <base/getThreadId.h>
#include <Interpreters/ReaderExecutorLog.h>
#include <chrono>
#include <limits>

#include "config.h"


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

/// ─────────────────────────────── FILE MAP ────────────────────────────────────
/// Regions in MODEL order (the two-cursors doc: ReaderExecutor.h class comment).
/// File order is historical; anchors are function names - grep, don't scroll.
///
/// CONSUMER (top of the model, inverted in file order):
///   `readNextWindow` -> `serveWindow` (the consumer loop) -> `pump` (in the
///   Schedule-driven interpreter region) -> `finishWindow`.
/// PLAN BUILD, three spans: `preparePlan` (the epoch scheduler, in Read path),
///   `mergeRanges`, and `observeAndSchedule` + its extract* helpers.
/// COLLECT, three spans: `collectInFlightInto`; the put pair `collectFillTargets` /
///   `runPutStep`; and teardown's `cancelMachine` / `drainAbandonedMachines`.
/// DISPLAY read surface, two spans: the `Display` methods, plus the plan-view
///   hit serve `readHitFromView` they join at `Display::read`.
/// PRODUCER: `coordinatedPrefetch` (machine fetch step) -> `fetchGapsFromSource`
///   -> `readFromSource` / the Long connection region; fills land through
///   `writeSliceToWriter`; deferred puts run at collect.
/// ──────────────────────────────────────────────────────────────────────────────


ReaderExecutorFetchMachine::ReaderExecutorFetchMachine()
    : inflight_gauge(CurrentMetrics::ReaderExecutorPrefetchInFlight)
{
}

// ─── Construction / teardown ───────────────────────────────────────────────

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
    , plan_look_ahead_max_window(options.plan_look_ahead_max_window)
    , long_connection_open_range(options.long_connection_open_range)
    , long_connection_max_bound(options.long_connection_max_bound)
    , fill_ahead_lead(options.fill_ahead_lead)
    , prefetch_pool(std::move(options.prefetch_pool))
    , runner(prefetch_pool ? std::make_unique<PoolFetchMachineRunner>(prefetch_pool) : nullptr)
    , local_runner(std::make_unique<LocalFetchMachineRunner>())
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
    ReadContinuityTracker::Options continuity_options;
    continuity_options.bridgeable_gap = min_bytes_for_seek;
    continuity_tracker = ReadContinuityTracker(continuity_options);
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
    if (fill_lane.conn)
    {
        accountLongConnectionDrop(fill_lane.conn, /*at_eof=*/false, stats);
        fill_lane.conn.reset();
    }

    /// A transient `readBigAt` executor rolls its stats into the parent via
    /// mergeTransientStats; emitting ProfileEvents / a reader_executor_log row
    /// here too would double-count. The parent's destructor reports the aggregate.
    if (is_transient)
        return;

    LOG_DEBUG(log,
        "Destroyed: from_page_cache={} from_filesystem_cache={} from_source={} "
        "pushed_to_cache_sync={} "
        "get_reqs={} populate_reqs={} src_reqs={} "
        "get_us={} populate_us={} src_us={} decrypt_us={} "
        "prefetch_wait_us={} sync_read_us={} work_us={} "
        "prefetch_hits={} prefetch_cancelled={} prefetch_pool_full={} "
        "prefetch_discarded_running={} "
        "prefetch_issued_source_bytes={} "
        "prefetch_wasted_source_bytes={} "
        "incomplete_connections={}",
        stats.get(Stats::BytesFromPageCache), stats.get(Stats::BytesFromFilesystemCache), stats.get(Stats::BytesFromSource),
        stats.get(Stats::BytesPushedToCacheSync),
        stats.get(Stats::CacheGetRequests), stats.get(Stats::CachePopulateRequests), stats.get(Stats::SourceRequests),
        stats.get(Stats::CacheGetMicroseconds), stats.get(Stats::CachePopulateMicroseconds),
        stats.get(Stats::SourceReadMicroseconds), stats.get(Stats::DecryptMicroseconds),
        stats.get(Stats::PrefetchWaitMicroseconds), stats.get(Stats::SyncReadMicroseconds), stats.get(Stats::WorkMicroseconds),
        stats.get(Stats::PrefetchHits), stats.get(Stats::PrefetchCancelled), stats.get(Stats::PrefetchPoolFull),
        stats.get(Stats::PrefetchDiscardedRunning),
        stats.get(Stats::PrefetchIssuedSourceBytes),
        stats.get(Stats::PrefetchWastedSourceBytes),
        stats.get(Stats::IncompleteConnections));

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
        elem.cache_get_requests = stats.get(Stats::CacheGetRequests);
        elem.cache_populate_requests = stats.get(Stats::CachePopulateRequests);
        elem.source_requests = stats.get(Stats::SourceRequests);
        elem.incomplete_connections = stats.get(Stats::IncompleteConnections);
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
        elem.prefetch_issued_source_bytes = stats.get(Stats::PrefetchIssuedSourceBytes);
        elem.prefetch_wasted_source_bytes = stats.get(Stats::PrefetchWastedSourceBytes);

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

// ─── Read path ─────────────────────────────────────────────────────────────

/// One window of bytes, or empty at EOF. At EOF an in-flight prefetch is drained FIRST: an
/// unknown-size worker can latch `reached_eof` on a short read while still holding the file's
/// final bytes, so reporting EOF before draining it would drop them. Only with nothing in
/// flight is EOF reported (releasing the fill pin). Otherwise the plan is brought up to date
/// and the position's window is served - a resident run streamed from the held cache handle, or an
/// in-flight / synchronous gap fetch.
ChainedBuffers ReaderExecutor::readNextWindow()
{
    /// Total foreground time in the read call (planning, cache reads, source reads,
    /// prefetch waits) - the executor's direct contribution to query read latency.
    StatTimer work_timer(stats, Stats::WorkMicroseconds);

    const size_t position_phys = toPhys(position);

    /// At EOF with no machine there is nothing left. A machine launched before EOF can have
    /// its worker latch `reached_eof` via a short read on an unknown-size source while still
    /// holding the final bytes - that last window drains through the normal serve below, and
    /// the next call finds no machine and returns empty here.
    if (atEnd() && !machine)
    {
        LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
        /// Drop the in-flight fill pin at EOF instead of waiting for the caller to drop the
        /// `PipelineReadBuffer`; a subsequent seek-back re-establishes it.
        fill_lane.pin.reset();
        return {};
    }

    /// `preparePlan` no-ops at the extent (`atExtent()`), where `serveWindow` then returns
    /// empty - the correct EOF for this extent.
    preparePlan(position_phys);
    return finishWindow(serveWindow(position_phys));
}

void ReaderExecutor::preparePlan(size_t position_phys, size_t coverage_ahead)
{
    /// At the read extent there is nothing left to (re)plan: `boundedPlanSpan` clamps to the
    /// extent, so a replan would only build an empty plan (and needlessly reset the in-flight
    /// pin). `serveWindow` then returns empty - the correct EOF for this extent; a later
    /// `setReadExtent` re-plans from the new bound. (There is no machine to collect here either:
    /// fetches are extent-clamped, so by the extent any machine's range is already served.)
    if (atExtent())
        return;

    const bool at_plan_end = read_plan.geometry() && position_phys >= read_plan.geometry()->plan_end;

    /// At the boundary, collect the in-flight machine BEFORE replanning. A consumed step at
    /// `plan_end` serves nothing, so the serve's gap branch never collects it; without this the
    /// replan stays blocked (machine still set -> `observeAndSchedule`'s `chassert(!machine)`)
    /// and the executor stalls at `plan_end` - a premature interior EOF. Collecting commits the
    /// machine's cells (so the replan below sees them resident) and clears `machine`.
    if (machine && at_plan_end)
        collectInFlightInto(machine->retrieve_index);

    /// The consumer/producer role split is the declaration's contract; the mechanics here:
    /// the post-collect gap above is the ONE instant a mid-plan replan is legal, a replan
    /// never runs while a machine is in flight (it would re-probe residency and could see
    /// the worker's just-fetched gap as resident), and the per-plan pressure level is
    /// sampled once inside `observeAndSchedule`.
    const bool want_replan = coverage_ahead
        ? (!read_plan.geometry() || !read_plan.geometry()->covers(ByteRange{position_phys, coverage_ahead}))
        : (!read_plan.geometry()
            || position_phys < read_plan.geometry()->plan_start
            || (at_plan_end && !planReachesEnd()));
    if (!machine && want_replan)
    {
        observeAndSchedule(position_phys);
    }
}

ChainedBuffers ReaderExecutor::finishWindow(ChainedBuffers chain)
{
    stats.add(Stats::RequestedBytes, chain.range().size);
    position += chain.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        chain.range().size, chain.getNodes().size(), position);

    /// Unknown-size EOF is latched by a short read here, not the pre-read gate, and the caller
    /// stops on the empty chain without a follow-up call - so drop the in-flight fill pin now
    /// rather than leaking it.
    if (reached_eof)
        fill_lane.pin.reset();

    advanceAhead();

    /// THE consumer exit: the whole serve machinery works in physical coordinates;
    /// this one shift rebases the window to logical for the decryptor (CTR position
    /// = payload offset) and the caller.
    if (data_start_offset)
        chain.shift(-static_cast<ssize_t>(data_start_offset));
    return decryptWindow(std::move(chain));
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_DEBUG(log, "seek to {}, current position={}", new_position, position);

    /// The machine's requested LOGICAL range is its `physical_window` shifted by the header.
    const size_t requested_logical_offset = machine ? toLogical(machine->physical_window.offset) : 0;
    const size_t requested_logical_end = machine ? toLogical(machine->physical_window.end()) : 0;
    if (machine
        && new_position >= requested_logical_offset
        && new_position < requested_logical_end)
    {
        LOG_TRACE(log, "seek: target within prefetch [{}, {}), keeping prefetch",
            requested_logical_offset, requested_logical_end);
        position = new_position;
        return;
    }

    cancelMachine(/*cancelled=*/true);

    const size_t new_physical = toPhys(new_position);
    /// Feed the seek to the continuity estimator and rewind the plan-feed watermark,
    /// so the post-seek plan re-feeds its predicted reads from here.
    continuity_tracker.recordSeek(new_physical);
    continuity_fed_end = new_physical;

    /// A seek away from the current frontier strands the in-flight fill segment;
    /// drop its pin (the next window re-establishes it).
    fill_lane.pin.reset();

    position = new_position;
    reached_eof = false;
    /// A jumped position invalidates the plan epoch: bytes banked AHEAD of the old cursor
    /// would, after the jump, sit disjoint from the new one. Drop the plan AND the lane's
    /// epoch state here - by ownership, not by trusting the next replan to reset it (the
    /// executor can go idle at EOF holding a stale bank otherwise). (The fast path above
    /// keeps the plan, the cursor, and the bank for any target inside the in-flight window -
    /// a backward one re-serves committed cells, which the serve reads ahead-cursor-blind.)
    read_plan = {};
    fill_lane.resetEpoch();

    advanceAhead();
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

// ─── Transient reads (readBigAt) ───────────────────────────────────────────

std::unique_ptr<ReaderExecutor> ReaderExecutor::makeTransientForReadAt(size_t start_position, size_t read_size) const
{
    /// `prefetch_pool` and `reader_executor_log` are intentionally NOT propagated:
    /// a one-shot `readBigAt` can't amortise prefetch latency, and per-call log rows
    /// would spam `system.reader_executor_log`. (Fills/promotes always run inline.)
    /// `long_connection_limit` is shared so transient reads honour the server-wide cap.
    Options transient_options;
    transient_options.window_size = window_size;
    transient_options.min_bytes_for_seek = min_bytes_for_seek;
    transient_options.block_size = block_size;
    transient_options.log_file_path = log_file_path;
    transient_options.max_tail_for_drain = max_tail_for_drain;
    transient_options.plan_look_ahead_max_window = plan_look_ahead_max_window;
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

// ─── Decryption ────────────────────────────────────────────────────────────

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

    ChainedBuffers header_chain = fetchEncryptionHeader();

    /// Under size-unknown sources `fetchGapsFromSource` latches `reached_eof`
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

ChainedBuffers ReaderExecutor::fetchEncryptionHeader()
{
    /// The encryption headers are ordinary file bytes and belong in the caches like any
    /// others: every executor over the file needs them, and - more important - they are
    /// the FIRST bytes of the first cache cell. Data writes start at `data_start_offset`
    /// and an append-only cell can never fill a hole below its write frontier, so a
    /// header that bypassed the cache would leave the first cell's prefix permanently
    /// uncommitted and the first data window uncacheable.
    const ByteRange header_range{0, data_start_offset};

    /// The headers sit at the front of the FIRST object; the exotic layout where that
    /// object is shorter than the headers falls back to the direct source read.
    auto pieces = offset_map.map(header_range);
    const bool cacheable = !caches.empty() && pieces.size() == 1;

    /// ONE residency probe per tier (the probe/open decomposition the provider API is
    /// built around): a tier holding the full header serves it right here - the view is
    /// destroyed after the read, running its deferred LRU bump. Otherwise the view is
    /// KEPT when the tier can take the header prefix, so the post-fetch populate reuses
    /// its aligned misses instead of re-probing. Whole-cell tiers (page cache) cannot
    /// take a header-sized prefix by construction.
    VectorWithMemoryTracking<std::pair<ICacheProvider *, CacheViewPtr>> populate_views;
    if (cacheable)
    {
        for (auto & cache : caches)
        {
            auto view = cache->planResidencyView(pieces.front().object, /*object_file_offset=*/0, header_range);
            if (view->allHit())
            {
                ChainedBuffers chain;
                for (const auto & hit : view->hits())
                {
                    if (!hit.reader)
                        continue;
                    const size_t lo = std::max(hit.range.offset, header_range.offset);
                    const size_t hi = std::min({hit.range.end(), header_range.end(), hit.reader->readable()});
                    if (lo >= hi)
                        continue;
                    chain.append(hit.reader->read(ByteRange{lo, hi - lo}));
                }
                if (chain.covers(header_range))
                {
                    stats.add(Stats::CacheGetRequests);
                    stats.add(cache->tier() == CacheTier::PageCache ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache,
                        data_start_offset);
                    LOG_DEBUG(log, "initDecryption: headers served from cache {}", cache->name());
                    return chain.slice(header_range);
                }
            }
            if (cache->populatesOnMiss() && !cache->fillsWholeCell() && !view->misses().empty())
                populate_views.emplace_back(cache.get(), std::move(view));
        }
    }

    /// Miss: one one-shot source read (no long connection, no plan exists yet).
    ChainedBuffers fetched = fetchGapsFromSource(header_range,
        /*from_prefetch=*/false, reached_eof, MemoryPressureLevel{}, /*read_extent=*/data_start_offset,
        /*lc=*/nullptr, /*stop=*/nullptr, /*may_open_long=*/false, stats);

    /// Populate the incrementally-fillable tiers so the first cell's append-only prefix
    /// commits and the following data writes can continue from `data_start_offset`.
    if (fetched.totalBytes() == data_start_offset)
    {
        for (auto & [cache, view] : populate_views)
        {
            VectorWithMemoryTracking<ByteRange> miss_ranges;
            for (const auto & m : view->misses())
                miss_ranges.push_back(m.range);
            for (auto & m : cache->openWriteBuffers(pieces.front().object, /*object_file_offset=*/0, miss_ranges))
            {
                if (!m.writer)
                    continue;
                auto fill_claim = m.writer->claim(header_range);
                stats.add(Stats::CachePopulateRequests);
                StatTimer put_scope(stats, Stats::CachePopulateMicroseconds);
                stats.add(Stats::BytesPushedToCacheSync,
                    m.writer->write(fetched.slice(header_range)));
            }
        }
    }
    return fetched;
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
        decryptInPlace(block->data(), node.size, node.offset);
        plain.append(ChainedBufferNode{block, 0, node.size, node.offset});
    }
    return plain;
}

size_t ReaderExecutor::totalSize() const
{
    size_t physical = offset_map.totalSize();
    return physical > data_start_offset ? physical - data_start_offset : 0;
}

// ─── Plan build (cont. at observeAndSchedule) ──────────────────────────────

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

// ─── Window serve path - collect ────────────────────────────────────────────

void ReaderExecutor::collectInFlightInto(size_t ri)
{
    const auto & r = read_plan.schedule.retrieves[ri];
    /// The worker may own the connection mid-read, so the revoke/release handoff
    /// must complete before any source touch.
    auto m = std::move(machine);

    if (collectRunner().tryCancelQueued(*m))
    {
        /// The worker never ran - the carried long connection is pristine; reclaim it
        /// so the synchronous read can continue it.
        fill_lane.reclaim(*m);
        /// Still queued: revoke and let the caller read synchronously. Stash the
        /// machine - the pool's no-op pickup attaches a `ThreadGroupSwitcher`
        /// before checking cancellation, so ~ReaderExecutor must join it before
        /// our state is freed (a throw on the unwind would otherwise drop it
        /// un-joined; see `cancelMachine`).
        LOG_TRACE(log, "collect: prefetch was queued, cancelling and reading from position {}", position);
        stats.add(Stats::PrefetchCancelled);
        abandoned_machines.push_back(std::move(m));
        return;
    }

    /// Started/finished: collect the worker's raw PHYSICAL gap bytes, then fold the
    /// machine-local source I/O into `this->stats`. Collect WAITS at the barrier -
    /// no takeover: a one-shot fetch has nothing to take over (the GET is read to
    /// its bound, and splitting it would forfeit the request); a stall-join interrupts
    /// first, so the wait is bounded by one tile.
    LOG_TRACE(log, "collect: waiting on prefetched [{}, {})",
        toLogical(m->physical_window.offset), toLogical(m->physical_window.end()));
    StatTimer wait_scope(stats, Stats::PrefetchWaitMicroseconds);
    collectRunner().waitReleased(*m);

    /// The fetch step failed: mandatory work, so the read fails. Keep the machine's
    /// issued-I/O counters before rethrowing - the bytes crossed the wire. A POOL machine's
    /// carried connection dies with it (its mid-read state is not reusable), but it is no
    /// longer LENT - the lane may open a fresh one. An INLINE step read through the LANE's
    /// slot: a mid-read throw leaves that connection wire-desynced (`current_position` is
    /// advanced only after the block loop), and continuing it would serve bytes from the
    /// wrong offset - destroy it.
    if (m->failure)
    {
        stats += m->stats;
        if (m->inline_serve && fill_lane.conn)
        {
            accountLongConnectionDrop(fill_lane.conn, /*at_eof=*/false, stats);
            fill_lane.conn.reset();
        }
        fill_lane.conn_lent = false;
        std::rethrow_exception(m->failure);
    }

    /// The worker released the machine - reclaim the carried long connection (now
    /// advanced) so the next launch re-carries it (one GET across the run). Safe: the
    /// release edge has passed, so the worker no longer touches the payload.
    fill_lane.reclaim(*m);

    const bool interrupted = m->state.load() == MachineState::Interrupted;
    /// Reconcile the worker's one-way EOF latch - ONLY here (its bytes are kept); the
    /// cancel paths must not, or a wasted read-ahead's EOF strands us at false EOF.
    /// (An interrupt-short return never latches it - see `fetchGapsFromSource`.)
    reached_eof |= m->reached_eof;
    stats += m->stats;
    HistogramMetrics::ReaderExecutorPrefetchWaitLatency.observe(
        static_cast<HistogramMetrics::Value>(wait_scope.elapsedMicroseconds()));

    /// Sparse fetch: the worker led only some segments (a sibling leads the rest), so `fetched`
    /// has holes. Its led bytes are already written to cache, so REVOKE to the synchronous path
    /// rather than assemble a possibly non-contiguous window here. The sync read re-serves the
    /// worker's led bytes as cache hits and elects/waits on the sibling-led ones through the
    /// proven foreground coordination - and never trips the single-contiguous-run guard (the
    /// sparse assembly tripped it on seek / partial / multi-tier patterns). Uncontended windows
    /// (no sibling) keep the direct collect below.
    if (m->contended)
        return;

    const size_t fetched_end = std::max(m->fetched_end, m->physical_window.offset);

    if (interrupted)
    {
        /// An interrupted step that produced nothing degrades to the revoke path:
        /// the connection is reclaimed (above), the caller reads synchronously.
        if (m->fetched.empty() && fetched_end <= m->physical_window.offset)
            return;

        /// A fetched prefix that cannot serve the cursor (extension-only bytes below the
        /// requested range, or a kept seek moved past it) is already in its cells (the
        /// worker commits per tile); retry only the refused residue into the writers,
        /// then let the caller read synchronously - serving an empty window here would
        /// read as a false EOF upstream.
        if (toLogical(fetched_end) <= position)
        {
            runPutStep(m, m->fetched);
            return;
        }
        stats.add(Stats::PartialCollects);
    }
    else if (!m->inline_serve)
    {
        /// A prefetch hit is a POOL machine's chain served in full; an inline
        /// (serve-thread) machine is a plain synchronous fetch, not a prefetch.
        stats.add(Stats::PrefetchHits);
    }

    /// The worker committed its led bytes per tile, so the collect has no window to
    /// assemble - the display serves the cells. What remains here: pin the in-flight
    /// segment at the frontier the fetch actually reached (an interrupted or
    /// residue-capped step stops short of the window end), retry the refused residue
    /// into the writers (a role a sibling held at fetch time may be free now; evicted
    /// space may have opened), and hand the caller what is STILL homeless - the bank is
    /// its only route to the display.
    if (!reached_eof && !is_transient)
    {
        fill_lane.pin = writerPinAt(std::min(m->physical_window.end(), fetched_end));

        /// Test hook: pause here while the in-flight segment is pinned, so a test can
        /// drop/evict the cache and observe that the pinned segment survives. No-op
        /// unless enabled.
        if (fill_lane.pin)
            FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_window);
    }
    else
        fill_lane.pin.reset();

    ChainedBuffers residue = std::move(m->fetched);
    runPutStep(m, residue);   /// `m` stays alive - the still-refused scan below reads its views

    /// The still-refused residue, sliced exactly-once per uncommitted sub-range (the put
    /// above may have landed parts of it). A bypass window has no views, so everything
    /// it fetched comes back - the bank is its transport.
    ChainedBuffers collected;
    if (!residue.empty())
        for (const auto & still : uncommittedIn(m->writer_views, residue.range()))
            collected.append(residue.slice(still));

    /// A seek landed inside the fetched window: trim the consumed prefix away.
    const size_t position_phys = toPhys(position);
    if (!collected.empty() && position_phys > collected.range().offset)
    {
        const size_t end = collected.range().end();
        collected = collected.slice(ByteRange{position_phys, end - position_phys});
    }

    /// A populatable retrieve's committed bytes live in its cells - the display IS its
    /// data progress, and the serve reads them back from the cache (the cache is the
    /// buffer). What remains is a bypass window's transport (`r.into.empty()`) or a
    /// populating window's STILL-REFUSED residue; bank both - the bank is their only
    /// route to the display. Refused residue also raises the launch backpressure: no
    /// new lead until the serve consumed it.
    if (!collected.empty())
    {
        if (r.into.empty())
            fill_lane.bank.append(std::move(collected));
        else if (!display.covers(collected.range()))
        {
            fill_lane.bank.append(std::move(collected));
            fill_lane.bank_refused = true;
        }
    }
    /// The lane's ahead cursor: attempted through what the fetch actually REACHED. A full
    /// fetch (or one that saw EOF) attempted the window end to end; an interrupted or
    /// residue-capped one stopped at `fetched_end` - its tail was never attempted and the
    /// launcher may relaunch it.
    fill_lane.advanceAttempted(
        (m->reached_eof || fetched_end >= m->physical_window.end())
            ? m->physical_window.end()
            : fetched_end);
}

// ─── The display (cont.): plan-view hit serve ──────────────────────────────

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

// ─── Machine fetch step ────────────────────────────────────────────────────

void ReaderExecutor::coordinatedPrefetch(FetchMachine & m)
{
    const ByteRange window = m.physical_window;
    const MemoryPressureLevel level = m.pressure_snapshot;
    m.fetched_end = window.offset;

    /// `m.fetched` hands the accumulated bytes to the collect whatever way this scope exits
    /// (a cancel mid-loop, an EOF short, a source exception). Declared BEFORE the claims so
    /// the claims' destructors - the downloader release - run FIRST on scope exit: the
    /// collect must never observe bytes whose segments this thread still holds DOWNLOADING.
    ChainedBuffers led_bytes;
    SCOPE_EXIT_SAFE({ m.fetched = std::move(led_bytes); });

    /// Claim the FileCache downloader roles over the window's fill-target writers (recorded
    /// at launch). Runs WE win (`to_fetch`) this worker fetches+writes inline below while the
    /// claims stay open - it is the downloader, on THIS thread. Runs a sibling already leads
    /// go to `sibling_led`: the worker SKIPS them (the foreground, seeing `m.contended`,
    /// revokes to the sync path at collect, which serves them from the sibling's fill), which
    /// dedups concurrent cold populate - each segment is fetched once across executors. The
    /// claims' destructors complete-and-release exactly the roles won here, so an interrupted
    /// fetch can never leak a segment DOWNLOADING (which would abort the writer's holder dtor
    /// on `chassert(!is_last_holder)` - the foreground teardown cannot reset a foreign
    /// downloader).
    VectorWithMemoryTracking<ByteRange> led_misses;
    VectorWithMemoryTracking<ByteRange> sibling_led;
    VectorWithMemoryTracking<CacheWriter::FillClaim> claims;
    for (const auto & view : m.writer_views)
    {
        if (!view.writer)
            continue;
        const size_t lo = std::max(window.offset, view.writer->range().offset);
        const size_t hi = std::min(window.end(), view.writer->range().end());
        if (lo >= hi)
            continue;
        auto fill_claim = view.writer->claim(ByteRange{lo, hi - lo});
        for (const auto & r : fill_claim.to_fetch)
            led_misses.push_back(r);
        for (const auto & sl : fill_claim.sibling_led)
            sibling_led.push_back(sl);
        claims.push_back(std::move(fill_claim));
    }
    /// Record contention for the collect side (the led set itself stays worker-local: the
    /// foreground only needs to know WHETHER a sibling lead exists, to revoke to the sync path).
    m.contended = !sibling_led.empty();

    /// "Stop at the first loss" (inline serve only): bound the fetch at the first sibling-led
    /// segment so this thread fetches just the contiguous LED PREFIX `[window.offset, fetch_bound)`.
    /// The serve reads that prefix (we are its downloader, so it is committed to our cells) as a
    /// short window; the caller's next read resolves the sibling boundary (claim/wait). A led
    /// segment past the boundary was claimed above but is NOT fetched here - its claim's
    /// destructor resets the downloader. A pool worker leaves `fetch_bound` at the window end (it fetches the
    /// whole led set and revokes on contention at collect), keeping its read-ahead behavior.
    size_t fetch_bound = window.end();
    if (m.inline_serve)
        for (const auto & sl : sibling_led)
            fetch_bound = std::min(fetch_bound, sl.offset);

    /// A window byte that no fill-target writer covers cannot be deduped (no cache tier
    /// populates it): fetch it plainly. Add the window remainder (minus elected + sibling-led)
    /// to the led set.
    IntervalSet elected;
    for (const auto & r : led_misses)
        elected.add(r);
    for (const auto & sl : sibling_led)
        elected.add(sl);
    for (const auto & g : elected.subtract(window))
        led_misses.push_back(g);

    /// Coalesce the led ranges into a DISJOINT set clamped to the window (overlapping tier
    /// writers can elect the same bytes; round-trip through `IntervalSet`).
    IntervalSet led_set;
    for (const auto & r : led_misses)
        led_set.add(r);
    IntervalSet non_led;
    for (const auto & g : led_set.subtract(window))
        non_led.add(g);
    VectorWithMemoryTracking<ByteRange> led_disjoint = non_led.subtract(window);

    /// Fetch the led runs on this worker thread via the machine's own connection, then write
    /// them inline (we hold the claims). A sibling-led hole between two led runs breaks the
    /// connection - correct, those bytes are not ours to fetch.
    ///
    /// Fill the LEAD progressively: the BACKGROUND run-ahead tiles each led run into window-sized
    /// pieces and COMMITS each as it lands (`pushChainToWriters` per tile), so a concurrent
    /// foreground serve sees the committed prefix grow and reads it while this worker keeps fetching
    /// ahead - the lead is one GET (the long connection persists across the tiles). Without per-tile
    /// commit that serve would block on the whole lead. The INLINE serve runs this fetch on the
    /// serve thread itself (fetch-then-serve), so there is no concurrent reader to hand a growing
    /// prefix to: it fetches each led run in ONE source read - one GET on the stateless arm (tiling
    /// would issue one GET per window) - and commits it whole.
    ///
    /// `led_bytes` retains ONLY the RESIDUE - what no cell accepted (a refused write, a
    /// sibling-claimed cell; everything on a bypass window). Committed bytes live in the cells
    /// and the serve reads them there; holding them again would double the lead's memory. The
    /// residue is CAPPED at one (pressure-scaled) window: when nothing commits - cache full,
    /// read-only tier, no cache at all - the lead stops early instead of ballooning in memory,
    /// degrading to the old one-window cadence (the collect banks the residue; the launcher
    /// holds a refused bank until the serve consumes it).
    const size_t residue_cap = std::max<size_t>(effectiveWindowSize(level), 1);
    bool residue_full = false;
    for (const auto & led : mergeRanges(led_disjoint, min_bytes_for_seek))
    {
        /// Clamp the run to the led prefix: an inline serve stops at `fetch_bound` (the first
        /// sibling); a pool worker has `fetch_bound == window.end()`, so this is a no-op for it.
        const size_t run_hi = std::min(led.end(), fetch_bound);
        const size_t step = m.inline_serve ? std::max<size_t>(run_hi - led.offset, 1)
                                            : std::max<size_t>(effectiveWindowSize(level), 1);
        for (size_t off = led.offset; off < run_hi && !m.reached_eof; off += step)
        {
            const ByteRange piece{off, std::min(step, run_hi - off)};
            ChainedBuffers run = fetchGapsFromSource(piece, /*from_prefetch=*/true, m.reached_eof, level,
                m.extent_snapshot, m.inline_serve ? &fill_lane.conn : &m.long_conn, &m,
                /*may_open_long=*/m.inline_serve, m.stats);
            pushChainToWriters(m.writer_views, piece, run, m.stats);
            if (!run.empty())
                m.fetched_end = std::max(m.fetched_end, run.range().end());
            for (const auto & keep : uncommittedIn(m.writer_views, piece))
                led_bytes.append(run.slice(keep));
            residue_full = led_bytes.totalBytes() >= residue_cap;
            if (residue_full || m.interrupt_requested.load(std::memory_order_relaxed))
                break;  /// stop-short; the scope guard still finishes every elected segment
        }
        if (residue_full || m.interrupt_requested.load(std::memory_order_relaxed))
            break;
    }
}

// ─── Gap fetch + backfill ──────────────────────────────────────────────────

/// The cooperative stop probe. The policy lives at the call sites: a LIVE
/// connection stops at the next block (it is saved with the machine and
/// continues from its frontier later - nothing is forfeited); a one-shot GET
/// is never cut mid-response (its request would be forfeited and the remainder
/// would pay a fresh one) - stateless fetches stop only BETWEEN connections.
static bool stopRequested(const MachineBase * stop)
{
    return stop && stop->interrupt_requested.load(std::memory_order_relaxed);
}

ChainedBuffers ReaderExecutor::fetchGapsFromSource(ByteRange physical_window, bool from_prefetch,
    bool & eof_latch, MemoryPressureLevel pressure_level, std::optional<size_t> read_extent,
    std::optional<LongConnection> * lc, const MachineBase * stop, bool may_open_long, Stats & out_stats)
{
    /// PURE source fetch: read the WHOLE window from the source as one contiguous
    /// physical run (short at EOF or at an interrupt point). No cache
    /// `lookup`/`get`/`put`, no plan - this is all a machine fetch step runs (it
    /// cannot touch shared cache/plan state) - `coordinatedPrefetch` (worker) and the
    /// foreground loser-tail call it. The window is already clamped to one plan gap by the
    /// caller, so it never straddles a resident run; the cache fill is the caller's
    /// per-tile commit (`pushChainToWriters`).
    ChainedBuffers result;
    if (physical_window.size == 0)
        return result;

    /// Block size for the source-read tiles, from the per-plan cached pressure level
    /// (a worker passes the machine's `pressure_snapshot`, the foreground `read_plan.geometry()`'s).
    const size_t window_block_size = effectiveBlockSize(pressure_level);

    auto physical_ranges = offset_map.map(physical_window);
    size_t file_pos = physical_window.offset;
    for (const auto & pr : physical_ranges)
    {
        LOG_TRACE(log, "fetchGapsFromSource: source read object={}, offset={}, size={}",
            pr.object.remote_path, pr.object_offset, pr.size);

        /// The ONE connection-policy point: at each OBJECT-piece start, open a long connection
        /// when it is convenient (`openLongConnectionIfWarranted`: the predicted reach warrants one, a
        /// slot is free, and a held unusable channel is dropped first) - so a run that crosses
        /// an object boundary opens the tail object's connection with ITS own reach. Only in
        /// FOREGROUND context (`may_open_long`: the foreground itself and INLINE machines, which
        /// run on the serve thread) - the foreground stays the sole opener; a pool worker
        /// carries what its launch gave it. The policy operates on the lane's slot, which for
        /// every foreground caller IS `lc`.
        if (may_open_long)
            openLongConnectionIfWarranted(pr.object, pr.object_offset, file_pos, out_stats);

        /// No head/tail-extension splits: the window IS the fetch range (the cache
        /// `getOrSet` segment-aligned the miss at plan build, in `openWriteBuffers`).
        auto blocks = allocateBlocks(pr.size, window_block_size);
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

bool ReaderExecutor::isScheduledFillTarget(ByteRange window, size_t entry, ByteRange cell) const
{
    /// A MACHINE fill target: only the `Remote` jobs' cells - the handed kinds
    /// (`UpperCacheRead`/`HandedChain`) fill their cells from served bytes on the serve front
    /// (`runHandedFills`), never from a fetch (the pc fill trails the serve cursor, it does not
    /// ride the fetch lead).
    for (const auto & r : read_plan.schedule.retrieves)
    {
        if (r.source != PlanSchedule::Source::Remote)
            continue;
        if (!(r.range.offset < window.end() && window.offset < r.range.end()))
            continue;  /// retrieve does not cover this window
        for (const auto & t : r.into)
            if (t.entry == entry && t.cell.offset == cell.offset && t.cell.size == cell.size)
                return true;
    }
    return false;
}

void ReaderExecutor::writeSliceToWriter(CacheWriter * writer, ByteRange window, const ChainedBuffers & chain,
    Stats & out_stats)
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

    /// Claim the target cells for the duration of this call - `claim` is the sole
    /// role-acquisition site, a write never adopts a role. Under a machine's window-long
    /// claim (the per-tile commits) this nested claim wins nothing new and its destructor
    /// releases nothing of the machine's; on the claimless paths (the put step at collect,
    /// handed fills, the header populate) it holds the roles for exactly this write. Cells
    /// claimed by a sibling refuse the bytes, as before.
    auto fill_claim = writer->claim(ByteRange{lo, hi - lo});

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
        out_stats.add(Stats::BytesPushedToCacheSync, writer->write(std::move(slice)));
        HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
            static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
    }
}

// ─── Fill lane ─────────────────────────────────────────────────────────────

void ReaderExecutor::pushChainToWriters(const VectorWithMemoryTracking<WriterView> & views, ByteRange window,
    const ChainedBuffers & chain, Stats & out_stats)
{
    for (const auto & view : views)
        writeSliceToWriter(view.writer, window, chain, out_stats);
}

/// The committed parts of `window` within `writer`'s cell. The double subtraction is forced
/// by `IntervalSet` exposing only `add`/`subtract`: `committed().subtract(clamped)` is the
/// UNcommitted part of the clamp, and subtracting that from the clamp recovers the committed
/// parts.
static VectorWithMemoryTracking<ByteRange> committedPartsIn(const CacheWriter & writer, ByteRange window)
{
    const size_t lo = std::max(writer.range().offset, window.offset);
    const size_t hi = std::min(writer.range().end(), window.end());
    if (lo >= hi)
        return {};
    const ByteRange clamped{lo, hi - lo};
    IntervalSet uncommitted;
    for (const auto & gap : writer.committed().subtract(clamped))
        uncommitted.add(gap);
    return uncommitted.subtract(clamped);
}

VectorWithMemoryTracking<ByteRange> ReaderExecutor::uncommittedIn(
    const VectorWithMemoryTracking<WriterView> & views, ByteRange range)
{
    IntervalSet committed_union;
    for (const auto & view : views)
        if (view.writer)
            for (const auto & part : committedPartsIn(*view.writer, range))
                committed_union.add(part);
    return committed_union.subtract(range);
}

void ReaderExecutor::Display::recreditCommittedPrefixes(
    ByteRange window, ChainedBuffers & result, IntervalSet & covered, Stats & out_stats)
{
    /// Re-credit any committed prefix of a frozen miss that a concurrent reader (or this
    /// plan's own write) has grown since plan-build, serving it from the held write buffer's
    /// own `read`. Held write buffers are in tier-priority order, so the `covered` guard
    /// serves each byte from the fastest tier under the SAME shared `covered`.
    for (const auto & buf : plan.bufs)
    {
        if (!buf.provider)
            continue;
        const bool is_page = buf.provider->tier() == CacheTier::PageCache;
        const Stats::Counter tier_counter = is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache;
        for (const auto & w : buf.writers)
        {
            if (!w.writer)
                continue;
            /// The committed prefix this buffer can serve from its own held segment/cells.
            /// Disk: a grown PARTIALLY_DOWNLOADED prefix. Page: a self-populated complete
            /// block re-touched within the plan span.
            for (const auto & committed_part : committedPartsIn(*w.writer, window))
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
                    result.append(chunk.slice(sub));
                    covered.add(sub);
                    out_stats.add(tier_counter, sub.size);
                }
                HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                    static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
            }
        }
    }
}

// ─── Source read ───────────────────────────────────────────────────────────

ChainedBuffers ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t file_pos,
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
    /// within its bound. `lc` is the lane's `conn` or the worker's machine
    /// payload, never the other's, so each thread drains only its own.
    ChainedBuffers head;  /// the prefix served from a held connection that drains to its bound mid-read
    if (lc && *lc)
    {
        if ((*lc)->servesObject(object.remote_path)
            && (*lc)->canContinue(offset, want, min_bytes_for_seek))
            return serveFromLongConnection(*lc, offset, std::move(blocks), file_pos, stop, out_stats);
        /// The read is forward-continuable from `offset` but CROSSES the channel bound. Serve the
        /// prefix up to `read_until` from the held connection - it drains exactly to its bound and
        /// releases clean - then read the remainder from a fresh GET below (the same request a
        /// reopen would cost, but the connection is no longer abandoned mid-run as an incomplete).
        /// Only split on a block boundary; if `read_until` does not land on one (rare - the reach
        /// is cache-aligned), or the connection cannot continue at all, drop and reopen.
        bool split = false;
        if ((*lc)->servesObject(object.remote_path)
            && (*lc)->canStartServing(offset, min_bytes_for_seek))
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
                head = serveFromLongConnection(*lc, offset, std::move(prefix), file_pos, stop, out_stats);
                if (*lc)
                    return head;   /// EOF before the bound: the read ends here
                file_pos += prefix_bytes;
                offset += prefix_span;   /// == read_until; continue with the suffix below
                want -= prefix_bytes;
                blocks = std::move(suffix);
                split = true;
            }
        }
        if (!split)
            dropLongConnection(*lc, out_stats);
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

        chain.append(ChainedBufferNode{block, 0, got, file_pos + total_read});
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

VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> ReaderExecutor::allocateBlocks(size_t size, size_t block_size)
{
    chassert(block_size > 0);
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks;
    blocks.reserve((size + block_size - 1) / block_size);
    size_t pos = 0;
    while (pos < size)
    {
        const size_t chunk = std::min(block_size, size - pos);
        blocks.push_back(std::make_shared<OwnedChainedBuffer>(chunk));
        pos += chunk;
    }
    return blocks;
}

// ─── Long connection ────────────────────────────────────────────────────────

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
        end = std::min(end, toPhys(totalSize()));
    return end;
}

size_t ReaderExecutor::boundedReach(size_t phys_off) const
{
    /// The physical reach a long connection opened at `phys_off` actually gets, BEFORE any
    /// extent floor: the estimator's `predictedForwardLength` clamped to the file end, then clamped
    /// DOWN at the next WIDE cached run the plan shows - a resident run at/above
    /// `min_bytes_for_seek` before `plan_end`, where the channel must stop (that region is
    /// served from cache / filled down, not over-read; holes strictly below the bound are
    /// bridged by `LongConnection::canContinue` on the open GET). A run cut by the plan
    /// boundary appears short here and is not a real stop, so the trajectory stays free to
    /// extend past the look-ahead. This is the SINGLE reach source shared by the open trigger
    /// (`shouldOpenLongConnection`) and the channel bound (`longConnectionBound`), so the two can never
    /// disagree on how far the channel reaches. Reads only the tracker scalar + plan geometry.
    size_t reach = clampReach(continuity_tracker.predictedForwardLength(), phys_off);
    const auto & geom = read_plan.geometry();
    if (geom)
    {
        const size_t wide = scheduleLookaheadReach(phys_off);
        const auto res = wide < geom->plan_end ? geom->residentAt(wide) : CoverageMap::Resident{};
        if (res.resident() && res.run_end - wide >= min_bytes_for_seek)
            reach = std::min(reach, wide);
    }
    /// Cap the long-connection reach so an over-predicted continuous run cannot open or
    /// extend a GET beyond the bound.
    reach = std::min(reach, phys_off + long_connection_max_bound);
    return reach;
}

bool ReaderExecutor::shouldOpenLongConnection(size_t phys_off) const
{
    /// Open a long connection when the estimator's predicted contiguous reach runs past
    /// the current read window - "a connection whose range exceeds the read window is
    /// long". Gated by the connection limit (the `reader_executor_use_long_connections`
    /// setting); suppressed under High/Critical pressure exactly where prefetch is.
    if (fill_lane.conn || fill_lane.conn_lent || !long_connection_limit)
        return false;
    const MemoryPressureLevel level
        = read_plan.geometry() ? read_plan.geometry()->pressure_level : MemoryPressureLevel::Normal;
    if (!prefetchEnabled(level))
        return false;
    /// Open when the forward reach runs past the current read extent - the right boundary
    /// where a short connection stops and the next read pays a fresh request. A long connection
    /// continues past it instead. The reach is `boundedReach` - the SAME value `longConnectionBound`
    /// sizes the channel with - so the trigger never opens a "long" channel the bound would then
    /// clamp back to the extent (a reverse/scattered pattern, or a run walled off by a near wide
    /// cached run, stays short). When no extent is advertised, fall back to one window.
    const size_t boundary = read_extent_end
        ? toPhys(*read_extent_end)
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
    /// The reach (`boundedReach`: `predictedForwardLength` clamped at the next wide cached run) is the
    /// read's forward trajectory, which extrapolates past the current extent. It is the same
    /// value `shouldOpenLongConnection` triggers on, so the GET drains cleanly at a wide cached run
    /// instead of being abandoned mid-run, and the trigger never opens a channel this bound
    /// would clamp back to the extent. Holes strictly below the bound are bridged by
    /// `LongConnection::canContinue` on the open GET.
    const size_t object_base = phys_offset - object_offset;
    const size_t object_end = hasUnknownSize()
        ? std::numeric_limits<size_t>::max()
        : object_base + object.bytes_size;
    const size_t extent = read_extent_end
        ? std::min<size_t>(toPhys(*read_extent_end), object_end)
        : object_end;
    const size_t reach = boundedReach(phys_offset);
    size_t phys_bound = std::max(extent, reach);
    /// A warranted long connection opens with at least `long_connection_open_range`
    /// and never streams past `long_connection_max_bound`: it bounds an over-predicted
    /// continuous-read reach so the GET drains within the cap instead of running away.
    /// The open-range floor is for forward-spanning reads only -- a one-shot `readBigAt`
    /// transient stays bounded to its request (no continuity, no look-ahead), so flooring
    /// it would over-read past the requested piece of an object.
    if (!is_transient)
        phys_bound = std::max(phys_bound, phys_offset + long_connection_open_range);
    phys_bound = std::min(phys_bound, phys_offset + long_connection_max_bound);
    phys_bound = std::min(phys_bound, object_end);
    return phys_bound - object_base;
}

void ReaderExecutor::openLongConnectionIfWarranted(const StoredObject & object, size_t object_offset,
    size_t phys_offset, Stats & out_stats)
{
    /// Drop a held channel that cannot even START serving this fetch - wrong object, backward,
    /// a gap at/above `min_bytes_for_seek`, or already past its bound - before deciding to open,
    /// so a fresh channel covers the run from its first byte. A channel that CAN start serving
    /// is left for `readFromSource`, which serves up to the bound and reopens for any remainder;
    /// dropping it here would degrade the window to a one-shot and reopen only on the NEXT
    /// window, doubling the GET count of every cold run that follows a wide cached gap.
    /// `canStartServing` compares against the connection's OBJECT-LOCAL state, so the check
    /// takes `object_offset` (a physical position here would offset the comparison by the
    /// preceding blobs' size on a multi-blob file and drop a perfectly continuable channel).
    if (fill_lane.conn && !(fill_lane.conn->servesObject(object.remote_path)
            && fill_lane.conn->canStartServing(object_offset, min_bytes_for_seek)))
        dropLongConnection(fill_lane.conn, out_stats);
    if (!shouldOpenLongConnection(phys_offset))
        return;
    LongConnectionSlot slot = long_connection_limit->tryAcquire(long_connection_limit);
    if (!slot)
    {
        /// Wanted a long connection but the pool is at capacity - read a one-shot instead.
        out_stats.add(Stats::LongConnectionFallbacks);
        return;
    }
    openLongConnection(fill_lane.conn, object, object_offset, longConnectionBound(object, object_offset, phys_offset),
        std::move(slot), out_stats);
}

void ReaderExecutor::openLongConnection(std::optional<LongConnection> & conn, const StoredObject & object,
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

ChainedBuffers ReaderExecutor::serveFromLongConnection(std::optional<LongConnection> & conn, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t file_pos,
    const MachineBase * stop, Stats & out_stats) const
{
    /// Precondition: the caller has checked `servesObject` + `canContinue`.
    if (offset > conn->current_position)
    {
        /// Bridge the small forward gap by discarding it on the open stream: the
        /// bytes cross the wire (over-read) but the source request is saved.
        const size_t skipped = conn->skipForward(offset - conn->current_position, block_size);
        out_stats.add(Stats::BytesFromSource, skipped);
    }
    /// The served bytes are counted as `BytesFromSource` by the caller (the returned
    /// chain), as on the one-shot path.
    ChainedBuffers chain = conn->readInto(std::move(blocks), file_pos, stop);
    out_stats.add(Stats::LongConnectionHits);
    out_stats.add(Stats::LongConnectionBytes, chain.totalBytes());
    releaseLongAtBound(conn);
    return chain;
}

void ReaderExecutor::dropLongConnection(std::optional<LongConnection> & conn, Stats & out_stats) const
{
    if (!conn)
        return;
    /// Drain a small tail (at most `max_tail_for_drain`) so the connection returns to
    /// the pool reusable; if it drained but did not reach the bound, it ended short at EOF.
    /// The drain is best-effort (`drainTail` never throws): a read error leaves the
    /// connection in an unknown state, so it is always released as incomplete.
    const auto drain = conn->drainTail(max_tail_for_drain, block_size, log);
    out_stats.add(Stats::BytesFromSource, drain.bytes);
    if (drain.failed)
        out_stats.add(Stats::IncompleteConnections);
    else
        accountLongConnectionDrop(conn, /*at_eof=*/drain.bytes > 0 && !conn->atBound(), out_stats);
    conn.reset();
}

void ReaderExecutor::accountLongConnectionDrop(const std::optional<LongConnection> & conn, bool at_eof, Stats & out_stats) const
{
    /// A connection dropped before it was fully consumed (not read to its bound or to
    /// EOF) is abandoned mid-response, not pool-reusable. One that never transferred
    /// is excluded: its lazy GET never started.
    if (conn && !(at_eof || conn->exhausted()) && conn->consumedAnyBytes())
        out_stats.add(Stats::IncompleteConnections);
}

void ReaderExecutor::releaseLongAtBound(std::optional<LongConnection> & conn) const
{
    if (conn && conn->exhausted())
        conn.reset();
}

// ─── Deferred puts / promotes ──────────────────────────────────────────────

void ReaderExecutor::collectFillTargets(FetchMachine & m)
{
    /// Record NON-OWNING views of the window's fill-target writers in the shared
    /// `read_plan.bufs`: the cells the plan SCHEDULE designates as REMOTE fill targets for a
    /// retrieve overlapping this window (`buildSchedule`'s `into` - the bottom tier and
    /// same-tier slower layers; the faster tiers fill by handed jobs on the serve front). Done
    /// at launch so the worker can write the led segments inline during its fetch; the writers
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

void ReaderExecutor::runPutStep(std::shared_ptr<FetchMachine> m, const ChainedBuffers & assembled)
{
    /// `writer_views` were recorded at LAUNCH (`collectFillTargets`): NON-OWNING views of this
    /// window's fill-target writers in the shared `read_plan.bufs`, written in place on THIS
    /// read thread, inline at collect - the machine's own counters were already folded there,
    /// so the fill accounts straight into the executor `stats`. Runs AFTER the collect pinned
    /// at the fetch frontier. A failed fill is logged, never thrown: a read must not fail
    /// because cache population did.
    if (m->writer_views.empty())
        return;  /// nothing to fill for this window

    try
    {
        const size_t fill_end = assembled.empty()
            ? m->physical_window.offset
            : std::min(m->physical_window.end(), assembled.range().end());
        pushChainToWriters(m->writer_views, m->physical_window, assembled, stats);
        /// Pin the partial segment under the just-written frontier (the lane's slot):
        /// the collect pinned BEFORE this fill landed, so a fresh segment was not pinnable
        /// there. A `readBigAt` transient reads its bounded extent once and is destroyed,
        /// so it pins NOTHING (mirrors the collect's `!is_transient` guard) - else its
        /// cell survives an eviction sweep that should drop it.
        if (!is_transient)
            for (const auto & view : m->writer_views)
            {
                if (view.writer && fill_end >= view.writer->range().offset && fill_end < view.writer->range().end())
                    if (auto pin = view.writer->pin(fill_end))
                    {
                        fill_lane.pin = std::move(pin);
                        break;
                    }
            }
    }
    catch (...)
    {
        stats.add(Stats::PutFailed);
        tryLogCurrentException(log, "Cache fill failed");
    }
}

void ReaderExecutor::runHandedFills(ByteRange served_range, const ChainedBuffers & bytes, Stats & out_stats)
{
    /// A writer the in-flight machine holds as a fill target is ON LOAN: its worker streams
    /// into it from the pool thread, and an `UpperCacheRead`'s `into` cell IS a Remote
    /// retrieve's cell - the same `CacheWriter`. Skip it (handed fills are opportunistic;
    /// a skipped down-fill lands on a later pass or the next plan) - the exclusion the old
    /// borrow protocol provided, and the invariant `FillLane`'s guard pins.
    const auto on_loan = [&](const CacheWriter * w)
    {
        if (!machine)
            return false;
        for (const auto & v : machine->writer_views)
            if (v.writer == w)
                return true;
        return false;
    };
    for (const auto & r : read_plan.schedule.retrieves)
    {
        if (r.source == PlanSchedule::Source::Remote)
            continue;
        if (!(r.range.offset < served_range.end() && served_range.offset < r.range.end()))
            continue;
        for (const auto & wt : r.into)
        {
            if (wt.entry >= read_plan.bufs.size() || !read_plan.bufs[wt.entry].provider)
                continue;
            for (auto & w : read_plan.bufs[wt.entry].writers)
            {
                if (!w.writer || on_loan(w.writer.get()))
                    continue;
                const size_t lo = std::max({served_range.offset, r.range.offset, w.writer->range().offset, wt.cell.offset});
                const size_t hi = std::min({served_range.end(), r.range.end(), w.writer->range().end(), wt.cell.end()});
                if (lo >= hi)
                    continue;
                auto slice = bytes.slice(ByteRange{lo, hi - lo});
                if (slice.empty())
                    continue;
                out_stats.add(Stats::CachePopulateRequests);
                StatTimer put_scope(out_stats, Stats::CachePopulateMicroseconds);
                auto handed_claim = w.writer->claim(ByteRange{lo, hi - lo});
                const size_t written = w.writer->write(std::move(slice));
                if (r.source == PlanSchedule::Source::HandedChain)
                {
                    out_stats.add(Stats::BytesPromoted, written);
                    HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
                        static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
                }
            }
        }
    }
}

/// The serve run whose `output` contains `pos_phys`. Clamps to the last run past the
/// materialized span (EOF / extent ceiling).
size_t ReaderExecutor::serveRunAt(size_t pos_phys) const
{
    const auto & runs = read_plan.schedule.serve_runs;
    for (size_t i = 0; i < runs.size(); ++i)
        if (runs[i].output.offset <= pos_phys && pos_phys < runs[i].output.end())
            return i;
    /// Past the materialized span (EOF / extent ceiling): clamp to the last run.
    return runs.empty() ? 0 : runs.size() - 1;
}

// ─── Schedule-driven interpreter ──────────────────────────────────────────────
//
// `readNextWindow` runs the schedule's already-planned jobs instead of re-deriving
// the next gap from the coverage map. Two decoupled frontiers: the serve `cursor`
// (what the query reads) and each job's display-derived progress (`jobFrontier`,
// running ahead of the cursor). ONE machine in flight - sequential serve is ordered, so a
// deeper read-ahead only trades memory for latency-hiding, and connection parallelism
// comes from multiple executors. A populatable job's bytes live in its cells (the cache is
// the buffer); what the cells cannot hold goes to the lane's bank (PHYSICAL coords, like
// everything inside the executor - the one shift to logical happens on the consumer exit,
// `finishWindow`). The long connection coalesces the GETs across pieces.

bool ReaderExecutor::clampAllowsAhead(size_t ri) const
{
    /// THE CLAMP - same-cell ordering as a frontier bound, not a graph. A cell is
    /// append-only: bytes launched past an unfilled range the job does NOT fetch itself -
    /// an embedded resident middle awaiting its serve-time down-fill (`UpperCacheRead` /
    /// handed) - would only be refused at the frontier and discarded. Hold the AHEAD launch
    /// until everything in the target cells below the launch position is committed or lies
    /// in the job's OWN fetch runs (its refused or pending bytes were never a reason to
    /// hold - the serve banks them; same-cell gaps FOLD into one Remote, so no
    /// Remote-vs-Remote ordering exists). The PUMP is exempt:
    /// demand production heals through the bank; only the ahead anchor waits. A hold is
    /// BOUNDED by the held job's own consumption: the pump advances the launch high-water
    /// past every served window regardless of refusals, so the scan retires the job even
    /// when the awaited down-fill itself was refused - degradation, never a livelock.
    const auto & r = read_plan.schedule.retrieves[ri];
    const auto & geom = read_plan.geometry();
    const size_t launch_pos = std::max(r.range.offset, launchProgress(ri));
    for (const auto & wt : r.into)
    {
        const size_t lo = wt.cell.offset;
        const size_t hi = std::min(wt.cell.end(), launch_pos);
        if (lo >= hi)
            continue;
        const ByteRange below{lo, hi - lo};
        for (const auto & gap : display.committedCoverage(below).subtract(below))
        {
            bool own = false;
            for (const auto & run : r.fetch_runs)
                if (run.offset <= gap.offset && gap.end() <= run.end())
                {
                    own = true;
                    break;
                }
            if (own)
                continue;
            /// Bytes resident in the CELL's OWN tier are already the segment's content - a
            /// resumed partially-downloaded segment's prefix, a DOWNLOADED middle merged into
            /// the cell - and can never enter this plan's committed sets (the writer appends
            /// at the live segment frontier, past them). Nothing to wait for: only a FASTER
            /// tier's resident range awaits a down-fill into this cell.
            bool in_cell_tier = false;
            for (const auto & res : geom->entries[wt.entry].resident)
                if (res.offset <= gap.offset && gap.end() <= res.end())
                {
                    in_cell_tier = true;
                    break;
                }
            if (!in_cell_tier)
                return false;
        }
    }
    return true;
}

std::function<StepResult()> ReaderExecutor::makeFetchStep(FetchMachine & m)
{
    /// The machine's fetch step, runner-independent: `PoolFetchMachineRunner` runs it on a pool
    /// worker, a `LocalFetchMachineRunner` inline on the serve thread. Elect + fetch the led
    /// segments + write them inline on the running thread (it is the downloader); sibling-led
    /// holes set `contended` so the foreground revokes to the sync path at collect. A cancel
    /// mid-fetch leaves a partial led prefix; collect revokes or serves it (a sparse `fetched`
    /// from sibling-led holes is NORMAL, not a stop indicator).
    return [this, self = &m]
    {
        coordinatedPrefetch(*self);
        if (self->interrupt_requested.load() && !self->reached_eof)
        {
            self->stats.add(Stats::MachineInterrupted);
            return StepResult::Interrupted;
        }
        return StepResult::AwaitCollect;
    };
}

bool ReaderExecutor::launchMachineForWindow(size_t ri, ByteRange window, IFetchMachineRunner & machine_runner)
{
    auto m = std::make_shared<FetchMachine>();
    m->physical_window = window;
    m->retrieve_index = ri;
    m->pressure_snapshot = read_plan.geometry()->pressure_level;
    m->extent_snapshot = read_extent_end;
    /// Inline (serve-thread) runner -> the fetch stops at the first sibling-led segment.
    m->inline_serve = (&machine_runner == local_runner.get());
    /// Record the fill-target writers now so the step can write its led segments inline during
    /// the fetch (the collect's `runPutStep` reuses these views).
    collectFillTargets(*m);

    /// The foreground is the sole opener; the aligned window's first physical range gives the
    /// object and its object-local offset. A no-op when not warranted / at capacity / a usable
    /// connection is already held. The channel bound comes from the runtime reach
    /// (`longConnectionBound`: `predictedForwardLength` clamped at the next wide cached run), the same on
    /// the prefetch and foreground paths - the schedule no longer hands down a span.
    auto prefetch_ranges = offset_map.map(window);
    if (!prefetch_ranges.empty())
        openLongConnectionIfWarranted(prefetch_ranges.front().object, prefetch_ranges.front().object_offset,
            window.offset, stats);
    /// A POOL piece borrows the lane's connection for its flight (the worker advances it
    /// on its thread); an INLINE piece runs on the serve thread and reads through the
    /// lane's slot directly - nothing to hand over.
    if (!m->inline_serve)
        fill_lane.lend(*m);

    m->run_step = makeFetchStep(*m);

    if (!machine_runner.schedule(m))
    {
        /// Queue reject (pool runner only): the machine is parked, payload untouched - reclaim
        /// the pristine connection so the caller reads synchronously.
        fill_lane.reclaim(*m);
        stats.add(Stats::PrefetchPoolFull);
        return false;
    }
    machine = std::move(m);
    return true;
}

void ReaderExecutor::launchRetrieve(size_t ri)
{
    const auto & r = read_plan.schedule.retrieves[ri];
    const MemoryPressureLevel level = read_plan.geometry()->pressure_level;

    /// Fetch within the job range at its launch frontier - never `r.range` itself (a coalesced
    /// connection can be a whole column). The frontier is the display truth advanced past
    /// already-attempted bytes (`launchProgress`): the background continues the job from
    /// wherever the last piece - its own or an inline one - left off, which is what makes
    /// stopping a piece anywhere a free migration.
    const size_t base = launchProgress(ri);
    if (base >= r.range.end())
        return;

    /// The fill-ahead lead is a HIGH-WATER anchored at the CONSUMER: launch only work that
    /// lands inside `[cursor, cursor + lead)`. Data past the horizon is not fetched yet - the
    /// cursor advancing pulls the horizon (and with it the next launch) forward - so the
    /// read-ahead footprint stays lead-bounded and adjacent to the serve.
    const size_t horizon_end = toPhys(position) + fillAheadLead(level);
    if (base >= horizon_end)
        return;
    const size_t chunk = std::min(r.range.end() - base, boundedFetchSize(horizon_end - base));
    if (chunk == 0)
        return;
    /// Refill hysteresis: a launch costs a machine round-trip (and, on the stateless arm, its
    /// own GET), so top the lead up in at-least-window pieces - except the job tail, which is
    /// whatever remains.
    if (base + chunk < r.range.end() && chunk < effectiveWindowSize(level))
        return;

    /// Read-ahead runs on the pool (async), committing cells progressively; the serve cursor
    /// reads the committed prefix live.
    launchMachineForWindow(ri, ByteRange{base, chunk}, *runner);
}

void ReaderExecutor::advanceAhead()
{
    if (!prefetch_pool)
        return;
    /// Finalize the in-flight machine FIRST - before the `atEnd()` early-return below, so the
    /// machine that fills the tail up to the extent/EOF is still collected. Collect as soon as
    /// the worker RELEASED the machine (its products are in the cells, so joining is free), not
    /// only once the cursor consumed the lead: the freed slot lets the launch below top the
    /// read-ahead back up to the cursor-anchored horizon, so the producer never idles while the
    /// consumer drains. A machine still RUNNING with the cursor inside its lead keeps going -
    /// the serve reads its committed cells live, and the cursor cannot pass the worker's
    /// committed frontier (the serve waits on it), so a consumed-lead collect does not block.
    if (machine)
    {
        const size_t cursor_phys = toPhys(position);
        const bool lead_consumed = atEnd() || cursor_phys >= machine->physical_window.end();
        if (!lead_consumed && !machineReleased())
            return;  /// still filling ahead of the cursor
        collectInFlightInto(machine->retrieve_index);
    }
    if (atEnd())
        return;
    drainAbandonedMachines();

    /// FULL-CACHE BACKPRESSURE: a collect banked residue the cells refused. Launching
    /// more lead would only fetch more bytes the cache cannot take - hold until the
    /// serve consumed the refused bank, then resume.
    if (fill_lane.bank_refused)
    {
        if (!fill_lane.bank.empty())
            return;
        fill_lane.bank_refused = false;
    }

    const size_t position_phys = toPhys(position);
    const size_t probe = boundedFetchSize(window_size);
    if (probe == 0)
        return;
    /// The producer's look-ahead replan: demand coverage through the next ahead window
    /// (the machine above is collected, so the mid-plan rebuild is legal here).
    preparePlan(position_phys, /*coverage_ahead=*/probe);
    /// Fully cache-served plan: the look-ahead re-plan above has already pulled any
    /// upcoming cold region into the plan, so if there is still no `Source::Remote`
    /// retrieve there is nothing to prefetch - skip the rest of the bookkeeping.
    if (!read_plan.has_remote_retrieves)
        return;
    if (!prefetchEnabled(read_plan.geometry()->pressure_level))
        return;  /// read-ahead suppressed under High/Critical memory pressure

    auto & retrieves = read_plan.schedule.retrieves;
    for (size_t ri = read_plan.launch_frontier; ri < retrieves.size(); ++ri)
    {
        const auto & r = retrieves[ri];
        /// The schedule says which jobs may run ahead (`ahead_eligible`: the handed kinds take
        /// the serve's output as input, so they are serve-front only); a job whose launch
        /// frontier reached its end is done. Advance the scan past them so it never rescans.
        if (!r.ahead_eligible || launchProgress(ri) >= r.range.end())
        {
            if (ri == read_plan.launch_frontier)
                ++read_plan.launch_frontier;
            continue;
        }
        if (!clampAllowsAhead(ri))
            return;  /// hold: the target cell waits on another job's fill below the launch position
        launchRetrieve(ri);
        return;
    }
}

ByteRange ReaderExecutor::nextScheduledPiece(size_t ri, ByteRange window_phys) const
{
    /// The next PIECE of a populatable retrieve, straight off the schedule: the job's
    /// `fetch_runs` are the source ranges (split at every embedded resident region - served /
    /// filled down from its tier, never scheduled as a source read), and its fetch grids give
    /// the cell-fill granularity. The piece starts at the CELL frontier of the grid-floored
    /// window start - a mid-cell read fills from the cell floor (append-only), and a
    /// down-filled hit advances the frontier past itself so the next piece resumes after it -
    /// and runs to the window's end ceiled to the grid (the whole-cell over-read that makes
    /// one cold cell ONE source read), clamped into the run. No geometry is consulted here -
    /// the schedule is the job.
    const auto & r = read_plan.schedule.retrieves[ri];
    /// The walk frontier is committed CELLS plus the BANK - not resident views (a refused
    /// down-fill's resident hole must be read through, below) - so a refused-write piece whose
    /// bytes went to the bank is walked PAST, not refetched forever.
    const auto fill_prefix_end = [&](ByteRange range)
    {
        IntervalSet cov = display.committedCoverage(range);
        fill_lane.addBankCoverage(cov, range);
        auto gaps = cov.subtract(range);
        return gaps.empty() ? range.end() : gaps.front().offset;
    };
    const size_t missing = fill_prefix_end(window_phys);
    const size_t head_grid = std::max<size_t>(r.fetch_head_grid, 1);
    const size_t tail_grid = std::max<size_t>(r.fetch_tail_grid, 1);
    /// The append-only floor: grid-floor the first missing byte (clamped to the job) and walk
    /// the fill frontier from there - ACROSS runs, so a before-slack run no serve window ever
    /// reaches (a seek past it) is still fetched and the cell fills whole from its floor.
    const size_t floor_off = std::max(r.range.offset, missing / head_grid * head_grid);
    const size_t base = floor_off < missing
        ? fill_prefix_end(ByteRange{floor_off, missing - floor_off})
        : missing;
    /// The piece: from the frontier to the end of the first run past it. The frontier can sit
    /// in an inter-run resident hole (its down-fill write was skipped by the append-only
    /// cell); the piece then reads THROUGH the hole from the source so the cell still
    /// completes - display gaps stop at resident regions and would leave the cell short.
    for (const auto & fr : r.fetch_runs)
        if (fr.end() > base)
            return ByteRange{base,
                std::min(fr.end(), (window_phys.end() + tail_grid - 1) / tail_grid * tail_grid) - base};
    return {};
}

void ReaderExecutor::interruptAndCollectMachine()
{
    /// Bound the stall-join: ask a still-running worker to wrap at its next tile instead
    /// of holding the cursor for the whole remaining lead (a released one no-ops); the
    /// un-fetched tail stays un-attempted and relaunches.
    collectRunner().requestInterrupt(*machine);
    collectInFlightInto(machine->retrieve_index);
}

bool ReaderExecutor::waitSiblingFills(ByteRange window)
{
    /// Dedup + late hits + our own leading worker's progress: wait on any cell a LIVE
    /// writer is filling (a completed one returns immediately), bounded to the cursor
    /// WINDOW. Bytes our committed cells do not hold (a sibling's download) are BANKED -
    /// the bank is their only route to the display - and their cache-read credit is folded
    /// here (the bank serve adds no counters). Bytes our own worker committed meanwhile
    /// are dropped: the serve reads and counts them from the cells, once. A bypass gap
    /// has no fill-target writer: no-op there. TRUE = the display can now serve the
    /// window's first byte.
    ChainedBuffers waited;
    IntervalSet wait_cov = display.coverage(window);
    display.wait(window, waited, wait_cov);
    if (waited.empty())
        return false;

    const IntervalSet committed = display.committedCoverage(window);
    ChainedBuffers sibling_bytes;
    for (const auto & iv : waited.getIntervals())
        for (const auto & gap : committed.subtract(iv))
            sibling_bytes.append(waited.slice(gap));
    if (!sibling_bytes.empty())
    {
        stats.add(Stats::BytesFromFilesystemCache, sibling_bytes.totalBytes());
        fill_lane.bank.append(std::move(sibling_bytes));
    }
    /// Advance the ahead cursor only to the CONTIGUOUS display frontier: a waited
    /// middle that returned short leaves a real hole, and marking it attempted would
    /// stop the background from ever fetching it (the foreground would heal it, late).
    const size_t contiguous = display.frontier(window);
    fill_lane.advanceAttempted(contiguous);
    return contiguous > window.offset;
}

bool ReaderExecutor::pump(std::optional<size_t> ri_opt, ByteRange window)
{
    /// The HEAL is the first production step: a claimed-but-unreadable cursor (a raced
    /// shrink/detach staled the truth; a hit run's view goes stale the same way) is
    /// producible only by the cache-blind read, banked into the lane - and it is the ONLY
    /// production a job-less (hit) run has. A false heal proves the source empty at the
    /// window: nothing left to produce.
    if (display.frontier(window) > window.offset)
        return bankDirectRead(window);
    if (!ri_opt.has_value())
        return false;   /// a hit run that stops covering has nothing else to produce

    const size_t ri = *ri_opt;
    const auto & r = read_plan.schedule.retrieves[ri];

    /// Join an in-flight machine first - EXCEPT our own machine still LEADING this window:
    /// its worker commits cells progressively, so the window-bounded WAIT below lets it
    /// land the window instead of blocking on the whole remaining lead. A FOREIGN machine (or our own past the cursor) holds the single slot the cursor
    /// outran - free the slot.
    const bool own_leading = machineFor(ri)
        && machine->physical_window.offset <= window.offset
        && window.offset < machine->physical_window.end();
    if (machine && !own_leading)
    {
        interruptAndCollectMachine();
        return true;
    }

    /// The piece extends to the job's fetch grids, clamped into its range (cell-fill
    /// granularity; identity for a bypass job - its grids are 1). The grid head can reach
    /// across a same-tier resident run - at most one grid cell, cache-served when
    /// resident, once per plan.
    const size_t head_grid = std::max<size_t>(r.fetch_head_grid, 1);
    const size_t tail_grid = std::max<size_t>(r.fetch_tail_grid, 1);
    const size_t fetch_lo
        = std::min(window.offset, std::max(r.range.offset, window.offset / head_grid * head_grid));
    const size_t fetch_hi = std::max(window.end(),
        std::min(r.range.end(), (window.end() + tail_grid - 1) / tail_grid * tail_grid));
    const ByteRange fetch_window{fetch_lo, fetch_hi - fetch_lo};

    /// 1) The wait step (`waitSiblingFills`) - bounded to the cursor WINDOW; the
    ///    grid-extended tail is not needed to serve it.
    if (waitSiblingFills(window))
        return true;

    /// The wait landed nothing servable at the cursor with our own machine still in flight:
    /// the worker is done or stuck - join it (a done one's refused bytes overflow-bank here).
    if (machine)
    {
        interruptAndCollectMachine();
        return true;
    }

    /// 2) A source piece run as an INLINE machine (the same Fill flow as the background: elect
    ///    + fetch with the in-flow connection policy + commit; the collect pins, runs the
    ///    deferred put, and overflow-banks what the cells refused). A POPULATABLE job's piece
    ///    comes off the SCHEDULE walk (the cell's append-only floor and the fetch runs, reading
    ///    through refused-down-fill resident holes so the cell completes); when the walk is
    ///    exhausted - or for a bypass job - the piece is the display's first uncovered gap.
    ///    A latched `reached_eof` does NOT refuse the launch: under a size-unknown source the
    ///    latch records that AN end was seen, not where - a below-end gap (a pool lead's put
    ///    the cache refused) must still be re-fetched or its window is silently dropped. A gap
    ///    past the true end costs one empty read and the loop breaks, as the legacy path paid.
    ByteRange piece{};
    if (!r.into.empty())
        piece = nextScheduledPiece(ri, window);
    if (piece.size == 0)
    {
        /// Coverage read HERE, after the wait: bytes it banked or the worker committed
        /// meanwhile are not gaps anymore, so the piece is not re-fetched over them.
        auto gaps = display.coverage(fetch_window).subtract(fetch_window);
        if (gaps.empty())
            return false;
        piece = gaps.front();
    }
    const size_t piece_covered_before = display.coverage(piece).totalBytes();
    if (!launchMachineForWindow(ri, piece, *local_runner))
        return false;
    collectInFlightInto(ri);
    /// The ahead cursor to the window end: attempted, whether the cells took the bytes or
    /// the overflow bank did - the background never re-launches it.
    fill_lane.advanceAttempted(window.end());
    /// Progress = this piece landed NEW servable bytes (a coverage DELTA, not the absolute
    /// frontier - a read-through piece's pre-covered head would make the absolute check
    /// vacuously true and starve the escape hatch below) or the display can now serve the
    /// window's first byte. A populatable piece can legitimately sit LEFT of the cursor,
    /// filling its cell from the floor - the next call walks past what it landed.
    if (display.coverage(piece).totalBytes() > piece_covered_before
        || display.frontier(window) > window.offset)
        return true;

    /// 3) Last resort - the wait-timeout escape hatch: a sibling leader hung mid-download, so
    ///    the wait returned short AND our election still loses (the segment keeps a foreign
    ///    downloader). Bounded to one window, only on this rare path.
    return bankDirectRead(window);
}

bool ReaderExecutor::bankDirectRead(ByteRange window)
{
    /// One bounded cache-blind source read of the window, banked - the display serves it and
    /// the consuming trim retires it. The heal verb for state no planned job can produce:
    /// a hung sibling leader (the election keeps losing) or a raced shrink/detach that staled
    /// the committed truth at the cursor. Empty = nothing there (EOF for this extent).
    ChainedBuffers direct = fetchGapsFromSource(window, /*from_prefetch=*/false, reached_eof,
        read_plan.geometry()->pressure_level, read_extent_end, &fill_lane.conn, /*stop=*/nullptr,
        /*may_open_long=*/true, stats);
    if (direct.empty())
        return false;
    fill_lane.bank.append(std::move(direct));
    return true;
}

IntervalSet ReaderExecutor::Display::committedCoverage(ByteRange window_phys) const
{
    /// Mirrors the committed-range computation in `recreditCommittedPrefixes` but only
    /// accumulates coverage - no `read`, no stats - so the serve can poll the fill front.
    IntervalSet covered;
    for (const auto & buf : plan.bufs)
        for (const auto & w : buf.writers)
            if (w.writer)
                for (const auto & part : committedPartsIn(*w.writer, window_phys))
                    covered.add(part);
    return covered;
}

size_t ReaderExecutor::committedCellPrefixEnd(ByteRange window_phys) const
{
    /// The first uncovered byte (in increasing-offset order) is the end of the contiguous
    /// committed prefix; with the window fully covered there is no gap, so it is the window end.
    auto gaps = display.committedCoverage(window_phys).subtract(window_phys);
    return gaps.empty() ? window_phys.end() : gaps.front().offset;
}

size_t ReaderExecutor::jobFrontier(size_t ri) const
{
    const auto & r = read_plan.schedule.retrieves[ri];
    /// A bypass job has no cell to derive from: its bank is consumed as it serves, so the
    /// lane's ahead cursor, clamped into the job, is the frontier.
    if (r.into.empty())
        return std::min(std::max(r.range.offset, fill_lane.attempted_end), r.range.end());
    for (const auto & run : r.fetch_runs)
    {
        const size_t frontier = committedCellPrefixEnd(run);
        if (frontier < run.end())
            return frontier;
    }
    return r.range.end();
}

size_t ReaderExecutor::launchProgress(size_t ri) const
{
    /// The background launch POLICY frontier: the display truth (`jobFrontier`) advanced past
    /// bytes this executor already ATTEMPTED - launched a machine over, or served inline past
    /// the cursor - that can never enter its OWN committed set: a refused cell write (cache
    /// full / download budget) or a segment a sibling executor downloaded (the per-writer
    /// committed set excludes both). Without the high-water the launcher would re-GET the same
    /// pinned lead every serve window and the launch scan would never retire the job. The
    /// SERVE never uses this - it reads the display, which is the data truth.
    const auto & r = read_plan.schedule.retrieves[ri];
    return std::max(jobFrontier(ri), std::min(std::max(r.range.offset, fill_lane.attempted_end), r.range.end()));
}

// ─── The display ───────────────────────────────────────────────────────────

bool ReaderExecutor::Display::coversByte(size_t phys) const
{
    return covers(ByteRange{phys, 1});
}

IntervalSet ReaderExecutor::Display::coverage(ByteRange window_phys) const
{
    /// Committed cells - the writers' LIVE committed sets, so an in-flight worker's streaming
    /// commits show up here as they land (the fill front's current progress).
    IntervalSet cov = committedCoverage(window_phys);
    /// Resident hit views - the plan's pinned facts (an entry can only serve through its held view).
    if (const auto & geom = plan.geometry())
        for (size_t i = 0; i < geom->entries.size(); ++i)
        {
            if (i >= plan.bufs.size() || !plan.bufs[i].view)
                continue;
            for (const auto & res : geom->entries[i].resident)
            {
                const size_t lo = std::max(res.offset, window_phys.offset);
                const size_t hi = std::min(res.end(), window_phys.end());
                if (lo < hi)
                    cov.add(ByteRange{lo, hi - lo});
            }
        }
    lane.addBankCoverage(cov, window_phys);
    return cov;
}

bool ReaderExecutor::Display::covers(ByteRange window_phys) const
{
    return coverage(window_phys).subtract(window_phys).empty();
}

size_t ReaderExecutor::Display::frontier(ByteRange window_phys) const
{
    auto gaps = coverage(window_phys).subtract(window_phys);
    return gaps.empty() ? window_phys.end() : gaps.front().offset;
}

void ReaderExecutor::Display::read(ByteRange window_phys, ChainedBuffers & out, IntervalSet & covered, Stats & out_stats)
{
    /// The caller delivers only the CONTIGUOUS prefix, so an unservable FIRST byte makes the
    /// whole pass moot - and the pass is not free: it would read AND count mid-window
    /// committed islands the caller then discards. The serve-first cycle probes by serving,
    /// and this one-byte gate is what keeps an empty probe costless.
    if (!coversByte(window_phys.offset))
        return;

    /// 1) Resident HIT views: stream contiguous resident runs from the plan's held (pinning)
    ///    readers, the fastest tier at each position (`residentAt`), stopping at the first
    ///    non-resident byte - the later holders take over under the shared `covered` guard.
    ///    Entered only when the window STARTS resident (a hit step), so the classification
    ///    failpoint and the read-latency histogram fire exactly as the old hit path did.
    const auto & geom = plan.geometry();
    if (geom && geom->residentAt(window_phys.offset).resident())
    {
        /// Test hook: pause after residency classified this a hit but before the read, so a
        /// test can drop/evict the cache and verify the plan-pinned segment survives.
        FailPointInjection::pauseFailPoint(FailPoints::reader_executor_pause_after_cache_status);
        StatTimer get_scope(out_stats, Stats::CacheGetMicroseconds);
        for (size_t pos = window_phys.offset; pos < window_phys.end();)
        {
            auto run = geom->residentAt(pos);
            if (!run.resident() || run.entry >= plan.bufs.size()
                || !plan.bufs[run.entry].view)
                break;
            const size_t serve_end = std::min(run.run_end, window_phys.end());
            ChainedBuffers chunk = readHitFromView(*plan.bufs[run.entry].view, ByteRange{pos, serve_end - pos});
            const size_t got = chunk.range().size;
            if (got == 0)
                break;
            out_stats.add(Stats::CacheGetRequests);
            out_stats.add(run.tier == CacheTier::PageCache ? Stats::BytesFromPageCache
                                                           : Stats::BytesFromFilesystemCache, got);
            covered.add(ByteRange{pos, got});
            out.append(std::move(chunk));
            pos += got;
            if (pos < serve_end)
                break;
        }
        HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
            static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
    }

    /// 2) Committed cells (fastest resident tier first, under the shared `covered`).
    recreditCommittedPrefixes(window_phys, out, covered, out_stats);

    /// 3) The bank - bytes a piece fetched that no cell could hold. Served per INTERVAL, the
    ///    exact shape `coverage` claims: the bank can be holey (a sibling-waited middle that
    ///    returned short, a pagecache-tier region between two waited disk cells), and serving
    ///    the intersection of each uncovered gap with each interval keeps `frontier` and `read`
    ///    in agreement - a claimed prefix always serves, never a false empty window.
    ///    No cache counters - the bytes were counted at fetch.
    auto & bank = lane.bank;
    if (!bank.empty())
        for (const auto & iv : bank.getIntervals())
        {
            const size_t lo = std::max(iv.offset, window_phys.offset);
            const size_t hi = std::min(iv.end(), window_phys.end());
            if (lo >= hi)
                continue;
            for (const auto & g : covered.subtract(ByteRange{lo, hi - lo}))
            {
                ChainedBuffers slice = bank.slice(g);
                /// Within one interval the slice covers the gap by construction; the guard
                /// stays so a byte is never marked covered that was not appended.
                if (!slice.covers(g))
                    continue;
                out.append(std::move(slice));
                covered.add(g);
            }
        }
    /// Serving CONSUMES, but only what the caller DELIVERS: trim every bank below the window's
    /// contiguous covered prefix (`serveFromDisplay` slices exactly that prefix). Banked bytes
    /// beyond the first uncovered hole stay banked - they serve a later window once the hole is
    /// fetched - while bytes below the prefix are delivered or held by a faster holder, so the
    /// banked footprint still stays ~one window.
    const auto prefix_gaps = covered.subtract(window_phys);
    const size_t prefix_end_phys = prefix_gaps.empty() ? window_phys.end() : prefix_gaps.front().offset;
    if (prefix_end_phys > window_phys.offset && !bank.empty())
    {
        const ByteRange held = bank.range();
        if (prefix_end_phys > held.offset)
            bank = prefix_end_phys < held.end()
                ? bank.slice(ByteRange{prefix_end_phys, held.end() - prefix_end_phys})
                : ChainedBuffers{};
    }
}

void ReaderExecutor::Display::wait(ByteRange window_phys, ChainedBuffers & out, IntervalSet & covered)
{
    /// No cache-read credit here: the caller banks the sibling bytes and credits them once
    /// (its own committed bytes are dropped - the serve reads and counts them from the cells).
    for (const auto & buf : plan.bufs)
    {
        /// A page cell is filled by promotion at the serve, not downloaded - no downloader,
        /// a wait on it would never wake.
        if (!buf.provider || buf.provider->tier() == CacheTier::PageCache)
            continue;
        for (const auto & w : buf.writers)
        {
            if (!w.writer)
                continue;
            const size_t lo = std::max(w.writer->range().offset, window_phys.offset);
            const size_t hi = std::min(w.writer->range().end(), window_phys.end());
            if (lo >= hi)
                continue;
            for (const auto & u : covered.subtract(ByteRange{lo, hi - lo}))
            {
                ChainedBuffers c = w.writer->waitAndReadSiblingLed(u);
                if (!c.covers(u))
                    continue;   /// no live writer / raced reset / short commit - the caller's fallback fetches it
                out.append(c.slice(u));
                covered.add(u);
            }
        }
    }
}

// ─── Window serve path (cont.): the consumer loop ──────────────────────────

ChainedBuffers ReaderExecutor::serveFromDisplay(ByteRange window)
{
    ChainedBuffers out;
    IntervalSet covered;
    display.read(window, out, covered, stats);
    /// The contiguous served prefix; the next call serves from the first gap.
    auto gaps = covered.subtract(window);
    const size_t prefix_end = gaps.empty() ? window.end() : gaps.front().offset;
    ChainedBuffers chain = out.slice(ByteRange{window.offset, prefix_end - window.offset});
    if (!chain.empty())
        runHandedFills(ByteRange{window.offset, chain.range().size}, chain, stats);
    return chain;
}

ChainedBuffers ReaderExecutor::serveWindow(size_t position_phys)
{
    /// Nothing to serve: the read extent is exhausted (`readCeiling() == 0`) or the plan is
    /// empty. `preparePlan` is the sole scheduler - it (re)plans before every serve when the
    /// position outruns the plan, so there is no reschedule to do here; an empty result is EOF
    /// for this extent.
    if (readCeiling() == 0 || !read_plan.geometry() || read_plan.schedule.serve_runs.empty())
        return {};

    const auto & run = read_plan.schedule.serve_runs[serveRunAt(position_phys)];

    /// ONE window of THIS serve run - never past its end (an embedded faster-tier hit splits
    /// a gap into gap / hit / gap, and reading past the boundary would key the pump to an
    /// ambiguous job; the long connection still coalesces ACROSS runs via the lane's `conn`).
    /// The granularity is SCHEDULE data (`serve_bound`) and only the ASK: the serve returns
    /// whatever contiguous prefix is ready, the rest at the next call. `readCeiling` already
    /// clamps to the file and the extent.
    const size_t want = std::min({readCeiling(), run.output.end() - position_phys, run.serve_bound});
    if (want == 0)
        return {};
    const ByteRange window{position_phys, want};

    /// THE CONSUMER LOOP: consume any ready prefix off the display, else ask the producer
    /// for progress. The serve returns whatever contiguous prefix is ready (the bound is
    /// only the ask; the rest is the next call's) and runs the scheduled handed fills from
    /// the served bytes; an unservable window costs one byte-probe (`Display::read`'s gate),
    /// so the warm path pays no coverage walk. EOF is not display state - the display is a
    /// positional buffer, and "nothing will ever appear here" is knowledge only the producer
    /// has: a false `pump` IS this extent's EOF. The cache is the buffer: a populatable
    /// job's bytes are read back OUT of the committed cells, so a cold miss legitimately
    /// shows BOTH the source fetch and the cache read.
    while (true)
    {
        ChainedBuffers out = serveFromDisplay(window);
        if (!out.empty())
            return out;
        if (!pump(run.require_retrieve, window))
            return {};
    }
}

// ─── Plan build ────────────────────────────────────────────────────────────

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
    /// buffers). The pin is re-established through the NEW buffer at the next collect.
    fill_lane.pin.reset();

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
    /// and remote, foreground and the prefetch worker via the machine's `pressure_snapshot`)
    /// sizes off this cached level instead of re-querying the global monitor per call.
    geom->pressure_level = memoryPressureMonitor().currentLevel();

    /// TRIM: the plan span, bounded to the file end and the read extent. An empty
    /// span (the start already at/past a bound) publishes an empty plan.
    /// New plan epoch: the ahead cursor re-derives from the fresh display truth and the
    /// bank drops with the plan it served - BEFORE the empty-plan early return below, so an
    /// at-bound replan cannot leave the previous epoch's state alive (the seek fast path
    /// keeps the surviving plan, the cursor, AND the bank).
    fill_lane.resetEpoch();

    const ByteRange plan_range = boundedPlanSpan(physical_start);
    if (plan_range.size == 0)
    {
        ReadPlan empty;
        empty.geometry_snapshot = std::move(geom);  /// empty plan; covers()==false
        read_plan = std::move(empty);
        return;
    }
    /// ITERATIVE WINDOWED PROBE with cell-aligned enrichment: a cache reports misses
    /// SEGMENT-aligned, so a miss touching a probe's edge extends past it to the cell
    /// boundary. Probe ONE window, enrich it with those miss extents, and stop as soon as
    /// the enriched end reaches the plan target (`boundedPlanSpan`, ceiling
    /// `plan_look_ahead_max_window`) - a big cell fold can overshoot the target, which is
    /// enough. Otherwise probe the next window AFTER the enriched end and repeat. The
    /// target bounds the PROBING; the enrichment itself stays object-end-bounded only,
    /// exactly as a single-step fold did - so the geometry pass below probes every tier
    /// over the full enriched `probe_range` and a faster tier's coverage of a miss tail
    /// fills the lower cell DOWN (`UpperCacheRead`) rather than the tail being fetched.
    /// KEEP the step views: when no step's enrichment exceeded its probe end the views
    /// tile `probe_range` exactly (each next step starts at the previous end) and the
    /// geometry pass reuses them, cache-major, instead of probing a second time.
    struct ProbeView
    {
        size_t cache_idx;
        StoredObject object;
        size_t object_file_offset;
        ByteRange piece;
        CacheViewPtr view;
    };
    VectorWithMemoryTracking<ProbeView> base_views;
    /// One probe pass over `span`: every tier x object-piece, recording the views and
    /// returning the miss-enriched end (populating tiers only).
    bool step_misses = false;
    const auto probe_span = [&](ByteRange span, VectorWithMemoryTracking<ProbeView> & out)
    {
        size_t enriched_end = span.end();
        size_t piece_file_start = span.offset;
        for (const auto & pr : offset_map.map(span))
        {
            for (size_t ci = 0; ci < caches.size(); ++ci)
            {
                auto v = caches[ci]->planResidencyView(
                    pr.object, piece_file_start - pr.object_offset, ByteRange{piece_file_start, pr.size});
                if (caches[ci]->populatesOnMiss() && !v->misses().empty())
                {
                    step_misses = true;
                    for (const auto & m : v->misses())
                        enriched_end = std::max(enriched_end, m.range.end());
                }
                out.push_back(ProbeView{
                    ci, pr.object, piece_file_start - pr.object_offset,
                    ByteRange{piece_file_start, pr.size}, std::move(v)});
            }
            piece_file_start += pr.size;
        }
        return enriched_end;
    };

    ByteRange probe_range = plan_range;
    bool plan_expanded = false;
    size_t probe_steps = 0;
    {
        const size_t step_size = std::max<size_t>(std::min<size_t>(window_size, plan_range.size), 1);
        size_t cursor = plan_range.offset;
        size_t probe_end = plan_range.offset;
        while (cursor < plan_range.end())
        {
            ++probe_steps;
            const size_t step_end = std::min(cursor + step_size, plan_range.end());
            const size_t enriched_end = probe_span(ByteRange{cursor, step_end - cursor}, base_views);
            /// An enrichment past the step's own end leaves the fold gap unprobed - the
            /// geometry pass must re-probe the whole enriched range.
            plan_expanded |= enriched_end > step_end;
            probe_end = std::max(probe_end, enriched_end);
            if (probe_end >= plan_range.end())
                break;  /// the enriched plan reached (or overshot) the target - enough
            cursor = probe_end;
        }
        probe_range = ByteRange{plan_range.offset, probe_end - plan_range.offset};
    }
    /// Cache SEGMENTATION must not depend on the probe stepping: a full-span probe merges
    /// adjacent aligned misses into one spanning cell, while per-step views would cut them
    /// at every step boundary (fragmenting the append-only cells the clamp orders jobs
    /// around). So the step views are reusable only when they tile the span exactly AND no
    /// populating tier reported a miss across MULTIPLE steps - a single step merges its own
    /// misses, and an all-resident plan has nothing to merge.
    const bool reprobe_full_span = plan_expanded || (step_misses && probe_steps > 1);

    geom->plan_end = probe_range.end();
    ReadPlan plan;

    /// One read-only residency probe (`planResidencyView`) per cache tier per object-piece,
    /// each translated by the two extract helpers into a 1:1 `GeometryEntry`/`BufEntry` pair
    /// (pushed BOTH-or-NEITHER, so `geometry()->entries` and `bufs` stay positionally
    /// aligned — `residentAt`'s entry index maps into `bufs`). `caches` is fastest-first, so
    /// `upper_hits` (the running union of already-processed, faster tiers' hits) lets a
    /// slower tier PRUNE the miss cells a faster tier already holds. The streaming `covered`
    /// guard in the serve path re-establishes the same priority when serving.
    /// Hits fold up to the ceiling OR the expanded `probe_range` end, whichever is larger,
    /// so a hit segment straddling the expanded end folds whole into the plan.
    const size_t resident_clip_end
        = std::max(probe_range.end(), plan_range.offset + effectivePlanCeiling());

    /// The geometry consumes (cache, object, piece, view) tuples CACHE-MAJOR (fastest
    /// first, so `upper_hits` below prunes the slower tiers). Tiling step views are
    /// consumed as recorded (offset-ascending within a cache); an expanded plan re-probes
    /// the enriched range fresh - the step views no longer tile it.
    VectorWithMemoryTracking<ProbeView> fresh;
    if (reprobe_full_span)
        probe_span(probe_range, fresh);
    auto & probed = reprobe_full_span ? fresh : base_views;
    VectorWithMemoryTracking<ProbeView> work;
    work.reserve(probed.size());
    for (size_t ci = 0; ci < caches.size(); ++ci)
        for (auto & pv : probed)
            if (pv.cache_idx == ci)
                work.push_back(std::move(pv));

    IntervalSet upper_hits;
    for (auto & pv : work)
    {
        auto & cache = caches[pv.cache_idx];
        auto view = std::move(pv.view);

        GeometryEntry geom_entry;
        geom_entry.tier = cache->tier();
        geom_entry.head_align = cache->fetchHeadAlignment();
        geom_entry.tail_align = cache->fetchTailAlignment();
        geom_entry.whole_cell = cache->fillsWholeCell();
        BufEntry buf_entry;
        buf_entry.provider = cache.get();

        extractResidentRuns(*view, probe_range, resident_clip_end, geom_entry);
        extractMissesAndOpenWriters(*cache, *view, pv.object, pv.object_file_offset, upper_hits, geom_entry, buf_entry);

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
    }

    chassert(geom->entries.size() == plan.bufs.size());

    /// The cross-cache expansion already pulled the touched MISS segments inside
    /// `[plan_start, plan_end)`, so extend `plan_end` only over a HIT segment straddling the
    /// expanded end (fewer replans): no dead zone, and the schedule may cover the whole span
    /// because a miss tail a faster tier holds is filled DOWN (`UpperCacheRead`), not fetched.
    {
        size_t hit_end = geom->plan_end;
        for (const auto & e : geom->entries)
            for (const auto & r : e.resident)
                hit_end = std::max(hit_end, r.end());
        geom->plan_end = hit_end;
    }

    /// Publish atomically: `geometry()` and `bufs` are one object (`read_plan`), so a
    /// reader can never see new geometry against a stale buffer vector. Assigning
    /// `read_plan` finalizes the previous plan's write buffers and runs its deferred
    /// LRU bumps.
    plan.geometry_snapshot = std::move(geom);
    read_plan = std::move(plan);

    /// Describe the plan's work once, here. The request for fill purposes is the
    /// whole plan span from the cursor: everything from `plan_start` forward is
    /// read by the scan (User), so only the alignment slack around it is
    /// FillOnly. `schedule.retrieves[*].into` then drives `runPutStep` so a
    /// faster tier never receives slack bytes (see `ReadPlan::schedule`).
    /// The User range is the whole extended span: the scan reads through the folded hit
    /// tail as the cursor advances, so the schedule must emit serve steps across
    /// `[plan_start, plan_end)`. The fold is all resident, so this adds no fetch -- the
    /// gaps to fetch still lie only within the base request.
    const ByteRange schedule_request{plan_range.offset, read_plan.geometry()->plan_end - plan_range.offset};
    read_plan.schedule = buildSchedule(
        *read_plan.geometry(),
        schedule_request,
        min_bytes_for_seek,
        effectiveWindowSize(read_plan.geometry()->pressure_level),
        effectiveBlockSize(read_plan.geometry()->pressure_level));

    /// Feed this plan's predicted source reads into the continuity estimator so its
    /// reach prediction (which sizes long source connections) stays current.
    feedScheduleToContinuity(read_plan.schedule);

    /// A plan with no `Source::Remote` retrieve is served entirely from cache; the
    /// prefetch look-ahead has nothing to launch.
    read_plan.has_remote_retrieves = std::any_of(
        read_plan.schedule.retrieves.begin(), read_plan.schedule.retrieves.end(),
        [](const auto & r) { return r.source == PlanSchedule::Source::Remote; });

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
        continuity_tracker.recordReadRange(start, range.end() - start);
        continuity_fed_end = range.end();
    }
}

bool ReaderExecutor::planReachesEnd() const
{
    return read_plan.geometry() && !offset_map.hasUnknownSize()
        && read_plan.geometry()->plan_end >= offset_map.totalSize();
}

size_t ReaderExecutor::effectivePlanCeiling() const
{
    /// A fixed plan window: at least `window_size`, raised to `plan_look_ahead_max_window`
    /// when configured larger. Not pressure-scaled - the window is small by default, and
    /// segment folding only ever adds the touched cells within it.
    return std::max(window_size, plan_look_ahead_max_window);
}

ByteRange ReaderExecutor::boundedPlanSpan(size_t physical_start) const
{
    const size_t ceiling = effectivePlanCeiling();
    size_t want = 0;
    if (is_transient)
    {
        /// A one-shot `readBigAt` transient: the request span IS the plan base. Segment
        /// folding still expands it so the touched cache cells are filled to their
        /// boundaries, but the base does NOT inflate to `window_size` when the request
        /// is smaller.
        chassert(read_extent_end);
        const size_t physical_extent_end = toPhys(*read_extent_end);
        if (physical_start >= physical_extent_end)
            return ByteRange{physical_start, 0};
        want = std::min(physical_extent_end - physical_start, ceiling);
    }
    else
    {
        /// The plan TARGET the iterative windowed probe grows toward. Independent of
        /// `read_extent_end` (which only clamps the serve), so the plan survives
        /// mark-range advances and is reused. An UNKNOWN-SIZE source is not planned past
        /// one window: the probe would tile cache segments beyond the real EOF only for
        /// the first short read to invalidate them.
        want = offset_map.hasUnknownSize() ? std::min(window_size, ceiling) : ceiling;
    }
    /// Segment folding then extends `want` (in `observeAndSchedule`) and the same
    /// ceiling caps the result; the file end is the only natural bound below it.
    if (!offset_map.hasUnknownSize())
    {
        const size_t physical_end = offset_map.totalSize();
        if (physical_start >= physical_end)
            return ByteRange{physical_start, 0};
        want = std::min(want, physical_end - physical_start);
    }
    return ByteRange{physical_start, want};
}

void ReaderExecutor::extractResidentRuns(const CacheView & view, ByteRange plan_range, size_t resident_clip_end, GeometryEntry & geom_entry)
{
    for (const auto & hit : view.hits())
    {
        /// Hits are segment-aligned and may extend past the plan span. Clamp the left
        /// at `plan_start` so streaming never reads behind the cursor; the right bound
        /// is `resident_clip_end` (the larger of the probe-range end and the
        /// pressure-scaled plan ceiling), which folds a whole hit segment straddling
        /// the probe edge into the plan instead of clipping it.
        const size_t lo = std::max(hit.range.offset, plan_range.offset);
        const size_t hi = std::min(hit.range.end(), resident_clip_end);
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
    /// fetch and the over-read bound. PRUNE any cell fully covered by a
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

// ─── Machine lifecycle ─────────────────────────────────────────────────────

void ReaderExecutor::cancelMachine(bool cancelled)
{
    drainAbandonedMachines();

    auto m = std::move(machine);
    if (!m)
        return;
    /// The global `machine` was just moved out above, so `machineFor` reports no machine for
    /// this retrieve from here on; the bank stays valid - the cursor has not moved
    /// (`setReadExtent`), or a seek re-plans and rebuilds it (see `seek`).

    LOG_TRACE(log, "Prefetch: discarding [{}, {})",
        toLogical(m->physical_window.offset), toLogical(m->physical_window.end()));

    if (collectRunner().tryCancelQueued(*m))
    {
        /// The worker never ran - reclaim the carried connection (pristine). A seek
        /// keeps it (the read funnel decides bridge-or-reopen later); the destructor
        /// accounts it if still held.
        fill_lane.reclaim(*m);
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
        /// Already running: interrupt it, then JOIN it before tearing anything down. The worker
        /// writes the shared `read_plan.bufs` writers (its led segments) on the pool thread, so
        /// the foreground must NOT free the plan (the caller re-plans / drops the extent / seeks
        /// right after this) until the worker has finished and completed every elected segment -
        /// else the writer dtor aborts on a leaked DOWNLOADING segment
        /// (`chassert(!is_last_holder)`) or the worker writes into freed memory. The interrupt
        /// makes the worker wrap at its next block, so the wait is bounded.
        stats.add(Stats::PrefetchDiscardedRunning);
        collectRunner().requestInterrupt(*m);
        collectRunner().waitReleased(*m);
        if (m->failure)
            tryLogException(m->failure, log, "Cancelled prefetch task threw", LogsLevel::debug);
        /// Reconcile the joined machine HERE: its fetch really happened, so fold the stats,
        /// attribute the issued bytes to wasted (the chain is never collected), and account
        /// the forfeited long connection on this query-attached thread (left to the machine's
        /// shared_ptr, a detached owner would leak `DiskConnectionsReset` off-query). Never
        /// drain - this is reachable from the noexcept destructor. No longer LENT: the lane
        /// may open a fresh one.
        stats += m->stats;
        stats.add(Stats::PrefetchWastedSourceBytes, m->stats.get(Stats::PrefetchIssuedSourceBytes));
        accountLongConnectionDrop(m->long_conn, /*at_eof=*/m->reached_eof, stats);
        m->long_conn.reset();
        fill_lane.conn_lent = false;
    }
}

void ReaderExecutor::drainAbandonedMachines(bool wait_finished)
{
    /// Only QUEUED-REVOKED machines are stashed (`tryCancelQueued` won): their stats are
    /// zero and their connection was reclaimed at the revoke, so the reap is just the join
    /// - the pool's no-op pickup must resolve before the executor state is freed.
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

// ─── Sizing / bounds ───────────────────────────────────────────────────────

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

bool ReaderExecutor::prefetchEnabled(MemoryPressureLevel level) const
{
    /// Prefetch reads the same window as a synchronous read; under High/Critical it
    /// is suppressed entirely rather than shrunk.
    return PREFETCH_ENABLED[static_cast<size_t>(level)];
}

size_t ReaderExecutor::fillAheadLead(MemoryPressureLevel) const
{
    /// ONE uniform deep lead, self-limited by cell acceptance instead of a tier gate: the
    /// worker retains only bytes no cell accepted, capped at one window
    /// (`coordinatedPrefetch`'s residue cap). A populating tier runs the full lead in its
    /// cells; a bottom that accepts nothing - read-only tier, bypass gap, full cache - hits
    /// the cap ~a window into the lead and stops, emergently reproducing the one-window
    /// cadence; the pump's stall-join interrupt bounds a consumer wait to one tile.
    return fill_ahead_lead;
}

size_t ReaderExecutor::clampToExtent(size_t win_size) const
{
    if (!read_extent_end)
        return win_size;
    const size_t remaining = *read_extent_end > position ? *read_extent_end - position : 0;
    return std::min(win_size, remaining);
}

size_t ReaderExecutor::boundedFetchSize(size_t want) const
{
    /// PRODUCER-side clamp: an arbitrary fetch ask (the fill-ahead lead, the launch probe)
    /// bounded by what exists (the known file remainder) and what may be touched (the
    /// extent). Deliberately NOT capped at the serving horizon - the run-ahead may exceed
    /// one window; the consumer's ceiling is `readCeiling`.
    if (!offset_map.hasUnknownSize())
        want = std::min(want, totalSize() - position);
    return clampToExtent(want);
}

}
