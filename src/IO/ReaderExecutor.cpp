#include <IO/ReaderExecutor.h>
#include <IO/ResidencyIterator.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/FetchMachineRunner.h>
#include <IO/LocalFetchMachineRunner.h>
#include <IO/FiberFetchMachineRunner.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Cache/EncryptionHeaderCache.h>
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
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int SUPPORT_IS_DISABLED;
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
/// PLAN BUILD, two spans: `preparePlan` (the plan scheduler, in Read path)
///   and `observeAndSchedule` + its extract* helpers.
/// COLLECT, three spans: `collectInFlightInto`; the put pair `collectFillTargets` /
///   `runPutStep`; and teardown's `cancelMachine` / `drainAbandonedMachines`.
/// DISPLAY read surface, two spans: the `Display` methods, plus the plan-view
///   hit serve `readHitFromView` they join at `Display::read`.
/// PRODUCER: `coordinatedPrefetch` (machine fetch step, with its led-run merge
///   `mergeRanges`) -> `fetchWindowFromSource` -> `readFromSource` / the Long
///   connection region; fills land through `writeSliceToWriter`; deferred puts
///   run at collect.
/// ──────────────────────────────────────────────────────────────────────────────


ReaderExecutorFetchMachine::ReaderExecutorFetchMachine()
    : inflight_gauge(CurrentMetrics::ReaderExecutorPrefetchInFlight)
{
}

namespace
{

/// Selects the async read-ahead runner: a Silk fiber runner when requested (Silk build only -
/// otherwise fails fast, no silent fallback to the pool), else the pool runner when a pool was
/// provided, else null (no async read-ahead).
std::unique_ptr<IFetchMachineRunner> makeReadAheadRunner(
    bool use_fiber_runner, const std::shared_ptr<PrefetchThreadPool> & prefetch_pool)
{
#if USE_SILK
    if (use_fiber_runner)
        return std::make_unique<FiberFetchMachineRunner>();
#else
    if (use_fiber_runner)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Reader executor fiber runner requires a build with Silk");
#endif
    if (prefetch_pool)
        return std::make_unique<PoolFetchMachineRunner>(prefetch_pool);
    return nullptr;
}

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
    , encryption_header_cache(std::move(options.encryption_header_cache))
    , min_bytes_for_seek(options.min_bytes_for_seek)
    , block_size(options.block_size)
    , max_tail_for_drain(options.max_tail_for_drain)
    , plan_look_ahead_max_window(options.plan_look_ahead_max_window)
    , long_connection_open_range(options.long_connection_open_range)
    , long_connection_max_bound(options.long_connection_max_bound)
    , fill_ahead_lead(options.fill_ahead_lead)
    , prefetch_pool(std::move(options.prefetch_pool))
    , runner(makeReadAheadRunner(options.use_fiber_runner, prefetch_pool))
    , local_runner(std::make_unique<LocalFetchMachineRunner>())
    , long_connection_limit(std::move(options.long_connection_limit))
    , reader_executor_log(std::move(options.reader_executor_log))
    , active_metric(CurrentMetrics::ReaderExecutorActive)
{
    if (window_size == 0 || block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "reader_executor_window_size and reader_executor_block_size must be > 0, "
            "got window_size={}, block_size={}", window_size, block_size);
    fill_lane.bank_keep_behind = min_bytes_for_seek;

    offset_map.build(stored_objects);
    creator_query_id = String(CurrentThread::getQueryId());
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, block_size={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, block_size, caches.size());

    /// Keep the estimator's continuity gap in lockstep with the executor's seek
    /// bound, so a bridged gap feeds the same whether modeled as a read or a seek.
    ReadContinuityTracker::Options continuity_options;
    continuity_options.bridgeable_gap = min_bytes_for_seek;
    fetch_tracker = ReadContinuityTracker(continuity_options);
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
        "prefetch_wait_us={} work_us={} "
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
        stats.get(Stats::PrefetchWaitMicroseconds), stats.get(Stats::WorkMicroseconds),
        stats.get(Stats::PrefetchHits), stats.get(Stats::PrefetchCancelled), stats.get(Stats::PrefetchPoolFull),
        stats.get(Stats::PrefetchDiscardedRunning),
        stats.get(Stats::PrefetchIssuedSourceBytes),
        stats.get(Stats::PrefetchWastedSourceBytes),
        stats.get(Stats::IncompleteConnections));

    if (reader_executor_log)
    {
        /// `SystemLogQueue` allocates and can throw; this is a `noexcept`
        /// destructor (often unwinding from another exception), so suppress and log
        /// rather than `std::terminate`. The log row is best-effort observability.
        /// The fields are filled inside the callback so the string copies are charged
        /// to the global memory tracker, per the `add` contract.
        try
        {
            reader_executor_log->add([&](ReaderExecutorLogElement & elem)
            {
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
                elem.prefetch_hits = stats.get(Stats::PrefetchHits);
                elem.prefetch_cancelled = stats.get(Stats::PrefetchCancelled);
                elem.prefetch_pool_full = stats.get(Stats::PrefetchPoolFull);
                elem.prefetch_discarded_running = stats.get(Stats::PrefetchDiscardedRunning);
                elem.prefetch_issued_source_bytes = stats.get(Stats::PrefetchIssuedSourceBytes);
                elem.prefetch_wasted_source_bytes = stats.get(Stats::PrefetchWastedSourceBytes);
            });
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

    preparePlan(position_phys);
    return finishWindow(serveWindow(position_phys));
}

void ReaderExecutor::preparePlan(size_t position_phys, size_t coverage_ahead)
{
    const bool at_plan_end = read_plan.geometry() && position_phys >= read_plan.geometry()->plan_end;

    /// At the boundary, collect the in-flight machine BEFORE replanning. A consumed step at
    /// `plan_end` serves nothing, so the serve's gap branch never collects it; without this the
    /// replan stays blocked (machine still set -> `observeAndSchedule`'s `chassert(!machine)`)
    /// and the executor stalls at `plan_end` - a premature interior EOF. Collecting commits the
    /// machine's cells (so the replan below sees them resident) and clears `machine`.
    if (machine && at_plan_end)
        collectInFlightInto();

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
        /// EXTEND vs RESTART: a contiguous-forward need - the cursor still inside
        /// (or at the end of) the live span, only more look-ahead wanted - grows
        /// the plan in place, keeping the held buffers, the bank and the fill-lane
        /// state; each extension also SLIDES the released territory out, so the
        /// retained span stays bounded by the reuse reach plus the plan window.
        /// Anything else (no plan, backward cursor) rebuilds from scratch.
        const auto & g = read_plan.geometry();
        const bool live_plan = g && g->plan_end > g->plan_start;
        const bool contiguous_forward = live_plan
            && position_phys >= g->plan_start && position_phys <= g->plan_end;
        if (contiguous_forward)
            extendPlan(position_phys);
        else
            observeAndSchedule(position_phys);
    }
}

ChainedBuffers ReaderExecutor::finishWindow(ChainedBuffers chain)
{
    stats.add(Stats::RequestedBytes, chain.range().size);
    /// Feed the consumption estimator with what was actually served (physical space).
    if (chain.range().size)
    position += chain.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        chain.range().size, chain.getNodes().size(), position);

    /// Unknown-size EOF is latched by a short read here, not the pre-read gate, and the caller
    /// stops on the empty chain without a follow-up call - so drop the in-flight fill pin now
    /// rather than leaking it.
    if (reached_eof)
        fill_lane.pin.reset();

    prefetch();

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

    /// Compare on the PHYSICAL side: a cell-aligned machine window on an encrypted file
    /// can start BELOW the header (the header bytes are part of the first cell), where no
    /// logical coordinate exists.
    const size_t new_physical = toPhys(new_position);
    if (machine
        && new_physical >= machine->physical_window.offset
        && new_physical < machine->physical_window.end())
    {
        LOG_TRACE(log, "seek: target within prefetch (physical [{}, {})), keeping prefetch",
            machine->physical_window.offset, machine->physical_window.end());
        position = new_position;
        return;
    }

    /// REUSE: a NEAR target inside the live plan span changes only the cursor - the
    /// plan, the held buffers, the bank and the lane state all stay (a backward
    /// target re-serves committed cells, which the serve reads ahead-cursor-blind).
    /// NEAR inherits the connection-bridge policy: a jump the open connection would
    /// bridge (`min_bytes_for_seek`) is a jump the plan absorbs - the interleaved
    /// column-stream pattern of a compact merge. A FAR in-plan jump restarts
    /// instead: the connection reopens at the target anyway, and a fresh plan
    /// derives its fill cells from the new cursor rather than pumping the old
    /// whole-gap job (and its fuller cells) across the distance. The machine is
    /// left untouched wherever it is: a swing back into its window serves as a
    /// prefetch hit, and the serve's pump interrupt-collects it lazily - and
    /// partially - only when it actually blocks a needed job. An eager
    /// collect-on-seek here costs a completed-then-relaunched fetch per swing.
    /// The estimator is NOT fed: an absorbed jump is not a break in the fetch
    /// trajectory - recording it would collapse the predicted reach and shrink
    /// the long-connection sizing at every swing.
    const size_t cur_physical = toPhys(position);
    const size_t seek_distance
        = new_physical >= cur_physical ? new_physical - cur_physical : cur_physical - new_physical;
    if (const auto & g = read_plan.geometry();
        g && g->plan_end > g->plan_start && new_physical >= g->plan_start && new_physical < g->plan_end
        && seek_distance <= min_bytes_for_seek)
    {
        LOG_TRACE(log, "seek: target within plan (physical [{}, {})), reusing plan",
            g->plan_start, g->plan_end);
        position = new_position;
        reached_eof = false;
        prefetch();
        return;
    }

    cancelMachine(/*cancelled=*/true);
    /// Feed the seek to the reach estimator; it resets its frontier, so the
    /// post-seek plan's predicted reads feed from here.
    fetch_tracker.recordSeek(new_physical);

    /// A seek away from the current frontier strands the in-flight fill segment;
    /// drop its pin (the next window re-establishes it).
    fill_lane.pin.reset();

    position = new_position;
    reached_eof = false;
    /// A far jump invalidates the plan: bytes banked AHEAD of the old cursor
    /// would, after the jump, sit disjoint from the new one. Drop the plan AND the lane's
    /// plan-scoped state here - by ownership, not by trusting the next restart to reset it (the
    /// executor can go idle at EOF holding a stale bank otherwise). (The fast paths above
    /// keep the plan, the cursor, and the bank for a target inside the in-flight window
    /// or inside the live plan span - RESTART is only for a target outside both.)
    read_plan = {};
    fill_lane.resetOnRestart();

    prefetch();
}

void ReaderExecutor::setRequestMap(std::vector<std::pair<size_t, size_t>> ranges)  // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    /// Stored PHYSICAL: planning (`observeSpan`) works in physical space, and
    /// `toPhys` is a constant shift, so sizes are preserved.
    request_map = {};
    for (const auto & [offset, size] : ranges)
        if (size)
            request_map.add(ByteRange{toPhys(offset), size});
    request_map_set = !ranges.empty();
}

void ReaderExecutor::setReadBound(std::optional<size_t> logical_end)
{
    /// Monotone-max: an advance never invalidates an in-flight read-ahead - one
    /// GET keeps spanning the mark ranges - and a backward value is absorbed
    /// (the bound reflects the true end already). Clearing (read-to-EOF) lifts
    /// the bound to the file end.
    if (!logical_end)
        read_bound.reset();
    else if (!read_bound || *read_bound < *logical_end)
        read_bound = logical_end;
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
    t->read_bound = start_position + read_size;
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
    [[maybe_unused]] KeyFinderFunc key_finder)
{
#if USE_SSL
    decryptor.addLayer(std::move(path), std::move(key_finder));
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

    /// Under size-unknown sources `fetchWindowFromSource` latches `reached_eof`
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
            auto view = probeView(*cache, pieces.front().object, /*object_file_offset=*/0, header_range);
            if (view->allHit())
            {
                ChainedBuffers chain;
                for (const auto & hit : view->hits())
                {
                    if (!hit.reader)
                        continue;
                    const size_t lo = std::max(hit.range.offset, header_range.offset);
                    const size_t hi = std::min(hit.range.end(), header_range.end());
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

    /// Missed every tier: the global header cache (server-wide, keyed by remote path)
    /// skips the source read; the populate below still runs, so the first cell's
    /// append-only prefix commits. The size check guards against a stale entry from a
    /// differently-layered file at the same path.
    ChainedBuffers fetched;
    if (encryption_header_cache && pieces.size() == 1)
    {
        if (auto cached = encryption_header_cache->read(pieces.front().object.remote_path);
            cached && cached->size() == data_start_offset)
        {
            auto cached_block = std::make_shared<OwnedChainedBuffer>(data_start_offset);
            std::memcpy(cached_block->data(), cached->data(), data_start_offset);
            fetched.append(ChainedBufferNode{std::move(cached_block), 0, data_start_offset, 0});
        }
    }

    /// Miss: one one-shot source read (no long connection, no plan exists yet).
    if (fetched.empty())
    {
        fetched = fetchWindowFromSource(header_range,
            /*from_prefetch=*/false, reached_eof, MemoryPressureLevel{}, /*bound_advertised=*/true,
            /*lc=*/nullptr, /*stop=*/nullptr, stats);
        if (encryption_header_cache && pieces.size() == 1 && fetched.totalBytes() == data_start_offset)
        {
            String header_bytes(data_start_offset, '\0');
            fetched.copyTo(header_bytes.data(), header_range);
            encryption_header_cache->write(pieces.front().object.remote_path, std::move(header_bytes));
        }
    }

    /// Populate the incrementally-fillable tiers so the first cell's append-only prefix
    /// commits and the following data writes can continue from `data_start_offset`.
    if (fetched.totalBytes() == data_start_offset)
    {
        for (auto & [cache, view] : populate_views)
        {
            for (const auto & m : view->misses())
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

// ─── Window serve path - collect ────────────────────────────────────────────

bool ReaderExecutor::waitPublishedTile(FetchMachine & m, size_t phys)
{
    StatTimer wait_scope(stats, Stats::PrefetchWaitMicroseconds);
    std::unique_lock lock(m.published_mutex);
    if (!m.publish_started)
        return false;   /// may never run (queued cancel, stashed) - the interrupt path owns it
    const auto covered = [&] { return m.published.covers(ByteRange{phys, 1}); };
    while (!m.publish_done && !covered())
        m.published_cv.wait(lock);
    return covered();
}

void ReaderExecutor::collectInFlightInto()
{
    const size_t ri = machine->retrieve_index;
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
    LOG_TRACE(log, "collect: waiting on prefetched (physical [{}, {}))",
        m->physical_window.offset, m->physical_window.end());
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
    /// (An interrupt-short return never latches it - see `fetchWindowFromSource`.)
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
        /// the connection is reclaimed (above), the caller reads synchronously. Account
        /// it like a queued revoke - the read-ahead ran but delivered nothing - so every
        /// launched machine lands in exactly one of Hits/PartialCollects/Cancelled.
        if (m->fetched.empty() && fetched_end <= m->physical_window.offset)
        {
            stats.add(Stats::PrefetchCancelled);
            return;
        }

        /// A fetched prefix that cannot serve the cursor (extension-only bytes below the
        /// requested range, or a kept seek moved past it) is already in its cells (the
        /// worker commits per tile); retry only the refused residue into the writers,
        /// then let the caller read synchronously - serving an empty window here would
        /// read as a false EOF upstream.
        if (fetched_end <= toPhys(position))
        {
            runPutStep(*m, m->fetched);
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
    runPutStep(*m, residue);   /// `m` stays alive - the still-refused scan below reads its views

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
    /// route to the display. Banked bytes are memory-held until consumed; the launcher
    /// budgets new lead against them (`bankAheadBytes`), so the no-commit paths run a
    /// one-window cadence instead of banking the whole lead.
    if (!collected.empty())
    {
        if (r.into.empty() || !display.covers(collected.range()))
            fill_lane.bank.append(std::move(collected));
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

/// The end of the covered prefix of `range` in `cov`: the first uncovered byte,
/// or `range.end()` when the whole range is covered.
static size_t coveredPrefixEnd(const IntervalSet & cov, ByteRange range)
{
    auto gaps = cov.subtract(range);
    return gaps.empty() ? range.end() : gaps.front().offset;
}

/// Pin the in-flight segment under `frontier` if `writer` covers it - the one
/// statement of the pin rule; the writer lists differ per call site.
static CacheWriter::CacheSegmentPin pinIfCovering(CacheWriter * writer, size_t frontier)
{
    if (writer && frontier >= writer->range().offset && frontier < writer->range().end())
        return writer->pin(frontier);
    return {};
}

/// Cell-edge fetch shaping: the widest `into` cell STRICTLY containing `pos`
/// extends a piece cut to its edge, so a touched cache cell is fetched whole.
/// `pos` on a cell boundary (or an `into`-empty bypass job) stays put - a
/// bypass job reads exactly the requested bytes.
static size_t cellFloor(const PlanSchedule::Retrieve & r, size_t pos)
{
    size_t off = pos;
    for (const auto & t : r.into)
        if (t.cell.offset < pos && pos < t.cell.end())
            off = std::min(off, t.cell.offset);
    return off;
}

/// The fetch tail's cap at `end`: a WHOLE-CELL target (page cache) extends to
/// its cell edge - a partial block cannot be put, so capping would lose the
/// fill entirely; an incremental cell extends only while the overhang fits one
/// window (the historical read-ahead), else the tail caps at `end` and the
/// cell's remainder fills on later windows through the plan-held writer. With
/// several straddling targets the widest required cap wins.
static size_t cellTailCap(const PlanSchedule::Retrieve & r, size_t end, size_t window_size)
{
    size_t cap = end;
    for (const auto & t : r.into)
        if (t.cell.offset < end && end < t.cell.end())
            if (t.whole_cell || t.cell.end() - end <= window_size)
                cap = std::max(cap, t.cell.end());
    return cap;
}

static size_t cellCeil(const PlanSchedule::Retrieve & r, size_t pos)
{
    size_t end = pos;
    for (const auto & t : r.into)
        if (t.cell.offset < pos && pos < t.cell.end())
            end = std::max(end, t.cell.end());
    return end;
}

// ─── The display (cont.): plan-view hit serve ──────────────────────────────

/// Serve a clamped resident sub-range from a held probe view's hit read
/// buffers: find each `HitEntry` overlapping `clamped`, read the overlap from its
/// re-readable buffer, and append the pieces. A hit is readable in full (the probe
/// splits partial segments at their write offset), so the result is contiguous from
/// `clamped.offset`, short only at the TAIL (`Display::read` marks `range().size` bytes
/// covered, so a mid-range hole would over-mark coverage). Records each `read` on the
/// view for the deferred LRU bump.
ChainedBuffers ReaderExecutor::readHitFromView(CacheView & view, ByteRange clamped)
{
    ChainedBuffers out;
    for (const auto & hit : view.hits())
    {
        if (!hit.reader)
            continue;
        const size_t lo = std::max(hit.range.offset, clamped.offset);
        const size_t hi = std::min(hit.range.end(), clamped.end());
        if (lo >= hi)
            continue;
        out.append(hit.reader->read(ByteRange{lo, hi - lo}));
    }
    return out;
}

// ─── Machine fetch step ────────────────────────────────────────────────────

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
        /// costs about the same as over-reading it.
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
    {
        std::lock_guard lock(m.published_mutex);
        m.publish_started = true;
    }
    SCOPE_EXIT_SAFE({ m.fetched = std::move(led_bytes); });
    SCOPE_EXIT_SAFE({
        std::lock_guard lock(m.published_mutex);
        m.publish_done = true;
        m.published_cv.notify_all();
    });

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
    VectorWithMemoryTracking<ByteRange> led_misses;   /// the uncommitted tails WE won -> fetch here
    IntervalSet available_cov;      /// committed prefixes: read from cache, ACCOUNTED as covered - never fetched
    IntervalSet writer_coverage;    /// union of fill-target writer overlaps in the window
    VectorWithMemoryTracking<CacheWriter::FillClaim> claims;
    for (const auto & view : m.writer_views)
    {
        if (!view.writer)
            continue;
        const size_t lo = std::max(window.offset, view.writer->range().offset);
        const size_t hi = std::min(window.end(), view.writer->range().end());
        if (lo >= hi)
            continue;
        writer_coverage.add(ByteRange{lo, hi - lo});
        auto fill_claim = view.writer->claim(ByteRange{lo, hi - lo});
        for (const auto & r : fill_claim.available)
            available_cov.add(r);
        for (const auto & r : fill_claim.to_fetch)
            led_misses.push_back(r);
        claims.push_back(std::move(fill_claim));
    }

    /// A window byte no fill-target writer covers cannot be deduped (no cache tier populates
    /// it): fetch it plainly.
    for (const auto & g : writer_coverage.subtract(window))
        led_misses.push_back(g);

    /// CONTENDED = the window minus what is `available` (committed, read from cache) and what we
    /// fetch: the uncommitted tails a sibling leads. We neither fetch nor wait on them - `available`
    /// is progress accepted as-is; a leftover contended gap records contention so the collect
    /// revokes to the sync path (a sparse window must not be assembled here) and the foreground
    /// resolves it.
    IntervalSet resolved = available_cov;
    for (const auto & r : led_misses)
        resolved.add(r);
    const auto contended = resolved.subtract(window);
    m.contended = !contended.empty();

    /// "Stop at the first loss" (inline serve only): fetch just the contiguous prefix up to the
    /// first contended byte; the serve returns that prefix short and the next read resolves the
    /// boundary. A pool worker fetches the whole led set (fetch_bound = window end).
    size_t fetch_bound = window.end();
    if (m.inline_serve)
        for (const auto & c : contended)
            fetch_bound = std::min(fetch_bound, c.offset);

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
    /// them inline (we hold the claims). A WIDE sibling-led hole between two led runs breaks
    /// the connection - those bytes are not ours to fetch; holes below `min_bytes_for_seek`
    /// are bridged by `mergeRanges` (cheaper to read through than to reopen).
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
        const size_t step = std::max<size_t>(effectiveWindowSize(level), 1);
        for (size_t off = led.offset; off < run_hi && !m.reached_eof; off += step)
        {
            const ByteRange piece{off, std::min(step, run_hi - off)};
            ChainedBuffers run = fetchWindowFromSource(piece, /*from_prefetch=*/true, m.reached_eof, level,
                m.bound_advertised, m.inline_serve ? &fill_lane.conn : &m.long_conn, &m, m.stats);
            pushChainToWriters(m.writer_views, piece, run, m.stats);
            if (!run.empty())
                m.fetched_end = std::max(m.fetched_end, run.range().end());
            for (const auto & keep : uncommittedIn(m.writer_views, piece))
                led_bytes.append(run.slice(keep));
            if (!led_bytes.empty())
            {
                /// Refresh the published preview (a slice-copy of the whole
                /// residue: nodes share the payload blocks, so this is pointer
                /// work) - the display serves from it while the flight runs.
                ChainedBuffers preview = led_bytes.slice(led_bytes.range());
                std::lock_guard lock(m.published_mutex);
                m.published = std::move(preview);
                m.published_cv.notify_all();
            }
            residue_full = led_bytes.totalBytes() >= residue_cap;
            if (residue_full || m.interrupt_requested.load(std::memory_order_relaxed))
                break;  /// stop-short; the scope guard still finishes every elected segment
        }
        if (residue_full || m.interrupt_requested.load(std::memory_order_relaxed))
            break;
    }
}

// ─── Gap fetch + backfill ──────────────────────────────────────────────────

/// The cooperative stop probe. The policy lives at the call sites: a LONG
/// connection's drain stops at block granularity (the channel keeps its
/// frontier - nothing is forfeited); a one-shot GET is never cut mid-response
/// (its request would be forfeited and the remainder would pay a fresh one) -
/// one-shot fetches stop only BETWEEN connections.
static bool stopRequested(const MachineBase * stop)
{
    return stop && stop->interrupt_requested.load(std::memory_order_relaxed);
}

ChainedBuffers ReaderExecutor::fetchWindowFromSource(ByteRange physical_window, bool from_prefetch,
    bool & eof_latch, MemoryPressureLevel pressure_level, bool bound_advertised,
    std::optional<LongConnection> * lc, const MachineBase * stop, Stats & out_stats)
{
    /// FOREGROUND context iff the caller passed the LANE's slot (see the header doc).
    const bool may_open_long = lc == &fill_lane.conn;
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
        LOG_TRACE(log, "fetchWindowFromSource: source read object={}, offset={}, size={}",
            pr.object.remote_path, pr.object_offset, pr.size);

        /// The IN-FETCH opener: at each OBJECT-piece start, open a long connection
        /// when it is convenient (`openLongConnectionIfWarranted`: the predicted reach warrants one, a
        /// slot is free, and a held unusable channel is dropped first) - so a run that crosses
        /// an object boundary opens the tail object's connection with ITS own reach. Only in
        /// FOREGROUND context (`may_open_long`: the foreground itself and INLINE machines, which
        /// run on the serve thread) - a pool worker never opens in-fetch; it carries what its
        /// LAUNCH gave it (`launchMachineForWindow`, the other opener site). The policy operates
        /// on the lane's slot, which for every foreground caller IS `lc`.
        if (may_open_long)
            openLongConnectionIfWarranted(pr.object, pr.object_offset, file_pos, out_stats);

        /// No head/tail-extension splits: the window IS the fetch range (the cache
        /// `getOrSet` segment-aligned the miss at plan build, in `resolve`).
        auto blocks = allocateBlocks(pr.size, window_block_size);
        StatTimer src_scope(out_stats, Stats::SourceReadMicroseconds);
        ChainedBuffers source_chain = readFromSource(pr.object, pr.object_offset, std::move(blocks), file_pos,
            bound_advertised, lc, stop, out_stats);
        HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
            static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
        const size_t actual = source_chain.totalBytes();
        out_stats.add(Stats::BytesFromSource, actual);
        if (from_prefetch)
            out_stats.add(Stats::PrefetchIssuedSourceBytes, actual);
        result.append(std::move(source_chain));
        file_pos += pr.size;

        /// The BETWEEN-CONNECTIONS stop point (and the post-hoc classifier for a
        /// long-connection stop-short return): checked FIRST so a stop-short neither latches
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
    for (const auto & buf : plan.tiers)
    {
        if (!buf.provider)
            continue;
        const bool is_page = buf.provider->tier() == CacheTier::PageCache;
        const Stats::Counter tier_counter = is_page ? Stats::BytesFromPageCache : Stats::BytesFromFilesystemCache;
        for (const auto & w : buf.view->misses())
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
    bool bound_advertised, std::optional<LongConnection> * lc,
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
        /// reopen would cost, but the connection is no longer abandoned mid-run as an incomplete,
        /// and no byte is drained-and-refetched). A bound falling INSIDE a block re-cuts that
        /// block at the bound (an exact-span piece plus the remainder) - reach-predicted bounds
        /// are arbitrary byte values, so the mid-block case is the common one.
        bool split = false;
        if ((*lc)->servesObject(object.remote_path)
            && (*lc)->canStartServing(offset, min_bytes_for_seek))
        {
            const size_t prefix_span = (*lc)->read_until - offset;
            size_t prefix_bytes = 0;
            size_t n = 0;
            while (n < blocks.size() && prefix_bytes + blocks[n]->size() <= prefix_span)
                prefix_bytes += blocks[n++]->size();
            std::shared_ptr<OwnedChainedBuffer> recut_head;
            std::shared_ptr<OwnedChainedBuffer> recut_tail;
            if (prefix_bytes < prefix_span && n < blocks.size())
            {
                const size_t cut = prefix_span - prefix_bytes;
                recut_head = std::make_shared<OwnedChainedBuffer>(cut);
                recut_tail = std::make_shared<OwnedChainedBuffer>(blocks[n]->size() - cut);
            }
            if (prefix_bytes == prefix_span || recut_head)
            {
                VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> prefix;
                VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> suffix;
                for (size_t i = 0; i < blocks.size(); ++i)
                {
                    if (i == n && recut_head)
                    {
                        prefix.push_back(std::move(recut_head));
                        suffix.push_back(std::move(recut_tail));
                        continue;   /// `blocks[n]` replaced by the two cut pieces
                    }
                    (i < n ? prefix : suffix).push_back(std::move(blocks[i]));
                }
                head = serveFromLongConnection(*lc, offset, std::move(prefix), file_pos, stop, out_stats);
                if (*lc)
                    return head;   /// EOF before the bound: the read ends here
                file_pos += prefix_span;
                offset += prefix_span;   /// == read_until; continue with the suffix below
                want -= prefix_span;
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
    /// object size, or an advertised extent (`bound_advertised`) even when the size
    /// is unknown. Only a truly unbounded source (unknown size AND no advertised
    /// extent) is left open-ended.
    const bool stateless_bounded = opened->supportsRightBoundedReads() && want > 0
        && (!hasUnknownSize() || bound_advertised);
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
        /// `fetchWindowFromSource`), where nothing is in flight.
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
    /// only the open-ended case (unknown size AND no advertised extent) that did not
    /// reach EOF can produce that, since bounded one-shots are read to their bound.
    /// A reader with no right-bounded support (a local file) has no connection to
    /// abandon - nothing to count. Zero transfer means the lazy GET never started.
    if (!hit_eof && total_read > 0 && opened->supportsRightBoundedReads()
        && (!stateless_bounded || total_read < want))
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

size_t ReaderExecutor::clampReach(size_t predicted_end, size_t phys_off) const
{
    /// The estimator's predicted run end is run-anchored and unclamped; bound it to
    /// the physical file end when the size is known, and never below `phys_off` (a
    /// prediction behind the ask means no reach there, not a negative one).
    size_t end = std::max(predicted_end, phys_off);
    if (!hasUnknownSize())
        end = std::min(end, toPhys(totalSize()));
    return end;
}

size_t ReaderExecutor::boundedReach(size_t phys_off) const
{
    /// The physical reach a long connection opened at `phys_off` actually gets, BEFORE any
    /// extent floor: the estimator's `predictedEnd` clamped to the file end, then clamped
    /// DOWN at the next WIDE cached run the plan shows - a resident run at/above
    /// `min_bytes_for_seek` before `plan_end`, where the channel must stop (that region is
    /// served from cache, not over-read; holes strictly below the bound are
    /// bridged by `LongConnection::canContinue` on the open GET). A run cut by the plan
    /// boundary appears short here and is not a real stop, so the trajectory stays free to
    /// extend past the look-ahead. This is the SINGLE reach source shared by the open trigger
    /// (`shouldOpenLongConnection`) and the channel bound (`longConnectionBound`), so the two can never
    /// disagree on how far the channel reaches. Reads only the tracker scalar + plan geometry.
    size_t reach = clampReach(fetch_tracker.predictedEnd(), phys_off);
    const auto & geom = read_plan.geometry();
    if (geom)
    {
        const size_t wide = scheduleLookaheadReach(phys_off);
        const auto res = wide < geom->plan_end ? geom->residentAt(wide) : CoverageMap::Resident{};
        if (res.resident() && res.run_end - wide >= min_bytes_for_seek)
            reach = std::min(reach, wide);
    }
    /// The next WIDE request-map hole is a stop exactly like a wide cached run:
    /// the channel drains there instead of streaming bytes nobody will ask for
    /// (narrow holes are bridged inside `demandReachPhys`).
    reach = std::min(reach, std::max(demandReachPhys(phys_off), phys_off));
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
    /// STRUCTURAL rule: a read whose forward reach spans more than one window is
    /// long - one GET amortizes over the span instead of a request per window.
    /// The reach (`boundedReach` - the SAME value `longConnectionBound` sizes the
    /// channel with) is capped at the READ BOUND, so a small bounded read (a
    /// header probe, a lone reverse chunk) stays short: its capped reach cannot
    /// span a window. A scan or merge reads to its declared end and triggers.
    const size_t file_end = hasUnknownSize() ? std::numeric_limits<size_t>::max() : toPhys(totalSize());
    const size_t bound_phys = read_bound ? std::min<size_t>(toPhys(*read_bound), file_end) : file_end;
    return std::min(boundedReach(phys_off), bound_phys) > phys_off + effectiveWindowSize(level);
}

size_t ReaderExecutor::longConnectionBound(const StoredObject & object, size_t object_offset, size_t phys_offset) const
{
    /// The channel bound, in object-local coordinates: the forward reach, capped at
    /// the READ BOUND and the object end. The reach (`boundedReach`: `predictedEnd`
    /// clamped at the next wide cached run) is the read's forward trajectory - the
    /// same value `shouldOpenLongConnection` triggers on, so the GET drains cleanly
    /// at a wide cached run instead of being abandoned mid-run. The bound cap is the
    /// declared end of the whole assignment (per-range extents merge into it), so
    /// one GET spans the mark ranges and still never streams past the true end.
    /// Holes strictly below the bound are bridged by `LongConnection::canContinue`.
    const size_t object_base = phys_offset - object_offset;
    const size_t object_end = hasUnknownSize()
        ? std::numeric_limits<size_t>::max()
        : object_base + object.bytes_size;
    const size_t bound_phys = read_bound
        ? std::min<size_t>(toPhys(*read_bound), object_end)
        : object_end;
    size_t phys_bound = boundedReach(phys_offset);
    /// A warranted long connection opens with at least `long_connection_open_range`
    /// and never streams past `long_connection_max_bound`: it bounds an over-predicted
    /// continuous-read reach so the GET drains within the cap instead of running away.
    /// The open-range floor is for forward-spanning reads only -- a one-shot `readBigAt`
    /// transient stays bounded to its request (no continuity, no look-ahead). The READ
    /// BOUND caps LAST: nothing - not even the floor - streams past the declared end.
    if (!is_transient)
        phys_bound = std::max(phys_bound, phys_offset + long_connection_open_range);
    phys_bound = std::min(phys_bound, phys_offset + long_connection_max_bound);
    phys_bound = std::min(phys_bound, bound_phys);
    /// The demand caps last with the read bound: the open-range floor must not
    /// stream a channel into a wide request-map hole (the seek-time tail drain
    /// would then read the hole through). Narrow holes are bridged inside the
    /// reach.
    phys_bound = std::min(phys_bound, std::max(demandReachPhys(phys_offset), phys_offset));
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
    /// `read_plan.tiers`: the machine's own retrieve's `into` cells overlapping this window
    /// (`buildSchedule` already designated them - the bottom tier and same-tier slower
    /// layers; the faster tiers fill by handed jobs on the serve front). Done at launch so
    /// the worker can write the led segments inline during its fetch; the writers stay in
    /// `read_plan.tiers` (the plan is stable while a machine is in flight).
    for (const auto & t : read_plan.schedule.retrieves[m.retrieve_index].into)
    {
        if (!(t.cell.offset < m.physical_window.end() && m.physical_window.offset < t.cell.end()))
            continue;
        for (const auto & w : read_plan.tiers[t.entry].view->misses())
            if (w.writer && w.range.offset == t.cell.offset && w.range.size == t.cell.size)
                m.writer_views.push_back({w.writer.get(), w.range});
    }
}

void ReaderExecutor::runPutStep(const FetchMachine & m, const ChainedBuffers & assembled)
{
    /// `writer_views` were recorded at LAUNCH (`collectFillTargets`): NON-OWNING views of this
    /// window's fill-target writers in the shared `read_plan.tiers`, written in place on THIS
    /// read thread, inline at collect - the machine's own counters were already folded there,
    /// so the fill accounts straight into the executor `stats`. Runs AFTER the collect pinned
    /// at the fetch frontier. A failed fill is logged, never thrown: a read must not fail
    /// because cache population did.
    if (m.writer_views.empty())
        return;  /// nothing to fill for this window

    try
    {
        const size_t fill_end = assembled.empty()
            ? m.physical_window.offset
            : std::min(m.physical_window.end(), assembled.range().end());
        pushChainToWriters(m.writer_views, m.physical_window, assembled, stats);
        /// Pin the partial segment under the just-written frontier (the lane's slot):
        /// the collect pinned BEFORE this fill landed, so a fresh segment was not pinnable
        /// there. A `readBigAt` transient reads its bounded extent once and is destroyed,
        /// so it pins NOTHING (mirrors the collect's `!is_transient` guard) - else its
        /// cell survives an eviction sweep that should drop it.
        if (!is_transient)
            for (const auto & view : m.writer_views)
                if (auto pin = pinIfCovering(view.writer, fill_end))
                {
                    fill_lane.pin = std::move(pin);
                    break;
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
    /// A promote's targets are strictly-faster-tier cells, so its `CacheWriter`s are
    /// never the ones lent to the in-flight machine (a Remote's `into` routes only the
    /// bottom tier and same-tier slower layers) - no thread shares these writers.
    for (const auto & r : read_plan.schedule.retrieves)
    {
        if (r.source != PlanSchedule::Source::HandedChain)
            continue;
        if (!(r.range.offset < served_range.end() && served_range.offset < r.range.end()))
            continue;
        for (const auto & wt : r.into)
        {
            if (wt.entry >= read_plan.tiers.size() || !read_plan.tiers[wt.entry].provider)
                continue;
            for (const auto & w : read_plan.tiers[wt.entry].view->misses())
            {
                if (!w.writer)
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
                out_stats.add(Stats::BytesPromoted, written);
                HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
                    static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
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
    m->bound_advertised = read_bound.has_value();
    /// Inline (serve-thread) runner -> the fetch stops at the first sibling-led segment.
    m->inline_serve = (&machine_runner == local_runner.get());
    /// Record the fill-target writers now so the step can write its led segments inline during
    /// the fetch (the collect's `runPutStep` reuses these views).
    collectFillTargets(*m);

    /// The foreground is the sole opener; the aligned window's first physical range gives the
    /// object and its object-local offset. A no-op when not warranted / at capacity / a usable
    /// connection is already held. The channel bound comes from the runtime reach
    /// (`longConnectionBound`: `predictedEnd` clamped at the next wide cached run), the same on
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
    const size_t horizon_end = toPhys(position) + fillAheadLead();
    if (base >= horizon_end)
        return;
    const size_t capacity = horizon_end - base;
    /// The allowance is CELL-QUANTIZED: consumption prediction decides which cells to
    /// fetch, not which bytes. An extent/reach cut mid-cell would fill the touched cache
    /// cell in fragments - one small cache write per fragment and a fresh source request
    /// for the remainder - so it extends to the cell end (identity for bypass jobs). A
    /// horizon- or tail-bound cut needs no ceil: the next top-up continues the same job
    /// and completes the cell. Zero stays zero - the ceil must not resurrect an exhausted
    /// allowance.
    size_t allowance = prefetchAllowance(base);
    if (!allowance && request_map_set && !request_map.empty())
    {
        /// The frontier can sit in the grid slack BELOW a demand-run start (the
        /// head cell floors at the grid): when the demand resumes inside the
        /// frontier's cell, fetch from the frontier so the head cell completes -
        /// at most one grid quantum of hole bytes.
        if (const auto next = request_map.nextIntervalAfter(base); next && next->offset < cellCeil(r, base))
        {
            size_t hard = std::numeric_limits<size_t>::max();
            if (!hasUnknownSize())
                hard = toPhys(totalSize());
            if (read_bound)
                hard = std::min(hard, toPhys(*read_bound));
            const size_t reach = std::min(hard, demandReachPhys(next->offset));
            allowance = reach > base ? reach - base : 0;
        }
    }
    if (allowance)
        allowance = std::min(r.range.end(), cellCeil(r, base + allowance)) - base;
    size_t chunk = std::min({r.range.end() - base, capacity, allowance});
    /// A bypass job commits nothing - its whole window is memory-held transport
    /// (banked, launch-gated until consumed) - so never fetch more per machine
    /// than the serve consumes per window.
    if (r.into.empty())
        chunk = std::min(chunk, effectiveWindowSize(read_plan.geometry()->pressure_level));
    if (chunk == 0)
        return;
    /// Refill hysteresis: a launch costs a machine round-trip (and, on the stateless arm, its
    /// own GET), so wait for the cursor to open HALF the horizon before topping the lead up
    /// (classic double buffering) - but hold ONLY when the HORIZON is what makes the piece
    /// small (`chunk == capacity`). A chunk bounded by the job tail or by the fetch
    /// allowance (reach/EOF) is all the read-ahead currently allowed - the allowance grows
    /// with the confirmed run, so holding such a piece would keep prefetch permanently
    /// behind the consumer.
    if (chunk == capacity && capacity < fillAheadLead() / 2 && base + chunk < r.range.end())
        return;

    /// Read-ahead runs on the async runner (pool thread or Silk fiber), committing cells
    /// progressively; the serve cursor reads the committed prefix live.
    launchMachineForWindow(ri, ByteRange{base, chunk}, *runner);
}

void ReaderExecutor::prefetch()
{
    if (!runner)
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
        collectInFlightInto();
    }
    if (atEnd())
        return;
    drainAbandonedMachines();

    const size_t position_phys = toPhys(position);
    /// BANK BACKPRESSURE: banked bytes (bypass transport, refused residue) are
    /// memory-held until the serve consumes them - hold the lead while a full
    /// (pressure-scaled) window of them lies ahead of the consumer. Partial
    /// consumption frees the budget and the next launch tops it up, so the
    /// held-ahead footprint stays ~one window without serializing the pipeline.
    if (read_plan.geometry()
        && fill_lane.bankAheadBytes(position_phys)
            >= effectiveWindowSize(read_plan.geometry()->pressure_level))
        return;
    const size_t probe = std::min(window_size, prefetchAllowance(position_phys));
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
        /// The schedule says which jobs may run ahead (`ahead_eligible`: a promote takes
        /// the serve's output as input, so it is serve-front only); a job whose launch
        /// frontier reached its end is done. Advance the scan past them so it never rescans.
        if (!r.ahead_eligible || launchProgress(ri) >= r.range.end())
        {
            if (ri == read_plan.launch_frontier)
                ++read_plan.launch_frontier;
            continue;
        }
        launchRetrieve(ri);
        return;
    }
}

ByteRange ReaderExecutor::nextScheduledPiece(size_t ri, ByteRange window_phys) const
{
    /// The next PIECE of a populatable retrieve, straight off the schedule: the job's
    /// `fetch_runs` are the source ranges (split at every embedded resident region - served
    /// from its tier, never scheduled as a source read), and its fetch grids give
    /// the cell-fill granularity. The piece starts at the CELL frontier of the grid-floored
    /// window start - a mid-cell read fills from the cell floor (append-only) - and runs to
    /// the window's end ceiled to the grid (the whole-cell over-read that makes
    /// one cold cell ONE source read), clamped into the run. No geometry is consulted here -
    /// the schedule is the job.
    const auto & r = read_plan.schedule.retrieves[ri];
    /// The walk frontier is committed CELLS plus the BANK - not resident views (an inter-run
    /// resident hole is not cell content and must be read through, below) - so a
    /// refused-write piece whose bytes went to the bank is walked PAST, not refetched forever.
    const auto fill_prefix_end = [&](ByteRange range)
    {
        IntervalSet cov = display.committedCoverage(range);
        fill_lane.addBankCoverage(cov, range);
        return coveredPrefixEnd(cov, range);
    };
    const size_t missing = fill_prefix_end(window_phys);
    /// The append-only floor: open at the first missing byte's cell start (clamped to the
    /// job) and walk the fill frontier from there - ACROSS runs, so a before-slack run no
    /// serve window ever reaches (a seek past it) is still fetched and the cell fills
    /// whole from its floor.
    /// The floor never walks BACK across a wide demand hole to the global
    /// frontier - but it DOES drop to the touched CELL's floor: the head cell
    /// grid-floors below the demand start, and an append-only writer starting
    /// above its segment head would refuse every write. At most one grid
    /// quantum of hole bytes completes the intersecting cell (the accepted
    /// edge cost); bypass jobs (no cells) keep the raw demand floor.
    const size_t demand_floor
        = cellFloor(r, std::max(demandFloorPhys(window_phys.offset), r.range.offset));
    const size_t floor_off = std::max({r.range.offset, cellFloor(r, missing), demand_floor});
    const size_t base = floor_off < missing
        ? fill_prefix_end(ByteRange{floor_off, missing - floor_off})
        : missing;
    /// The piece: from the frontier to the end of the first run past it, its tail capped
    /// by `cellTailCap` - whole-cell targets extend it to their block edge, incremental
    /// cells only while the overhang fits one window; a demand-shaped cell that dwarfs
    /// the window caps AT the window (the consumer is waiting; the cell's tail fills on
    /// later windows through the same plan-held writer). The frontier can sit in an
    /// inter-run resident hole (nothing writes a faster tier's bytes into the cell - the
    /// cache-chain policy); the piece then reads THROUGH the hole from the source so the
    /// display has no gap.
    const size_t cell_cap = cellTailCap(r, window_phys.end(), window_phys.size);
    for (const auto & fr : r.fetch_runs)
        if (fr.end() > base)
        {
            const size_t piece_end = std::min(fr.end(), std::max(cell_cap, base));
            if (piece_end <= base)
                return {};
            return ByteRange{base, piece_end - base};
        }
    return {};
}

void ReaderExecutor::interruptAndCollectMachine()
{
    /// Bound the stall-join: ask a still-running worker to wrap at its next tile instead
    /// of holding the cursor for the whole remaining lead (a released one no-ops); the
    /// un-fetched tail stays un-attempted and relaunches.
    collectRunner().requestInterrupt(*machine);
    collectInFlightInto();
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
    /// land the window instead of blocking on the whole remaining lead. A FOREIGN machine
    /// holds the single slot the cursor outran - free the slot by aborting it at the next
    /// interrupt point. Our OWN machine on this job is JOINED whole instead: aborting it
    /// mid-window chops the producer at every consumer swing of an interleaved pattern
    /// (a compact merge's column streams), while a clean join commits/banks its window -
    /// the swung-to bytes are read from the display and the carried connection continues
    /// the job in offset order.
    const bool own_leading = machineFor(ri)
        && machine->physical_window.offset <= window.offset
        && window.offset < machine->physical_window.end();
    if (machine && !own_leading)
    {
        if (machineFor(ri))
            collectInFlightInto();
        else
            interruptAndCollectMachine();
        return true;
    }

    /// The piece extends DOWN to the touched cell's floor (append-only: a writer fills
    /// from its segment start), clamped into the job's range; the TAIL is capped by
    /// `cellTailCap` (whole-cell targets extend to their edge, incremental cells cap at
    /// the window once the overhang exceeds it - the tail fills on later windows).
    const size_t pump_demand_floor
        = cellFloor(r, std::max(demandFloorPhys(window.offset), r.range.offset));
    const size_t fetch_lo
        = std::min(window.offset, std::max({r.range.offset, cellFloor(r, window.offset), pump_demand_floor}));
    const size_t tail_cap = cellTailCap(r, window.end(), window.size);
    const size_t fetch_hi = std::max(window.end(), std::min(r.range.end(), tail_cap));
    const ByteRange fetch_window{fetch_lo, fetch_hi - fetch_lo};

    /// 1) The wait step (`waitSiblingFills`) - bounded to the cursor WINDOW; the
    ///    grid-extended tail is not needed to serve it.
    if (waitSiblingFills(window))
        return true;

    /// The wait landed nothing servable at the cursor with our own machine still in flight.
    /// If it LEADS this window, wait for its next PUBLISHED tile first - the cacheless
    /// analogue of waiting on a live cell; interrupting here would abort the producer at
    /// every consumer ask and restart the connection per window. A machine that finishes
    /// without covering the byte (or a stuck one) falls through to the join.
    if (machine)
    {
        /// Only when the residue is the pipe (no cell writers): cell-backed
        /// machines already deliver per tile through the cells and
        /// `waitSiblingFills` above is their wait.
        const bool residue_piped = std::none_of(machine->writer_views.begin(), machine->writer_views.end(),
            [](const auto & v) { return v.writer != nullptr; });
        if (machineFor(ri) && residue_piped && waitPublishedTile(*machine, window.offset))
            return true;
        interruptAndCollectMachine();
        return true;
    }

    /// 2) A source piece run as an INLINE machine (the same Fill flow as the background: elect
    ///    + fetch with the in-flow connection policy + commit; the collect pins, runs the
    ///    deferred put, and overflow-banks what the cells refused). A POPULATABLE job's piece
    ///    comes off the SCHEDULE walk (the cell's append-only floor and the fetch runs, reading
    ///    through inter-run resident holes so the cell completes); when the walk is
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
    collectInFlightInto();
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
    ChainedBuffers direct = fetchWindowFromSource(window, /*from_prefetch=*/false, reached_eof,
        read_plan.geometry()->pressure_level, read_bound.has_value(), &fill_lane.conn, /*stop=*/nullptr,
        stats);
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
    for (const auto & buf : plan.tiers)
        for (const auto & w : buf.view->misses())
            if (w.writer)
                for (const auto & part : committedPartsIn(*w.writer, window_phys))
                    covered.add(part);
    return covered;
}

size_t ReaderExecutor::committedCellPrefixEnd(ByteRange window_phys) const
{
    return coveredPrefixEnd(display.committedCoverage(window_phys), window_phys);
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
            if (i >= plan.tiers.size() || !plan.tiers[i].view)
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
    /// The in-flight machine's published residue preview (see the payload doc).
    if (machine)
    {
        std::lock_guard lock(machine->published_mutex);
        for (const auto & iv : machine->published.getIntervals())
        {
            const size_t lo = std::max(iv.offset, window_phys.offset);
            const size_t hi = std::min(iv.end(), window_phys.end());
            if (lo < hi)
                cov.add(ByteRange{lo, hi - lo});
        }
    }
    return cov;
}

bool ReaderExecutor::Display::covers(ByteRange window_phys) const
{
    return coverage(window_phys).subtract(window_phys).empty();
}

size_t ReaderExecutor::Display::frontier(ByteRange window_phys) const
{
    return coveredPrefixEnd(coverage(window_phys), window_phys);
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
            if (!run.resident() || run.entry >= plan.tiers.size()
                || !plan.tiers[run.entry].view)
                break;
            const size_t serve_end = std::min(run.run_end, window_phys.end());
            ChainedBuffers chunk = readHitFromView(*plan.tiers[run.entry].view, ByteRange{pos, serve_end - pos});
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
    /// 4) The in-flight machine's PUBLISHED residue preview - tiles the worker fetched
    ///    that no cell accepted, visible before the collect delivers them to the bank.
    ///    Slice-copies only: the preview is read-only and dies with the payload, so a
    ///    byte can serve from here now and from the bank after the collect - the same
    ///    bytes either way. No cache counters - counted at fetch, like the bank's.
    if (machine)
    {
        std::lock_guard lock(machine->published_mutex);
        if (!machine->published.empty())
            for (const auto & iv : machine->published.getIntervals())
            {
                const size_t lo = std::max(iv.offset, window_phys.offset);
                const size_t hi = std::min(iv.end(), window_phys.end());
                if (lo >= hi)
                    continue;
                for (const auto & g : covered.subtract(ByteRange{lo, hi - lo}))
                {
                    ChainedBuffers slice = machine->published.slice(g);
                    if (!slice.covers(g))
                        continue;
                    out.append(std::move(slice));
                    covered.add(g);
                }
            }
    }

    /// Serving CONSUMES the contiguous covered prefix - the display's contract is that a read
    /// DELIVERS exactly that prefix, so the bank trims below it. Banked bytes beyond the first
    /// uncovered hole stay banked - they serve a later window once the hole is fetched - while
    /// bytes below the prefix are delivered or held by a faster holder, so the banked footprint
    /// still stays ~one window.
    /// ...but never above the REUSE reach behind the cursor: a near seek may swing
    /// back within `bank_keep_behind` (one policy with the reuse gate and the slide
    /// line), and trimming those bytes would discard the producer's work at every
    /// alternation of an interleaved pattern, refetching backward per cycle.
    const size_t prefix_end_phys = coveredPrefixEnd(covered, window_phys);
    const size_t trim_line = std::min(prefix_end_phys,
        window_phys.offset > lane.bank_keep_behind ? window_phys.offset - lane.bank_keep_behind : 0);
    if (!bank.empty())
    {
        const ByteRange held = bank.range();
        if (trim_line > held.offset)
            bank = trim_line < held.end()
                ? bank.slice(ByteRange{trim_line, held.end() - trim_line})
                : ChainedBuffers{};
    }
}

void ReaderExecutor::Display::wait(ByteRange window_phys, ChainedBuffers & out, IntervalSet & covered)
{
    /// No cache-read credit here: the caller banks the sibling bytes and credits them once
    /// (its own committed bytes are dropped - the serve reads and counts them from the cells).
    for (const auto & buf : plan.tiers)
    {
        /// A page cell is filled by promotion at the serve, not downloaded - no downloader,
        /// a wait on it would never wake.
        if (!buf.provider || buf.provider->tier() == CacheTier::PageCache)
            continue;
        for (const auto & w : buf.view->misses())
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
    const size_t prefix_end = coveredPrefixEnd(covered, window);
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

VectorWithMemoryTracking<ReaderExecutor::PieceObservation> ReaderExecutor::observeSpan(
    const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & caches_,
    const OffsetMap & offset_map_,
    ByteRange span,
    const IntervalSet * request_map_,
    std::optional<size_t> demand_ceiling_phys)
{
    /// Per-tier classification the builder needs: the tier id (geometry entry),
    /// whether it accepts only whole-cell puts, and whether it populates on a
    /// miss (a read-only/bypass tier contributes no fill cells).
    struct TierTraits { CacheTier tier; bool whole_cell; bool populates; };
    VectorWithMemoryTracking<TierTraits> traits;
    for (const auto & cache : caches_)
        traits.push_back(TierTraits{cache->tier(), cache->fillsWholeCell(), cache->populatesOnMiss()});

    VectorWithMemoryTracking<PieceObservation> pieces;
    size_t piece_file_start = span.offset;
    for (const auto & pr : offset_map_.map(span))
    {
        PieceObservation piece;
        piece.object = pr.object;
        piece.object_file_offset = piece_file_start - pr.object_offset;
        const ByteRange piece_span{piece_file_start, pr.size};
        /// The whole span is the demand for now: the plan is KNOWLEDGE and
        /// must survive read-bound advances (the bound gates fetching, never
        /// observation - and the old writer upgrade opened cells for the whole
        /// span regardless of the bound too). The request map's hole semantics
        /// (asking only covered intervals) land with the plan-over-union stage.
        (void)request_map_;
        (void)demand_ceiling_phys;

        /// The RANGED builder: one writer-carrying `lookAt` per (tier, ask) -
        /// resolution and allocation in one cache transaction. Each lower tier
        /// is asked only for territory the FASTER tiers miss (prune by
        /// subtraction - a pruned cell never opens a writer), and the caller's
        /// exclusions (a plan extension's owned straddler cells) subtract the
        /// same way. Hits keep their true tail extent (overshoot is knowledge)
        /// with heads clamped to the span; miss cells may overhang the span by
        /// the provider's grid rounding.
        IntervalSet subtracted;
        IntervalSet coverage;
        for (size_t ci = 0; ci < caches_.size(); ++ci)
        {
            GeometryEntry entry;
            entry.tier = traits[ci].tier;
            entry.whole_cell = traits[ci].whole_cell;
            auto view = std::make_unique<CacheView>();

            /// This tier's already-collected extents. A WIDE existing segment is
            /// returned WHOLE by every ask that intersects it, so when a faster
            /// tier's hit splits this tier's span into several asks straddling
            /// one segment, `getOrSet`/`get` hands the same segment back per ask.
            /// Hits also shade slower tiers (they enter `subtracted`); misses do
            /// NOT shade slower tiers but must still not be re-emitted for THIS
            /// tier - so gate every resolution on `tier_emitted` (its raw extent),
            /// keeping `aligned_miss`/`resident` sorted, disjoint, exactly-once.
            IntervalSet tier_emitted;
            const auto asks = subtracted.subtract(piece_span);
            for (const auto & ask : asks)
            {
                for (const auto & sub : subtracted.subtract(ask))
                {
                    for (auto & res : caches_[ci]->resolve(piece.object, piece.object_file_offset, sub))
                    {
                        if (res.kind == ICacheProvider::Resolution::Kind::End)
                            continue;
                        /// Already collected by an earlier ask of this tier (a
                        /// wide segment spanning the split): skip whole.
                        if (tier_emitted.subtract(res.range).empty())
                            continue;
                        tier_emitted.add(res.range);

                        if (res.kind == ICacheProvider::Resolution::Kind::Hit)
                        {
                            const size_t lo = std::max(res.range.offset, sub.offset);
                            if (lo >= res.range.end())
                                continue;
                            const ByteRange clamped{lo, res.range.end() - lo};
                            entry.resident.push_back(clamped);
                            view->hit_entries.push_back(HitEntry{clamped, std::move(res.reader)});
                            /// Shades this tier's later asks AND every slower tier.
                            subtracted.add(clamped);
                            coverage.add(clamped);
                        }
                        else if (res.kind == ICacheProvider::Resolution::Kind::Miss)
                        {
                            if (!traits[ci].populates)
                                continue;
                            entry.aligned_miss.push_back(res.range);
                            view->miss_entries.push_back(MissEntry{res.range, std::move(res.writer)});
                            coverage.add(res.range);
                        }
                    }
                }
            }

            piece.folded.push_back(std::move(entry));
            piece.views.push_back(std::move(view));
        }
        /// The plan keeps the entries' tail overshoot as coverage (a hit's true
        /// extent, a cell's grid rounding reach past the span) - contiguously
        /// from the span end.
        piece.covered_end = piece_span.size
            ? std::max(piece_span.end(), coverage.contiguousEnd(piece_span.end() - 1, 1))
            : piece_span.end();

        pieces.push_back(std::move(piece));
        piece_file_start += pr.size;
    }
    return pieces;
}

void ReaderExecutor::emitObservation(
    const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & caches_,
    VectorWithMemoryTracking<PieceObservation> & pieces,
    CoverageMap & geom,
    VectorWithMemoryTracking<PlanTier> & tiers)
{
    /// The ranged builder produced entries and views 1:1 - pruned territory was
    /// never asked (subtraction), so there is nothing to drop, and `resolve`
    /// already attached the miss writers for a populating tier. Publish
    /// both-or-neither, cache-major fastest-first.
    for (size_t ci = 0; ci < caches_.size(); ++ci)
    {
        for (auto & piece : pieces)
        {
            GeometryEntry & folded = piece.folded[ci];
            if (folded.resident.empty() && folded.aligned_miss.empty())
                continue;

            CacheViewPtr view = std::move(piece.views[ci]);

            PlanTier plan_tier;
            plan_tier.provider = caches_[ci].get();
            plan_tier.view = std::move(view);
            geom.entries.push_back(std::move(folded));
            tiers.push_back(std::move(plan_tier));
        }
    }
}

void ReaderExecutor::observeAndSchedule(size_t physical_start)
{
    stats.add(Stats::Observations);
    /// Machine-check the threading invariant: the held read/write buffers are
    /// foreground-private and must never be torn down / rebuilt while a prefetch worker
    /// is in flight (the worker co-owns only the immutable geometry), so a segment is
    /// never aliased by a machine-held writer and a fresh writer upgrade of the next
    /// plan (`[CF-plan-rebuild]`). The cache fill is inline on the read thread, so there
    /// is nothing deferred to drain here.
    chassert(!machine);

    /// Reset the in-flight segment pin BEFORE tearing down the held buffers
    /// (`[CF-plan-rebuild]`): the pin aliases a held write buffer's own bare segment ref,
    /// so dropping it first makes `~DiskCacheWriter` the LAST owner and
    /// `FileSegment::complete` effective (otherwise a PARTIALLY_DOWNLOADED segment would
    /// stay un-shrunk and the next writer upgrade would alias the same segment in two
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
    /// RESTART: the ahead cursor re-derives from the fresh display truth and the
    /// bank drops with the plan it served - BEFORE the empty-plan early return below, so an
    /// at-bound restart cannot leave the previous plan's state alive (REUSE and
    /// EXTEND keep the surviving plan, the cursor, AND the bank).
    fill_lane.resetOnRestart();

    const ByteRange plan_range = boundedPlanSpan(physical_start);
    if (plan_range.size == 0)
    {
        ReadPlan empty;
        empty.geometry_snapshot = std::move(geom);  /// empty plan; covers()==false
        read_plan = std::move(empty);
        return;
    }
    /// A tier's miss CELLS may extend past the span in both directions (the
    /// provider clamps them to the object end only): the overhang is FILL-ONLY
    /// work carried by the schedule's cell closure (`fillRegion`) - never
    /// served span - so the unprobed overhang needs no residency knowledge and
    /// no second probe. Cell segmentation is probe-range-independent (virgin
    /// holes tile on the absolute grid; existing segments report their true
    /// extents), so one span-sized observation per piece is the whole story.
    auto pieces = observeSpan(caches, offset_map, plan_range, requestMapForPlanning(), boundCeilingPhys());

    /// The covered end, not the requested end: the walk's last resolution may
    /// overshoot the target, and that coverage is real plan knowledge.
    chassert(!pieces.empty());
    geom->plan_end = pieces.back().covered_end;
    ReadPlan plan;
    emitObservation(caches, pieces, *geom, plan.tiers);

    chassert(geom->entries.size() == plan.tiers.size());

    /// Publish atomically: `geometry()` and `tiers` are one object (`read_plan`), so a
    /// reader can never see new geometry against a stale buffer vector. Assigning
    /// `read_plan` finalizes the previous plan's write buffers and runs its deferred
    /// LRU bumps.
    plan.geometry_snapshot = std::move(geom);
    read_plan = std::move(plan);

    /// Describe the plan's work once, here - over the plan's own span (the schedule
    /// derives it from the geometry): everything within it is read by the scan
    /// (User), only the cell slack around it is FillOnly.
    /// `schedule.retrieves[*].into` then drives `runPutStep` so a faster tier never
    /// receives slack bytes (see `ReadPlan::schedule`).
    read_plan.schedule = buildSchedule(
        *read_plan.geometry(),
        effectiveWindowSize(read_plan.geometry()->pressure_level),
        effectiveBlockSize(read_plan.geometry()->pressure_level));

    /// Feed this plan's predicted source reads into the fetch tracker so its
    /// reach prediction (which sizes long source connections) stays current.
    feedScheduleToFetchTracker(read_plan.schedule);

    /// A plan with no `Source::Remote` retrieve is served entirely from cache; the
    /// prefetch look-ahead has nothing to launch.
    read_plan.has_remote_retrieves = std::any_of(
        read_plan.schedule.retrieves.begin(), read_plan.schedule.retrieves.end(),
        [](const auto & r) { return r.source == PlanSchedule::Source::Remote; });

    LOG_TRACE(log, "observeAndSchedule: planned [{}, {}), {} entries, {} retrieves",
        read_plan.geometry()->plan_start, read_plan.geometry()->plan_end,
        read_plan.geometry()->entries.size(), read_plan.schedule.retrieves.size());
}

void ReaderExecutor::extendPlan(size_t position_phys)
{
    chassert(!machine);
    const auto old_geom = read_plan.geometry();
    chassert(old_geom && old_geom->plan_end > old_geom->plan_start);

    const size_t old_end = old_geom->plan_end;
    const size_t target_end = boundedPlanSpan(position_phys).end();
    if (target_end <= old_end)
        return;

    stats.add(Stats::PlanExtensions);

    auto pieces = observeSpan(caches, offset_map, ByteRange{old_end, target_end - old_end}, requestMapForPlanning(), boundCeilingPhys());

    /// A cell straddling the old `plan_end` is already owned by an old entry's
    /// view (its writer was opened there); the extension derives the same
    /// absolute-grid cell and must not own it twice - a second writer
    /// would alias the segment in two buffers. Overlap implies identity: cells
    /// tile on the absolute grid and an existing segment reports its true extent.
    /// A HIT run straddling `old_end` needs no such dedup: the extension
    /// re-collects it from `old_end` with a second reader, and aliasing readers
    /// is benign - they are read-only and each view bumps the LRU once.
    VectorWithMemoryTracking<std::pair<CacheTier, ByteRange>> straddlers;
    for (const auto & e : old_geom->entries)
        for (const auto & c : e.aligned_miss)
            if (c.end() > old_end)
                straddlers.push_back({e.tier, c});
    if (!straddlers.empty())
    {
        for (auto & piece : pieces)
        {
            for (size_t ci = 0; ci < caches.size(); ++ci)
            {
                auto & cells = piece.folded[ci].aligned_miss;
                auto & view_misses = piece.views[ci]->miss_entries;
                /// The extension's cell for a segment the old plan already owns
                /// carries a LIVE writer (opened at the `lookAt` getOrSet) over
                /// the SAME segment - keeping it would alias the old owner's
                /// segment in a second holder. Every edit to the folded cell
                /// must mirror onto its held view miss entry (found by the
                /// cell's current offset/size), so the geometry and the view -
                /// which `collectFillTargets` exact-matches against - stay 1:1.
                auto erase_view_miss = [&](ByteRange cell)
                {
                    for (size_t v = view_misses.size(); v > 0; --v)
                        if (view_misses[v - 1].range.offset == cell.offset
                            && view_misses[v - 1].range.size == cell.size)
                        {
                            view_misses.erase(view_misses.begin() + (v - 1));
                            return;
                        }
                };
                auto trim_view_miss = [&](ByteRange cell, ByteRange trimmed)
                {
                    for (auto & me : view_misses)
                        if (me.range.offset == cell.offset && me.range.size == cell.size)
                        {
                            me.range = trimmed;
                            return;
                        }
                };
                for (size_t i = cells.size(); i > 0; --i)
                {
                    for (const auto & [tier, owned] : straddlers)
                    {
                        if (tier == caches[ci]->tier()
                            && cells[i - 1].offset < owned.end() && owned.offset < cells[i - 1].end())
                        {
                            /// Same absolute grid + same demand snapshot derive the same
                            /// cut, so overlap normally implies identity - drop the twin.
                            /// A shifted demand edge (bound advance, re-announced request
                            /// map) between the old plan and this extension can derive a
                            /// DIFFERENT cell: TRIM it to the owned cell's remainder (the
                            /// old writer keeps its cell; only uncovered territory stays
                            /// planned). The drop / trim mirrors onto the held view too.
                            if (cells[i - 1].offset == owned.offset && cells[i - 1].size == owned.size)
                            {
                                erase_view_miss(cells[i - 1]);
                                cells.erase(cells.begin() + (i - 1));
                            }
                            else if (cells[i - 1].end() > owned.end())
                            {
                                const ByteRange trimmed{owned.end(), cells[i - 1].end() - owned.end()};
                                trim_view_miss(cells[i - 1], trimmed);
                                cells[i - 1] = trimmed;
                            }
                            else
                            {
                                erase_view_miss(cells[i - 1]);
                                cells.erase(cells.begin() + (i - 1));
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Copy-on-extend: the published snapshot is immutable (workers co-own it),
    /// so the extension publishes a NEW geometry - the old entries copied, the
    /// extension's appended - while `read_plan.tiers` grows in the same order,
    /// keeping the 1:1 positional mapping. Pressure is resampled per extension.
    ///
    /// SLIDE: entries the cursor has fully passed are released with the copy -
    /// their hit readers drop (the view dtor runs the deferred LRU bumps) and
    /// their writers finalize (complete-or-abandoned cells) - so the retained
    /// span, not the stream length, bounds the held buffers. The release line
    /// is the REUSE reach (`min_bytes_for_seek`): anything a near seek could
    /// swing back to stays held; anything below it would RESTART anyway. A
    /// partially-passed entry is kept whole. `plan_start` advances to the
    /// line, so the reuse gate and `covers` refuse the released territory.
    const size_t release_line
        = position_phys > min_bytes_for_seek ? position_phys - min_bytes_for_seek : 0;
    auto geom = std::make_shared<CoverageMap>();
    geom->plan_start = std::min(std::max(old_geom->plan_start, release_line), position_phys);
    chassert(!pieces.empty());
    geom->plan_end = pieces.back().covered_end;
    geom->pressure_level = memoryPressureMonitor().currentLevel();
    for (size_t i = 0; i < old_geom->entries.size(); ++i)
    {
        const auto & entry = old_geom->entries[i];
        bool passed = true;
        for (const auto & run : entry.resident)
            passed = passed && run.end() <= release_line;
        for (const auto & cell : entry.aligned_miss)
            passed = passed && cell.end() <= release_line;
        if (passed)
            continue;
        const size_t kept = geom->entries.size();
        geom->entries.push_back(entry);
        if (kept != i)
            read_plan.tiers[kept] = std::move(read_plan.tiers[i]);
    }
    read_plan.tiers.resize(geom->entries.size());
    emitObservation(caches, pieces, *geom, read_plan.tiers);
    chassert(geom->entries.size() == read_plan.tiers.size());
    read_plan.geometry_snapshot = std::move(geom);

    /// The schedule is a pure function of the geometry - rebuild it over the
    /// full extended span rather than splice at the boundary. Jobs wholly
    /// behind the cursor are skipped by the launch frontier: the geometry
    /// behind the cursor is stale for cells fetched since their observation,
    /// and the display, not the schedule, is the execution truth there.
    read_plan.schedule = buildSchedule(
        *read_plan.geometry(),
        effectiveWindowSize(read_plan.geometry()->pressure_level),
        effectiveBlockSize(read_plan.geometry()->pressure_level));
    feedScheduleToFetchTracker(read_plan.schedule);
    read_plan.has_remote_retrieves = std::any_of(
        read_plan.schedule.retrieves.begin(), read_plan.schedule.retrieves.end(),
        [](const auto & r) { return r.source == PlanSchedule::Source::Remote; });
    size_t frontier = 0;
    while (frontier < read_plan.schedule.retrieves.size()
        && read_plan.schedule.retrieves[frontier].range.end() <= position_phys)
        ++frontier;
    read_plan.launch_frontier = frontier;

    LOG_TRACE(log, "extendPlan: extended [{}, {}) -> [{}, {}), {} entries, {} retrieves",
        read_plan.geometry()->plan_start, old_end,
        read_plan.geometry()->plan_start, read_plan.geometry()->plan_end,
        read_plan.geometry()->entries.size(), read_plan.schedule.retrieves.size());
}

void ReaderExecutor::feedScheduleToFetchTracker(const PlanSchedule & schedule)
{
    /// The predicted SOURCE reads are the `Source::Remote` retrieves; upper-tier
    /// reads and promotes open no source connection, so a wide upper hit between
    /// them correctly breaks the run. Feed in offset order (the tracker's gap
    /// bridging wants a monotone stream); the tracker itself skips spans an
    /// earlier overlapping plan already fed.
    VectorWithMemoryTracking<ByteRange> source_reads;
    for (const auto & r : schedule.retrieves)
        if (r.source == PlanSchedule::Source::Remote)
            source_reads.push_back(r.range);
    std::sort(source_reads.begin(), source_reads.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });

    for (const auto & range : source_reads)
        fetch_tracker.recordReadRange(range.offset, range.size);
}

bool ReaderExecutor::planReachesEnd() const
{
    return read_plan.geometry() && !offset_map.hasUnknownSize()
        && read_plan.geometry()->plan_end >= offset_map.totalSize();
}

size_t ReaderExecutor::effectivePlanCeiling() const
{
    /// The plan look-ahead target: at least `window_size`, raised to
    /// `plan_look_ahead_max_window` when configured larger. Not pressure-scaled.
    /// Kept wider than the fill-ahead lead by default, so the plan never caps
    /// the prefetch distance.
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
        chassert(read_bound);
        const size_t physical_extent_end = toPhys(*read_bound);
        if (physical_start >= physical_extent_end)
            return ByteRange{physical_start, 0};
        want = std::min(physical_extent_end - physical_start, ceiling);
    }
    else
    {
        /// The plan TARGET the iterative windowed probe grows toward. Independent of
        /// `read_bound` (which only clamps sizing), so the plan survives
        /// mark-range advances and is reused. An UNKNOWN-SIZE source is not planned past
        /// one window: with no file end to clamp the span, the probe would tile cache
        /// segments beyond the real EOF only for the first short read (the EOF marker)
        /// to invalidate them.
        want = offset_map.hasUnknownSize() ? window_size : ceiling;
    }
    /// CA3 - the request-map JOIN: confine the plan to the contiguous DEMAND run
    /// from the start (narrow holes bridged, stops at the first WIDE hole), so a
    /// wide hole's bytes are never observed, `getOrSet`, or scheduled - the plan
    /// ends at the hole and a seek to the next covered range re-plans there
    /// (holes jumped). Only clamp when demand runs AHEAD: a start that is itself
    /// in a hole (a service read the map did not predict) has no reach there, so
    /// it keeps the full ceiling and plans/serves normally - the map bounds
    /// speculation, never service. Transients are exempt (the request IS the
    /// demand). No map: `demandReachPhys` returns max(), no clamp.
    if (!is_transient)
    {
        const size_t reach = demandReachPhys(physical_start);
        if (reach > physical_start)
            want = std::min(want, reach - physical_start);
    }
    /// `want` is a COVER TARGET, not a cap: the walk iterates `lookAt` until it
    /// is covered, and the last resolution's true extent may overshoot it
    /// (`plan_end` is the covered end). The file end is the only natural bound.
    if (!offset_map.hasUnknownSize())
    {
        const size_t physical_end = offset_map.totalSize();
        if (physical_start >= physical_end)
            return ByteRange{physical_start, 0};
        want = std::min(want, physical_end - physical_start);
    }
    return ByteRange{physical_start, want};
}

CacheWriter::CacheSegmentPin ReaderExecutor::writerPinAt(size_t frontier) const
{
    for (const auto & buf : read_plan.tiers)
        for (const auto & w : buf.view->misses())
            if (auto pin = pinIfCovering(w.writer.get(), frontier))
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
    /// (`setReadBound`), or a seek re-plans and rebuilds it (see `seek`).

    LOG_TRACE(log, "Prefetch: discarding (physical [{}, {}))",
        m->physical_window.offset, m->physical_window.end());

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
        /// writes the shared `read_plan.tiers` writers (its led segments) on the pool thread, so
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

size_t ReaderExecutor::fillAheadLead() const
{
    /// ONE uniform deep lead, self-limited by cell acceptance instead of a tier gate: the
    /// worker retains only bytes no cell accepted, capped at one window
    /// (`coordinatedPrefetch`'s residue cap). A populating tier runs the full lead in its
    /// cells; a bottom that accepts nothing - read-only tier, bypass gap, full cache - hits
    /// the cap ~a window into the lead and stops, emergently reproducing the one-window
    /// cadence; the pump's stall-join interrupt bounds a consumer wait to one tile.
    return fill_ahead_lead;
}

size_t ReaderExecutor::clampToBound(size_t win_size) const
{
    if (!read_bound)
        return win_size;
    const size_t remaining = *read_bound > position ? *read_bound - position : 0;
    return std::min(win_size, remaining);
}

size_t ReaderExecutor::prefetchAllowance(size_t phys_from) const
{
    /// PRODUCER-side allowance: how many PHYSICAL bytes a fetch may take from `phys_from`
    /// (a launch frontier or the cursor). Bounded by what exists (the file end) and by the
    /// READ BOUND - the caller's declared boundary (the planned end of the whole
    /// assignment when it knows it, else the advancing read-until): everything below it is
    /// legitimate to fetch, nothing above it is worth speculating on. Deliberately NOT
    /// capped at the serving horizon - the caller applies it; the consumer's ceiling is
    /// `readCeiling`.
    size_t bound = std::numeric_limits<size_t>::max();
    if (!hasUnknownSize())
        bound = toPhys(totalSize());
    if (read_bound)
        bound = std::min(bound, toPhys(*read_bound));
    /// The request map stops speculation at a WIDE demand hole (bytes nobody
    /// will ask for); narrow holes are bridged. Service is never capped - a
    /// read into a hole serves synchronously.
    bound = std::min(bound, demandReachPhys(phys_from));
    return bound > phys_from ? bound - phys_from : 0;
}

}
