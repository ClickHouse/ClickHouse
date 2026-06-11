#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/LiveConnectionLimit.h>

#include <Common/Logger.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
#include <array>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "config.h"
#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#endif

namespace DB
{

class PrefetchThreadPool;
class ReaderExecutorLog;
class PrefetchHandle;

/// Reads a logical file (one or more `StoredObject`s mapped by `OffsetMap`)
/// through a fastest-first cache chain, falling back to the source. Tuned for
/// sequential scans: keeps one source connection alive across windows
/// (`connection`), reads the next gap ahead on a `PrefetchThreadPool`, and
/// shrinks its window/block sizes under memory pressure. Owns its cache and
/// decryption layers internally, so it is NOT wrapped by the legacy
/// async/decrypt/cache read buffers. One instance per column-stream; not
/// thread-safe beyond the prefetch-worker handoff: while a prefetch is in flight
/// the worker exclusively owns `connection` and `inflight_segment_pin`, and the
/// foreground must establish the `get()`/`tryCancel` happens-before edge before
/// touching them (enforced by a `chassert` in `readPhysicalWindow`). A foreground
/// read that skips that handoff reintroduces the `connection` use-after-free.
/// Served-byte counters are NOT shared: a prefetch worker accumulates into its own
/// job-local `Stats`, merged into `this->stats` at join (see `prefetch_job_stats`).
class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_WINDOW_SIZE = 8 * 1024 * 1024; /// 8 MiB
    /// Gap bound for the live-connection bridge / seek-keep and `mergeRanges`: a
    /// forward gap up to this is skipped on the open GET instead of reopening.
    /// Set near the bandwidth/request cost breakeven (~1.75 MiB) so bridging is
    /// cost-positive; larger gaps reopen.
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 2 * 1024 * 1024; /// 2 MiB
    /// Drain bound: a live connection dropped within this of its right bound is
    /// read out to the bound first, so it completes and returns to the pool
    /// reusable instead of counting an incomplete connection. ~ the I-weight /
    /// bandwidth breakeven (0.25 MiB), rounded up.
    static constexpr size_t DEFAULT_MAX_TAIL_FOR_DRAIN = 1 * 1024 * 1024; /// 1 MiB
    static constexpr size_t ROPE_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB per Rope node
    /// Look-ahead span over which residency is planned ONCE (plan-then-stream):
    /// the held plan lets many `window_size` reads stream resident bytes without
    /// per-window cache discovery, so `getOrSet`/holder-build is amortised across
    /// the span instead of paid per window. 8x the default window. Planning is
    /// disabled when this is below `window_size` (a plan must cover a full window).
    static constexpr size_t DEFAULT_PLAN_LOOK_AHEAD = 64 * 1024 * 1024; /// 64 MiB

    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches,
        size_t window_size = DEFAULT_WINDOW_SIZE,
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK,
        size_t block_size = ROPE_BLOCK_SIZE,
        String log_file_path = {},
        size_t max_tail_for_drain = DEFAULT_MAX_TAIL_FOR_DRAIN,
        size_t plan_look_ahead_window = DEFAULT_PLAN_LOOK_AHEAD);

    /// Destructor must be out-of-line because Connection holds unique_ptr<ReadBufferFromFileBase>.
    ~ReaderExecutor();

    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    /// A fresh executor for the half-open logical range `[start_position,
    /// start_position + read_size)`, sharing immutable state (caches, source,
    /// objects, decryption) but owning its own position / live connection. Drives
    /// `PipelineReadBuffer::readBigAt` as a one-shot read. Shares `buffer_limit`
    /// (its connection counts against the server budget) but gets no
    /// `prefetch_pool`/log: a one-shot read can't amortise prefetch and would
    /// steal slots from a sequential reader. `read_size` bounds every read (window
    /// and source range) to the request, so the connection it borrows is fully
    /// drained and returned to the pool reusable rather than abandoned mid-stream.
    std::unique_ptr<ReaderExecutor> makeTransientForReadAt(size_t start_position, size_t read_size) const;

    /// Roll a drained `makeTransientForReadAt` executor's stats into this (parent)
    /// executor, so the parent's `reader_executor_log` row and ProfileEvents
    /// account for the random-access (`readBigAt`) read. Thread-safe: concurrent
    /// `readBigAt` calls share one parent.
    void mergeTransientStats(const ReaderExecutor & transient);

    /// Whether `makeTransientForReadAt` / `readBigAt` is allowed. All current
    /// `IFileBasedSourceReader` implementations support concurrent `open()` (each call
    /// returns an independent buffer), so this is true whenever a source is
    /// configured. Kept as a method so future non-reusable sources can opt out.
    bool canReadAt() const { return static_cast<bool>(source); }

    void setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool);
    void setBufferLimit(std::shared_ptr<LiveConnectionLimit> limit);
    /// Override the live-connection gap-reach threshold (0 keeps the `window_size`
    /// default). From `reader_executor_live_connection_min_read_bytes`.
    void setLiveConnectionMinReadBytes(size_t bytes);
    void setReaderExecutorLog(std::shared_ptr<ReaderExecutorLog> log_);

    /// Advertise the read extent: the logical end offset the consumer intends to
    /// read up to. Driven via the standard `ReadBuffer::setReadUntilPosition`
    /// contract that `MergeTreeReaderStream::adjustRightMark` already issues per
    /// mark range. The executor bounds its live source connection to this extent
    /// - so the borrowed HTTP connection is read to a known end and returned to
    /// the pool reusable instead of abandoned open-ended mid-response - and keeps
    /// prefetches within it. `nullopt` clears it (read to the file end). Drains an
    /// in-flight prefetch when the extent changes (the worker reads the extent to
    /// bound its connection, and a prefetch for the old range must not serve the
    /// new one). Distinct from the `makeTransientForReadAt` one-shot extent, which
    /// also sets `is_transient`.
    void setReadExtent(std::optional<size_t> logical_end);

    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer. Can be called multiple times for layered encryption.
    /// No-op in builds without SSL. Call initDecryption() once after all layers
    /// have been added to read and parse the on-disk headers.
    void addDecryptionLayer(String path, size_t buffer_size, KeyFinderFunc key_finder);

    /// Read the encryption headers (one per layer) and resolve keys via the
    /// configured key_finders. Must be called after addDecryptionLayer setup
    /// and before any read. No-op when no layers are configured or when
    /// ClickHouse is built without SSL.
    void initDecryption();

    /// Reads carry encryption layers whose payload must be decrypted on consume
    /// (by PipelineReadBuffer via decryptInPlace). False in builds without SSL.
    bool needsDecryption() const { return data_start_offset > 0; }

    /// Decrypt `size` bytes in place at logical file offset `logical_offset` using
    /// persistent per-layer CTR encryptors (built lazily from the parsed headers).
    /// CTR is position-addressable, so the consumer decrypts only the chunk it
    /// serves. Single-threaded per executor (no-op without SSL / no layers).
    void decryptInPlace(char * data, size_t size, size_t logical_offset);

    size_t getPosition() const { return position; }

    /// Logical object path for diagnostics (format/decompression errors via
    /// `getFileNameFromReadBuffer`). The single-object `remote_path`; empty when
    /// no objects are configured.
    String getFileName() const { return log_file_path; }

    /// Test-only: is there a prefetch currently scheduled for the next window?
    bool hasInflightPrefetch() const { return prefetch_handle != nullptr; }
    /// Test-only: byte size of the in-flight prefetch window, or 0 when no
    /// prefetch is scheduled (e.g. suppressed under high memory pressure).
    size_t inflightPrefetchSize() const { return prefetch_handle ? prefetch_range.size : 0; }
    /// Test-only: number of cancelled prefetch handles still awaiting the
    /// destructor's drain (stashed on cancel so the worker can finish
    /// attaching before this executor's state is freed).
    size_t abandonedPrefetchCount() const { return abandoned_prefetches.size(); }

    /// Logical file size (physical size minus encryption headers).
    /// Saturates to 0 if the underlying objects sum to fewer bytes than the
    /// declared encryption headers — that file is corrupt/truncated; the
    /// next read (or initDecryption) will surface CANNOT_READ_ALL_DATA.
    size_t totalSize() const
    {
        size_t physical = offset_map.totalSize();
        return physical > data_start_offset ? physical - data_start_offset : 0;
    }

    /// True iff the underlying object had `StoredObject::UnknownSize` (e.g. S3
    /// HEAD without Content-Length). Callers that need to convert
    /// `totalSize()` into an `optional<size_t> file_size` MUST consult this
    /// first — `totalSize()` returns `~uint64_t::max()` in that case, which
    /// is meaningless as a literal byte count.
    bool hasUnknownSize() const { return offset_map.hasUnknownSize(); }

    /// Merge close-together ranges to reduce source request count.
    /// Ranges separated by less than min_gap are combined.
    static VectorWithMemoryTracking<ByteRange> mergeRanges(const VectorWithMemoryTracking<ByteRange> & ranges, size_t min_gap);

private:
    /// Per-executor accumulating stats (defined below). Forward-declared so the
    /// read-path methods can take a `Stats &` accumulator they write into - the
    /// foreground passes `this->stats`, a prefetch worker passes its own job-local
    /// `Stats` (merged into `this->stats` at join), so the worker never writes a
    /// shared counter.
    struct Stats;

    /// The in-flight prefetch's co-owned job: the worker's job-local `Stats` plus the
    /// source-connection cluster (`ConnState`) it owns for the job's duration.
    /// Forward-declared so the `prefetch_job` member can be a `shared_ptr<PrefetchJob>`
    /// (defined below, after `Stats`).
    struct PrefetchJob;

    /// The source-connection cluster threaded as a `ConnState &` through the read
    /// path (defined below, after `Connection`). Forward-declared so the
    /// conn-touching method signatures can take a `ConnState &`.
    struct ConnState;

    /// The immutable look-ahead residency snapshot (co-owned with a prefetch worker;
    /// defined below). Forward-declared so the read-path signatures can take a
    /// `const ReadPlanGeometry &`.
    struct ReadPlanGeometry;

    /// Foreground assembler for `physical_window`: resident bytes from the plan, the
    /// rest from the source, then the cache backfill + the Strategy-A pin. Reached by
    /// `initDecryption` (header) and the two synchronous gap reads in `readNextWindow` -
    /// a prefetch worker does NOT come through here (it runs `fetchGapsFromSource`
    /// directly and the foreground backfills its bytes at consume). Returns one
    /// contiguous run from the window start (a hole would shift the caller's offset
    /// interpretation). `geometry` is the residency snapshot (`*read_plan.geometry()`, or an
    /// empty one at init); `eof_latch` is the size-unknown EOF latch (`this->reached_eof`).
    Rope readPhysicalWindow(ByteRange physical_window, ConnState & conn,
        const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats);

    /// Append every byte the `geometry` snapshot reports resident in `physical_window`
    /// to `result` (recording it in `covered`), reading it from the matching `read_plan`
    /// `BufEntry`'s held hit read buffer. FOREGROUND-only (a worker serves no resident
    /// bytes - its window is a pure gap). Fastest-tier-first is preserved by the
    /// `covered` guard (geometry is in tier order). Also serves a grown committed prefix
    /// of a frozen miss from its held write buffer's `read` (`[CF-partial-prefix]`) and a
    /// self-populated complete page block (`[CF-reusable]`). Does NOT re-plan: an
    /// uncovered remainder is left for the source backfill.
    void serveResidentFromPlan(
        ByteRange physical_window, Rope & result, IntervalSet & covered,
        const ReadPlanGeometry & geometry, Stats & out_stats);

    /// Serve a clamped resident sub-range from a held `planResidencyView` view's hit read
    /// buffers, clamping each read to the buffer's live `readable()` (so a partial prefix
    /// is never over-read) and recording it on the view for the deferred LRU bump. Returns
    /// the assembled (possibly short) Rope; the caller checks `covers`.
    static Rope readHitFromView(CacheView & view, ByteRange clamped);

    /// A read-only multi-tier, priority-ordered, single-`covered` sweep over the ranges
    /// in `window` still uncovered after the plan's held buffers: a sibling reader /
    /// promotion may have populated a gap between plan-build and consume. Issues a fresh
    /// `planResidencyView` per still-uncovered piece, serves its hits fastest-tier-first
    /// under the SAME `covered`, and moves each view into `read_plan.deferred_lru_bumps` so
    /// its deferred LRU-bump lands after the held write buffers' writes (`[CF-lru]`). Its
    /// writers are ignored (we already have, or are about to fetch, the source bytes).
    /// Run BEFORE the source backfill.
    void serveLateHits(ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats);

    /// Synchronous foreground gap read + backfill in one pass: serve any late cache hit
    /// (`serveLateHits`), re-credit any grown committed prefix of the plan's misses, read
    /// the still-missing ranges from the source, append them from the source Rope, and
    /// push them into the plan's held write buffers. Used by the foreground (`readBigAt`,
    /// the sync gap reads, the header); the prefetch path instead splits this into the
    /// worker's `fetchGapsFromSource` + the consume's `backfillBytes`. Returns true if any
    /// source read happened (the caller's live-connection keep/drop signal).
    ///
    /// `fetch_window` is the cache-ALIGNED window to read and cache (the aligned prefix/suffix
    /// fills whole cells); `requested_window` is what the caller actually asked for. They
    /// differ by the alignment slack, which is counted as over-read and split into its own
    /// rope buffer. When no alignment applies the two are equal.
    bool fetchAndBackfillGaps(
        ByteRange fetch_window,
        ByteRange requested_window,
        Rope & result,
        IntervalSet & covered,
        ConnState & conn,
        bool & eof_latch,
        MemoryPressureLevel pressure_level,
        Stats & out_stats);

    /// PURE source fetch: read the WHOLE `physical_window` from the source as one
    /// contiguous physical Rope (short at EOF), no cache/plan/pin. This is ALL a
    /// prefetch worker runs (it cannot touch shared state). `from_prefetch` routes the
    /// issued-source counter into the worker's job-local `Stats`. `eof_latch` is set on
    /// a size-unknown short read. The window is the plan gap the foreground bounded at
    /// submit (the aligned miss ranges already live in the plan geometry).
    Rope fetchGapsFromSource(ByteRange physical_window, bool from_prefetch, bool keep_live, ConnState & conn,
        bool & eof_latch, MemoryPressureLevel pressure_level, Stats & out_stats);

    /// Backfill the cache for `physical_window` from `source_bytes` (the whole window
    /// already fetched from the source). FOREGROUND-only: serve any late-hit from cache
    /// (`serveLateHits`), append the still-missing ranges from `source_bytes` (clamped to
    /// what it actually delivered) into `result` through `covered` (assembly truth is the
    /// SOURCE Rope), then push the assembled misses into the plan's held write buffers
    /// (`pushAssembledToWriteBuffers`, fire-and-forget). The plan's held write buffers
    /// (not a per-window collection) take the writes, so their deferred LRU-bump runs
    /// after the writes and the in-flight segment can be pinned from them.
    void backfillBytes(
        ByteRange physical_window, const Rope & source_bytes,
        Rope & result, IntervalSet & covered, Stats & out_stats);

    /// Shared tail of an assembled window: re-point the Strategy-A pin to the partial
    /// segment under `pin_frontier` - the frontier the live connection actually reached,
    /// which (with page-block alignment) can sit past `slice_window.end()` - cleared at
    /// EOF / no live connection; then slice `result` to `slice_window` and enforce the
    /// single-contiguous-run-from-the-window-start guarantee. Used by both
    /// `readPhysicalWindow` (foreground sync) and the prefetch consume path. The pin is
    /// taken from the plan's held write buffers (`writerPinAt`).
    Rope finalizeAssembledWindow(ByteRange slice_window, size_t pin_frontier, Rope & result,
        ConnState & conn, bool eof_latch) const;

    /// Push the assembled `result`'s miss bytes into the plan's held write buffers
    /// (`writer->write(result.slice(miss))`), fire-and-forget: a short/zero landing
    /// affects only `BytesPushedToCacheSync`, never `result` (`[CF-contiguity]`). Writes
    /// only into the plan's authoritative `BufEntry::writers` (`chassert(writer)`); never
    /// the planResidency view's null-writer misses (`[CF-mutate]`). The write side of an
    /// assembled window.
    void pushAssembledToWriteBuffers(ByteRange physical_window, const Rope & result, Stats & out_stats);

    /// Promote a range just served from `from_tier` up into every populatable cache
    /// faster than it (those all miss it, since `from_tier` was the fastest hit). Walks
    /// the plan's held write buffers (`read_plan.bufs`) in chain order and BREAKS at the
    /// first `BufEntry` whose `provider->tier() == from_tier` (tier-equality, so a slower
    /// fs hit is never promoted to a faster fs - `[CF-promote]`), writing served bytes
    /// into faster populatable tiers' write buffers via `writer->write`. The committed-set
    /// makes out-of-order/sub-block promote slices idempotent; `chassert(writer)`. A no-op
    /// when nothing faster is populatable. `bytes`/`range` are file-level (physical),
    /// pre-decryption.
    void maybePromote(CacheTier from_tier, ByteRange range, const Rope & bytes, Stats & out_stats);

    /// Re-credit, BEFORE the source fetch, any committed prefix of a frozen miss that a
    /// concurrent (or this plan's own) writer has grown since plan-build: for each held
    /// write buffer, serve `[range.offset, committed-frontier)` ∩ `window` from the
    /// buffer's own `read`, marking it `covered` so only the truly-uncommitted tail drives
    /// the fetch + `setReadUntilPosition` (`[CF-partial-prefix]`). Served at the buffer's
    /// tier; under the SAME shared `covered`.
    void recreditCommittedPrefixes(ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats);

    /// Query cache residency ONCE over `[physical_start, physical_start +
    /// plan_look_ahead_window)` (clamped to the file end / read extent) via the
    /// read-only `ICacheProvider::planResidencyView`, stash the resident ranges in the
    /// immutable geometry and the hit read buffers + opened write buffers in the
    /// foreground-private `read_plan.bufs`. While the plan is held, `serveResidentFromPlan`
    /// streams resident bytes straight from the held read buffers — no per-window
    /// `getOrSet`. Rebuilt lazily whenever the cursor leaves the planned span. Resets the
    /// in-flight pin before discarding the old plan (`[CF-plan-rebuild]`).
    void planResidencyWindow(size_t physical_start);

    /// TRIM phase of the plan: the look-ahead span starting at physical `physical_start`,
    /// clamped to the physical file end (when known) and the advertised read extent.
    /// Empty (`size == 0`) when the start already sits at/past a bound - the caller then
    /// publishes an empty plan. The single place the plan is bounded, so every range it
    /// later holds is one the reader will actually consume.
    ByteRange boundedPlanSpan(size_t physical_start) const;

    /// Read from source into the pre-allocated `blocks`. Reuses the open connection if it
    /// continues; otherwise opens a kept-live connection when `keep_live` (a wide leased
    /// plan), else a one-shot. `keep_live` is decided by the caller, NOT here (the
    /// foreground passes `bool(connection_lease)`, the worker passes its `job` flag) -
    /// `readFromSource` never takes or releases the lease itself.
    /// `blocks` is consumed: blocks that receive data become RopeNodes in the returned Rope;
    /// blocks that receive no data (e.g., file ended early) are released when this function returns.
    Rope readFromSource(
        const StoredObject & object, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset,
        bool keep_live, ConnState & conn, Stats & out_stats);

    /// Before dropping the live connection away from its bound, if only a small
    /// tail (<= `max_tail_for_drain`) remains, read it out so the connection
    /// completes and returns to the pool reusable instead of counting an
    /// incomplete connection. No-op when there is no bound or the tail is larger.
    /// Over-read bytes are charged to `out_stats` (the worker's job-local stats on
    /// a prefetch drain, `this->stats` on a foreground drop). Operates on `conn`
    /// (const: touches no `this` state).
    bool maybeDrainLiveTail(ConnState & conn, Stats & out_stats) const;

    /// Allocate enough OwnedRopeBuffers to cover `size` bytes, each ≤ `block_size`.
    /// `splits` (sorted, relative offsets within `[0, size)`) forces a block boundary at each
    /// listed offset so the resulting `OwnedRopeBuffer` allocations don't straddle those points.
    /// Used to keep user-window bytes and over-read bytes in separate buffers so each can be
    /// released independently.
    static VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> allocateBlocks(
        size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits = {});

    void maybeTriggerPrefetch();
    /// Drop the in-flight prefetch. `cancelled` is true for a real cancellation
    /// (`seek` / extent change) so it counts into `ReaderExecutorPrefetchCancelled`,
    /// false for destructor cleanup (which is not a user-visible cancellation).
    void discardPrefetch(bool cancelled);

    void drainAbandonedPrefetches(bool wait_finished = false);

    /// EOF detection has two cases:
    ///   - size known: `position >= totalSize()`.
    ///   - size unknown: the source's short return latches `reached_eof`.
    /// Seek backward clears `reached_eof` so the source can re-deliver.
    bool atEnd() const
    {
        return reached_eof || (!offset_map.hasUnknownSize() && position >= totalSize());
    }

    /// Take a `buffer_limit` lease (object-agnostic) and record the outcome in the
    /// `ReaderExecutorBufferSlot{Acquired,Failed}` counters. Returns an empty lease at
    /// capacity. Caller must hold a non-null `buffer_limit`.
    LiveConnectionSlot acquireSlotCounted();

    /// Acquire `connection_lease` before a wide source read (plan span `> window_size`),
    /// unless one is already held or this is a transient. Called from the gap-read path
    /// (NOT `readFromSource`); the lease is released when the connection closes.
    void acquireLeaseIfWide();

    /// Effective window size for the next read: `effectiveBlockSize()` when we're
    /// (or about to be) on the live path, otherwise the constructor-supplied
    /// `window_size` clamped down by `level`. Caller still caps by remaining file
    /// bytes. `level` is the per-plan cached `MemoryPressureLevel` (from the
    /// `read_plan.geometry()`/job snapshot) - NOT a fresh global query per call.
    size_t effectiveWindowSize(MemoryPressureLevel level) const;

    /// Effective per-block allocation size: the configured `block_size` at
    /// normal memory, shrinks under `level` (see `MemoryPressureMonitor`). Sizes
    /// the `allocateBlocks` source-read tile so the in-flight allocation per call
    /// falls when free memory does.
    size_t effectiveBlockSize(MemoryPressureLevel level) const;

    /// Read-ahead window for the next prefetch: the full `effectiveWindowSize` (the
    /// same window a synchronous read uses) at Normal/Elevated, and 0 — prefetch
    /// suppressed — at High/Critical. Read-ahead is speculative, so once memory is
    /// tight it stops entirely rather than reading a shrunken window. `level` is the
    /// per-plan cached level.
    size_t effectivePrefetchWindowSize(MemoryPressureLevel level) const;

    /// Shrink `win_size` so the read does not pass `read_extent_end` (the
    /// `makeTransientForReadAt` one-shot extent, or a sequential reader's
    /// `setReadExtent`). No-op when no extent is set. Saturates to 0 once
    /// `position` reaches the extent (an empty window, recoverable: extending the
    /// extent resumes - the same contract as the legacy `setReadUntilPosition`).
    size_t clampToExtent(size_t win_size) const;

    /// Trim a desired (logical) read size at the current `position` to the file end
    /// (when known) and the advertised read extent - the per-read analogue of
    /// `boundedPlanSpan`, so a single window slice never reaches past what the plan
    /// was bounded to. An unknown-size source has no known file end here (EOF is
    /// latched by a short read), so only the extent bounds it.
    size_t boundedReadSize(size_t want) const;

    /// Return the live connection to the pool the moment it has been read to its
    /// right bound (the advertised extent, or a one-shot block): it is fully
    /// drained and reusable, and dropping it lets the next read open a fresh
    /// streamed connection instead of falling to the stateless one-shot path.
    /// Operates on `conn` (const: touches no `this` state).
    void releaseLiveConnectionAtBound(ConnState & conn) const;
    /// Account a `connection` about to be dropped: count it as an incomplete
    /// (not pool-reusable) connection unless it was drained to its effective end.
    /// `at_eof` lets EOF drop sites treat a reached-EOF connection as complete.
    /// The incomplete-connection count lands in `out_stats` (job-local on a worker
    /// drop, `this->stats` on a foreground drop). Inspects `conn.connection`.
    void accountLiveConnectionDrop(ConnState & conn, bool at_eof, Stats & out_stats) const;

    /// Close a live connection that will not be continued: drain its tail, account the
    /// drop (EOF-complete vs incomplete), and clear the connection + its segment pin in
    /// one place. Does NOT touch the lease (`conn.slot`) - an abandon-drop releases it,
    /// `readFromSource`'s close-to-reopen keeps it. No-op without a live connection.
    void dropLiveConnection(ConnState & conn, Stats & out_stats) const;

    /// Decide an open live connection's fate before the next read at
    /// `next_physical`: keep it only while that read is a small bridgeable forward
    /// gap within its bound (so the next source read skips the gap on it instead
    /// of reopening); otherwise drain its tail and drop it (and its pin) so the
    /// slot isn't held idle. No-op without a live connection or at EOF. Called
    /// after every cache-only serve (resident run, or a cache-only gap window).
    /// Drain/drop accounting lands in `out_stats` (see `maybeDrainLiveTail`).
    /// Operates on `conn` (const: touches no `this` state beyond read-only members);
    /// `reached_eof` is passed (foreground member or the worker's job latch) since this
    /// runs on both paths and the worker must not read the shared executor member.
    void maybeKeepLiveConnectionBefore(size_t next_physical, ConnState & conn, bool eof_latch, Stats & out_stats) const;

    /// readPhysicalWindow + remap the window's offsets to logical (subtract the
    /// encryption header). Payload decryption is deferred to the consumer
    /// (PipelineReadBuffer), so unconsumed read-ahead is never decrypted.
    Rope readWindowLogical(ByteRange physical_window, ConnState & conn,
        const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats);

    std::shared_ptr<IFileBasedSourceReader> source;
    StoredObjects stored_objects;  /// retained for makeTransientForReadAt
    OffsetMap offset_map;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    /// File path used only for `system.reader_executor_log` /
    /// `ReaderExecutorLogElement::source_file_path`. Cache identity is
    /// derived per-object by the cache providers themselves and no longer
    /// goes through the executor.
    String log_file_path;
    size_t window_size;
    size_t min_bytes_for_seek;
    size_t block_size;
    /// Drain bound for `maybeDrainLiveTail` (constructor-supplied, like the others).
    size_t max_tail_for_drain;
    /// Minimum gap reach (see `streamReach`) that warrants a live connection + lease in
    /// `acquireLeaseIfWide`. Defaults to `window_size`; `setLiveConnectionMinReadBytes`
    /// overrides it from `reader_executor_live_connection_min_read_bytes` (tests set it low
    /// so a small read still exercises the live-connection path).
    size_t live_connection_min_read_bytes;
    /// Look-ahead span for plan-then-stream (see `DEFAULT_PLAN_LOOK_AHEAD`);
    /// raised to at least `window_size` so a plan always covers a full window.
    size_t plan_look_ahead_window;
    size_t position = 0;

    /// One cache tier's RESIDENT geometry over the look-ahead window, for ONE
    /// object-piece: the file-level (physical-coordinate) ranges this tier holds
    /// resident, plus the cache-ALIGNED miss ranges this populatable tier lacks
    /// (`aligned_miss`, the gap-fetch + write targets). Holds NO cache buffer - those
    /// live in the same plan's foreground-private `ReadPlan::bufs` - so the geometry is
    /// immutable and `ReadPlan` hands it out as a `shared_ptr<const ReadPlanGeometry>`
    /// snapshot (`ReadPlan::geometry()`). `aligned_miss` is NOT clamped to the plan span /
    /// `read_extent_end`; it may extend past them, clamped only to object end so the
    /// cache segment/block is fully populated and the over-read bound is the aligned
    /// extent (`[CF-overread]`).
    struct GeometryEntry
    {
        CacheTier tier{};
        /// Grid the fetch rounds to at each edge of a miss run, filling the aligned
        /// prefix/suffix of the cache cell this read starts/ends in (`ICacheProvider::
        /// fetch{Head,Tail}Alignment`). `1` = no over-read. See `fetchWindowAt`.
        size_t head_align = 1;
        size_t tail_align = 1;
        VectorWithMemoryTracking<ByteRange> resident;
        VectorWithMemoryTracking<ByteRange> aligned_miss;
    };

    /// The IMMUTABLE geometry of one look-ahead plan: the resident layout + span,
    /// queried positionally by `readNextWindow` / `maybeTriggerPrefetch` (RESIDENT
    /// run vs GAP). Built once by `planResidencyWindow` and never mutated after
    /// publish; owned by `ReadPlan` and exposed as a `shared_ptr<const>` snapshot via
    /// `ReadPlan::geometry()`. `entries` is in cache-tier priority order (same order
    /// as `caches`), 1:1 POSITIONAL with `ReadPlan::bufs`, so `residentAt` returns the
    /// index of the fastest-tier entry
    /// holding a byte and the caller maps it to that entry's held read buffers. Empty /
    /// `plan_end == plan_start` means no valid plan.
    struct ReadPlanGeometry
    {
        static constexpr size_t npos = static_cast<size_t>(-1);

        size_t plan_start = 0;  /// physical (header-inclusive) coords
        size_t plan_end = 0;    /// [plan_start, plan_end)
        VectorWithMemoryTracking<GeometryEntry> entries;

        /// The `MemoryPressureMonitor` level sampled ONCE when this plan was built
        /// (`planResidencyWindow`). Reads within the plan use this cached level for
        /// their effective block/window sizing instead of re-querying the global
        /// monitor per call - so a warm cache-hit scan does zero pressure queries, and
        /// a prefetch worker reads it from its co-owned snapshot (stays a pure job).
        MemoryPressureLevel pressure_level{};

        bool covers(ByteRange w) const
        {
            return plan_end > plan_start && w.offset >= plan_start && w.end() <= plan_end;
        }

        /// What the plan holds at file-level `offset`: the index of the entry whose
        /// resident range covers it (`npos` = gap), its tier, and the end of that
        /// contiguous resident run. The caller maps `entry` to the held read buffers
        /// in `ReadPlan::bufs[entry]`.
        struct Resident
        {
            size_t entry = npos;
            CacheTier tier{};
            size_t run_end = 0;
            bool resident() const { return entry != npos; }
        };
        Resident residentAt(size_t offset) const
        {
            for (size_t i = 0; i < entries.size(); ++i)
                for (const auto & r : entries[i].resident)
                    if (offset >= r.offset && offset < r.end())
                        return {i, entries[i].tier, r.end()};
            return {};
        }

        /// First non-resident (gap) offset at or after `from`, within the plan;
        /// `plan_end` if everything from `from` to the plan end is resident.
        size_t nextGapStart(size_t from) const
        {
            size_t pos = std::max(from, plan_start);
            while (pos < plan_end)
            {
                auto r = residentAt(pos);
                if (!r.resident())
                    return pos;
                pos = r.run_end;
            }
            return plan_end;
        }

        /// End of the gap starting at `gap_start`: the next resident range's start
        /// after it, or `plan_end` if none follows.
        size_t gapEnd(size_t gap_start) const
        {
            size_t end = plan_end;
            for (const auto & entry : entries)
                for (const auto & r : entry.resident)
                    if (r.offset > gap_start && r.offset < end)
                        end = r.offset;
            return end;
        }

        /// The window to FETCH in order to serve `req`: `req` rounded OUT to the cache cell
        /// at each edge, so a touched cell is populated from its aligned floor. The result
        /// MAY START LEFT of `req.offset` (the disk-segment/boundary head below it) and end
        /// past `req.end()` (the page-block/segment tail) - the streaming code reads this
        /// extent and slices the delivered window back to `req`. A `req` with no overlapping
        /// aligned miss (fully resident) is returned unchanged. A pure read of the immutable
        /// geometry (replaces the old `alignToCaches` probe).
        ///
        /// The widening is BOUNDED by each tier's alignment grid (`head_align`/`tail_align`),
        /// NOT the length of the coalesced miss run: a cold scan's miss run can span the whole
        /// file, and unioning it would fetch the entire file in one request. Only the cell the
        /// read STARTS in (head, rounded down) and - for whole-block-write tiers - the cell it
        /// ENDS in (tail, rounded up) are pulled in, each clamped into the miss run so the
        /// extension never reaches into resident bytes. The head slack below `req` that the
        /// foreground already covered from earlier windows is dropped by `covered`; only the
        /// genuinely-missing aligned prefix is fetched and counted as over-read (`[CF-overread]`).
        ByteRange fetchWindowAt(ByteRange req) const
        {
            if (req.size == 0)
                return req;
            size_t lo = req.offset;
            size_t hi = req.end();
            for (const auto & entry : entries)
                for (const auto & m : entry.aligned_miss)
                {
                    /// Head: `req` starts inside this miss run -> round its offset down to
                    /// the tier's grid, clamped to the run start (bounded by `head_align`).
                    if (entry.head_align > 1 && m.offset <= req.offset && req.offset < m.end())
                    {
                        size_t floored = (req.offset / entry.head_align) * entry.head_align;
                        lo = std::min(lo, std::max(m.offset, floored));
                    }
                    /// Tail: `req` ends inside this miss run -> round its end up to the
                    /// tier's grid, clamped to the run end (bounded by `tail_align`). `1`
                    /// (incremental tiers) never extends.
                    if (entry.tail_align > 1 && m.offset < req.end() && req.end() <= m.end())
                    {
                        size_t ceiled = ((req.end() + entry.tail_align - 1) / entry.tail_align) * entry.tail_align;
                        hi = std::max(hi, std::min(m.end(), ceiled));
                    }
                }
            return ByteRange{lo, hi - lo};
        }

        /// How far a live connection opened at `from` would stream before it must reopen:
        /// it reads gaps and bridges resident (cached) runs no larger than `min_gap` (the
        /// `min_bytes_for_seek` skip-forward), stopping at the first resident run too large
        /// to bridge, or `plan_end`. The returned end minus `from` is the connection's
        /// useful reach; a reach no larger than a window means a one-shot serves it and no
        /// kept-live connection (lease) is warranted - even when individual gaps are tiny
        /// but bridged across a wide region (the page-cache small-hole case).
        size_t streamReach(size_t from, size_t min_gap) const
        {
            size_t pos = std::max(from, plan_start);
            while (pos < plan_end)
            {
                auto r = residentAt(pos);
                if (!r.resident())
                {
                    pos = gapEnd(pos);  /// stream across the gap
                    continue;
                }
                /// A resident run [pos, run_end): bridge it only if small enough to skip
                /// forward over AND something follows it (a trailing resident run is just
                /// where the connection stops, not bridged).
                if (r.run_end - pos <= min_gap && r.run_end < plan_end)
                {
                    pos = r.run_end;
                    continue;
                }
                break;
            }
            return pos;
        }
    };

    /// One (object-piece, tier) entry of the FOREGROUND-PRIVATE half of a look-ahead
    /// plan: the `provider`/`object`/`object_file_offset` identity, the read-only
    /// `planResidencyView` `view` (its `hits()` own the held pinning read buffers; its
    /// `misses()` carry NULL writers and are NEVER dereferenced - they only fed
    /// `GeometryEntry::aligned_miss`), and the AUTHORITATIVE `writers` opened by
    /// `openWriteBuffers` over those aligned miss ranges (`[CF-mutate]`). 1:1 POSITIONAL
    /// with `ReadPlanGeometry::entries`, provider-grouped fastest-first (so the first
    /// same-tier `BufEntry` is the fastest sibling, which `maybePromote`'s tier-equality
    /// break relies on). A prefetch worker NEVER indexes this (over its pure-gap window
    /// it reads only geometry), so the buffers are never shared across threads.
    struct BufEntry
    {
        ICacheProvider * provider = nullptr;
        StoredObject object;
        size_t object_file_offset = 0;
        CacheViewPtr view;
        std::vector<MissEntry> writers;
    };

    /// One look-ahead plan and the SOURCE OF TRUTH for the current read: the immutable
    /// geometry snapshot (`geometry()`), the held hit read buffers + opened write buffers
    /// (`bufs`, 1:1 positional with `geometry()->entries`), and the plan-lifetime late-hit
    /// views held only for their deferred LRU bump. FOREGROUND-PRIVATE: a prefetch worker
    /// never indexes it (it reads a pre-bounded window). Held across many windows;
    /// destroyed (write buffers finalize, deferred LRU-bumps run) at the next
    /// `planResidencyWindow` / on seek - after every write into the held buffers (`[CF-lru]`).
    struct ReadPlan
    {
        /// The immutable geometry projection of this plan (resident + aligned-miss ranges,
        /// no buffers). `ReadPlan` owns it; readers take a `shared_ptr<const>` snapshot
        /// through this method. Null until the first plan is built.
        const std::shared_ptr<const ReadPlanGeometry> & geometry() const { return geometry_snapshot; }

        /// Late-hit `CacheView`s held ALIVE until plan teardown SOLELY for their
        /// destructors: `~CacheView` runs the deferred LRU bump, which must land AFTER the
        /// write buffers in `bufs` are finalized. Declared BEFORE `bufs` so it is destroyed
        /// AFTER it (members destruct in reverse declaration order), giving bump-after-writes
        /// (`[CF-lru]`). Never indexed - the value is the destruction timing; writers ignored.
        std::vector<CacheViewPtr> deferred_lru_bumps;

        VectorWithMemoryTracking<BufEntry> bufs;

    private:
        friend class ReaderExecutor;  /// `planResidencyWindow` is the sole writer of `geometry_snapshot`.
        /// Set once by `planResidencyWindow`; read via `geometry()`. Holds only ByteRanges,
        /// so its destruction order relative to `bufs`/`deferred_lru_bumps` is irrelevant.
        std::shared_ptr<const ReadPlanGeometry> geometry_snapshot;
    };

    /// The current look-ahead plan and source of truth: held buffers + the immutable
    /// geometry snapshot it exposes via `read_plan.geometry()`, which is null until the
    /// first plan is built (the query methods' callers guard for that).
    ReadPlan read_plan;

    /// Pin the partial segment under `frontier` from the first held write buffer whose
    /// `range()` contains it and whose `pin(frontier)` is non-null (the full 3-part
    /// guard lives in the write buffer). Empty when nothing partial is there - the
    /// frontier landed in a hit region or past the last open writer (`[CF-pin]`).
    CacheWriter::CacheSegmentPin writerPinAt(size_t frontier) const
    {
        for (const auto & buf : read_plan.bufs)
            for (const auto & w : buf.writers)
                if (w.writer && frontier >= w.writer->range().offset && frontier < w.writer->range().end())
                    if (auto pin = w.writer->pin(frontier))
                        return pin;
        return {};
    }

    /// Translate ONE tier's `planResidencyView` into its 1:1 `GeometryEntry`/`BufEntry`
    /// (the per-piece body of `planResidencyWindow`, factored out so the translate reads as
    /// data-flow). `extractResidentRuns` records the tier's hits as resident ranges (clamped
    /// to the plan span). `extractMissesAndOpenWriters` records its cache-aligned misses as
    /// the fetch/write targets and opens the held write buffers (populatable tiers only).
    static void extractResidentRuns(const CacheView & view, ByteRange plan_range, GeometryEntry & geom_entry);
    static void extractMissesAndOpenWriters(
        ICacheProvider & cache, const CacheView & view,
        const StoredObject & object, size_t object_file_offset,
        GeometryEntry & geom_entry, BufEntry & buf_entry);

    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    /// Single source of truth for "is there a prefetch scheduled":
    /// `prefetch_handle != nullptr`. `prefetch_range` is only meaningful when
    /// the handle is non-null.
    std::shared_ptr<PrefetchHandle> prefetch_handle;
    /// `prefetch_range` is the LOGICAL requested read-ahead range (the space `position`,
    /// seek and the consume slice work in). `prefetch_physical_window` is the PHYSICAL,
    /// cache-aligned window the worker actually fetched (`ReadPlanGeometry::fetchWindowAt`
    /// widened it to whole page blocks / the disk-segment boundary) - the consume path
    /// backfills the caches over it and pins at its frontier. Both meaningful only while
    /// the handle is set.
    ByteRange prefetch_range;
    ByteRange prefetch_physical_window;
    /// The in-flight prefetch worker's job, co-owned with the worker lambda. Holds
    /// the worker's job-local `Stats` AND the source-connection cluster (`conn`) it
    /// took ownership of at submit. The worker writes ONLY `prefetch_job->stats` and
    /// operates ONLY on `prefetch_job->conn` (never the shared `this->stats` /
    /// `this->conn`); the foreground reconciles at join once the `get()`/`tryCancel`
    /// happens-before edge is established: `mergePrefetchJobStats` folds the stats in,
    /// and the connection cluster is moved back into `this->conn` (consume /
    /// cancel-queued) or dropped (discard-running). Because a job-local `Stats` starts
    /// at zero, its `prefetch_issued_*` ARE this prefetch's issued bytes, so a discard
    /// attributes exactly them to wasted (no snapshot needed). Null when no prefetch
    /// is in flight; a cancelled-while-queued job is untouched (worker never ran), so
    /// its `conn` is reclaimed and its stats stay zero.
    std::shared_ptr<PrefetchJob> prefetch_job;
    /// Merge a resolved prefetch's job-local stats into `this->stats`. `wasted` ⟹
    /// the rope was discarded unconsumed (a running prefetch dropped by seek /
    /// extent-change): the bytes still crossed the wire so they count as issued I/O,
    /// and additionally as wasted. Clears `prefetch_job` is the CALLER's job (it also
    /// reclaims/drops the connection cluster); this only touches the stats half.
    /// No-op (and harmless) for a cancelled-while-queued job whose stats are zero.
    void mergePrefetchJobStats(bool wasted);
    /// Cancelled prefetches whose worker may still be inside the pool job
    /// slot. The destructor waits on each; running calls sweep finished ones
    /// to keep the vector bounded under seek-heavy workloads.
    VectorWithMemoryTracking<std::shared_ptr<PrefetchHandle>> abandoned_prefetches;
    /// Set when the source returned fewer bytes than requested AND the
    /// total file size is unknown — in that mode the short return IS the
    /// EOF marker. `readNextWindow` consults this so a subsequent call
    /// short-circuits to EOF without re-issuing a read.
    bool reached_eof = false;

    /// One kept-open source connection for a sequential cold scan: the open source
    /// buffer, the object it streams (`object_path`), the object-local frontier
    /// (`current_position`) it has streamed to, and the right bound (`read_until`,
    /// `nullopt` = open-ended) it was opened to via `setReadUntilPosition`. Reused
    /// across windows while the next read continues forward within the bound; a read
    /// past it reopens, so the connection is always read to its bound and returned to
    /// the pool drained and reusable. The global-limit lease lives in `ConnState::slot`.
    /// Owns the mechanics of reading/skipping/draining its own connection; the
    /// executor owns the policy (when to keep or drop it) and the stats.
    struct Connection
    {
        size_t current_position = 0;
        std::optional<size_t> read_until;
        std::unique_ptr<ReadBufferFromFileBase> buffer;
        /// The object this open connection streams - used to decide whether the next
        /// read can continue it (same object) or must reopen. (The global limit lease
        /// lives in `ConnState::slot`, object-agnostic.)
        String object_path;

        /// Read to its right bound — fully consumed and pool-reusable. Always false
        /// for an open-ended connection.
        bool atBound() const { return read_until && current_position >= *read_until; }

        /// Dropping now leaves the connection pool-reusable: read to its bound, or EOF.
        bool isComplete(bool at_eof) const { return at_eof || atBound(); }

        /// Can be continued forward to object-local `next_local` without reopening:
        /// forward, within `min_gap` of the frontier, and not past the bound (the same
        /// over-read-vs-reopen trade `min_bytes_for_seek` makes; the executor supplies it).
        bool canContinueTo(size_t next_local, size_t min_gap) const
        {
            return next_local >= current_position
                && next_local - current_position <= min_gap
                && (!read_until || next_local <= *read_until);
        }

        /// Read the pre-allocated `blocks` off the open connection into a Rope using
        /// `set()`/`next()` (data lands directly in block memory), advancing the frontier.
        Rope readInto(VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks,
                      size_t logical_offset, const LoggerPtr & logger);

        /// Discard up to `gap` bytes off the open connection so the frontier advances
        /// over an already-cached hole; the bytes cross the wire (over-read), only the
        /// source request is saved. Returns bytes skipped (< `gap` only at EOF).
        size_t skipForward(size_t gap, size_t block_bytes);

        /// If only a small tail (<= `max_tail`) remains to the bound, read it out so
        /// the connection completes and is returned to the pool reusable rather than
        /// abandoned. Returns bytes drained (0 when open-ended, already at the bound,
        /// or the tail is larger); drained bytes are over-read (the caller accounts them).
        size_t drainTail(size_t max_tail, size_t block_bytes);
    };

    /// The source-connection cluster a prefetch worker takes ownership of for the
    /// duration of its job. Bundled and threaded as a `ConnState &` (exactly like
    /// `Stats & out_stats`) so a worker operates on its OWN cluster
    /// (`prefetch_job->conn`) and the foreground on `this->conn` — the
    /// shared-`connection` use-after-free becomes a compile-time impossibility,
    /// not a runtime invariant. Move-only (holds the connection's `unique_ptr`).
    struct ConnState
    {
        /// Special members are out-of-line (defined in the .cpp) because the inline
        /// move would instantiate `optional<Connection>`'s operations here, where
        /// `Connection`'s `unique_ptr<ReadBufferFromFileBase>` is still incomplete.
        ConnState();
        ~ConnState();
        ConnState(const ConnState &) = delete;
        ConnState & operator=(const ConnState &) = delete;
        /// Move transfers the cluster and leaves the source genuinely EMPTY -
        /// `std::optional`'s own move leaves the source ENGAGED (holding a moved-from
        /// value), so the .cpp resets it explicitly; that is what lets the foreground
        /// reclaim a job's cluster with a plain move and no manual clearing.
        ConnState(ConnState && other) noexcept;
        ConnState & operator=(ConnState && other) noexcept;

        std::optional<Connection> connection;

        /// While streaming sequentially through a DiskCache/FileCache segment, hold
        /// a bare ref to that segment so a mid-read eviction can't snap the next
        /// miss head back to the segment start and force a connection reset + a
        /// re-read of bytes already delivered. Re-pointed each window to the
        /// segment under the live frontier (from the plan's held write buffer's
        /// `pin`); dropped on seek/EOF/connection reset / plan rebuild.
        CacheWriter::CacheSegmentPin inflight_segment_pin;
    };

    /// The foreground's connection cluster. EMPTY while a prefetch is in flight —
    /// the cluster is moved into `prefetch_job->conn` at submit and moved back on
    /// consume / cancel-queued (dropped on discard-running). Named distinctly from
    /// the read-path `ConnState & conn` parameter (which a worker binds to
    /// `prefetch_job->conn` instead) so the two never shadow.
    ConnState foreground_connection_state;
    std::shared_ptr<LiveConnectionLimit> buffer_limit;
    /// The executor's single live-connection lease (one global-limit unit). Taken lazily
    /// before a WIDE gap read - one whose `streamReach` from the cursor exceeds `window_size`,
    /// so a source connection is kept live and reused across windows; a gap that fits one
    /// window (narrow tail, or a mostly-resident plan with tiny holes) reads short one-shots
    /// and holds no lease. Released whenever no live connection remains (drop / seek / EOF /
    /// re-plan). A prefetch worker is told whether its read is leased via `PrefetchJob::leased`,
    /// so it never reads this shared member.
    LiveConnectionSlot connection_lease;
    std::shared_ptr<ReaderExecutorLog> reader_executor_log;
    String creator_query_id;

    /// Logical end of the advertised read region: the `makeTransientForReadAt`
    /// one-shot extent, or a sequential reader's `setReadExtent` (from
    /// `setReadUntilPosition`). When set, windows are clamped to it and the live
    /// connection is bounded to it, so the borrowed connection is read to a known
    /// end and returned to the pool reusable. `nullopt` = read to the file end.
    std::optional<size_t> read_extent_end;

    /// True on a `makeTransientForReadAt` executor. Such an executor does not emit
    /// its own ProfileEvents / `reader_executor_log` row in the destructor — its
    /// stats are rolled into the parent via `mergeTransientStats`, so they would
    /// otherwise be double-counted.
    bool is_transient = false;
    /// Serializes `mergeTransientStats`: concurrent `readBigAt` calls roll their
    /// transients' stats into this one parent.
    std::mutex transient_stats_mutex;

#if USE_SSL
    /// Decryption
    struct DecryptionLayer
    {
        String path;
        size_t buffer_size;
        KeyFinderFunc key_finder;
        /// Populated by initDecryption
        String key;
    };

    VectorWithMemoryTracking<DecryptionLayer> decryption_layers;
    VectorWithMemoryTracking<FileEncryption::Header> decryption_headers;
    bool decryption_initialized = false;
    /// Persistent per-layer CTR encryptors, built lazily by decryptInPlace from
    /// the parsed headers and reused across served chunks. Single-threaded per
    /// executor (consumer's nextImpl, or one transient per readBigAt).
    VectorWithMemoryTracking<FileEncryption::Encryptor> payload_encryptors;
#endif
    size_t data_start_offset = 0;  /// N * Header::kSize (0 when no encryption)

    /// Per-executor accumulating stats. Flushed to ProfileEvents and logged at
    /// destruction. Cumulative; the destructor emits one summary line so
    /// triaging a slow query needs only the server log, not a separate trace.
    struct Stats
    {
        /// One entry per counter. `add` is the only mutator and the single place that
        /// maps a counter to its ProfileEvent (see `ReaderExecutor::Stats::add`), so a
        /// counter and its event can never drift apart and every update is observable
        /// instantly. The grouping mirrors the old named fields:
        ///   - bytes_from_*: physical bytes issued per serving tier (issued I/O incl.
        ///     background prefetch, not consumer-served bytes - that is RequestedBytes).
        ///   - BytesPushedToCache{Sync,Async}: bytes written back via `put`, by context.
        ///   - BytesPromoted: bytes written UP into a faster populatable tier.
        ///   - IncompleteConnections: source connections dropped before their right bound
        ///     (not pool-reusable; the metric's `I`).
        ///   - OverReadBytes: source bytes that did not serve the request (alignment slack
        ///     + bridged-gap bytes).
        ///   - RequestedBytes: useful bytes delivered to read requests (cost denominator).
        ///   - PrefetchSkippedResident: prefetches NOT submitted (next window fully
        ///     resident -> read synchronously); report-only, no ProfileEvent.
        ///   - PrefetchIssued*/Prefetch Wasted*: bytes a prefetch read (issued = all,
        ///     wasted = the subset a discarded running prefetch threw away).
        enum Counter : size_t
        {
            BytesFromPageCache,
            BytesFromFilesystemCache,
            BytesFromSource,
            BytesPushedToCacheSync,
            BytesPushedToCacheAsync,
            BytesPromoted,
            CacheGetRequests,
            CachePopulateRequests,
            SourceRequests,
            IncompleteConnections,
            OverReadBytes,
            RequestedBytes,
            CacheGetMicroseconds,
            CachePopulateMicroseconds,
            SourceReadMicroseconds,
            DecryptMicroseconds,
            PrefetchWaitMicroseconds,
            SyncReadMicroseconds,
            PrefetchHits,
            PrefetchCancelled,
            PrefetchPoolFull,
            PrefetchSkippedResident,
            PrefetchDiscardedRunning,
            PrefetchDiscardWaitMicroseconds,
            PrefetchIssuedSourceBytes,
            PrefetchIssuedCacheBytes,
            PrefetchWastedSourceBytes,
            PrefetchWastedCacheBytes,
            NumCounters
        };

        /// Setter: bump `c` by `value` AND emit its ProfileEvent (plus the modeled-cost
        /// contribution for the cost-model counters) - the one place ProfileEvents are
        /// incremented, so events advance as the read happens and the prefetch worker
        /// (running in the submitter's thread group) attributes to the query too.
        void add(Counter c, UInt64 value = 1);

        /// Getter: read a counter for the final report (the `Destroyed` log line and the
        /// `reader_executor_log` row). Does not emit.
        UInt64 get(Counter c) const { return values[c]; }

        /// Roll a transient `readBigAt` executor's (or a prefetch worker's) stats into the
        /// parent's report aggregate WITHOUT re-emitting: the source already emitted each
        /// counter to ProfileEvents at its `add`.
        Stats & operator+=(const Stats & o)
        {
            for (size_t i = 0; i < NumCounters; ++i)
                values[i] += o.values[i];
            return *this;
        }

    private:
        std::array<UInt64, NumCounters> values{};
    };

    /// RAII timer: on scope exit, add the elapsed microseconds to a `Stats` timing counter
    /// via `Stats::add` (which also emits the matching ProfileEvent), so even the `_us`
    /// counters flow through the one setter. Replaces a bare `StopwatchAccumulator`.
    class StatTimer
    {
    public:
        StatTimer(Stats & stats_, Stats::Counter counter_) : target(stats_), counter(counter_) {}
        ~StatTimer() { target.add(counter, watch.elapsedMicroseconds()); }

        StatTimer(const StatTimer &) = delete;
        StatTimer & operator=(const StatTimer &) = delete;

        UInt64 elapsedMicroseconds() const { return watch.elapsedMicroseconds(); }

    private:
        Stats & target;
        Stats::Counter counter;
        Stopwatch watch;
    };

    /// `mutable` so `const` read helpers can accumulate timings. Stats are
    /// observability, not state. The foreground owns this aggregate; a prefetch
    /// worker never writes it - it accumulates into its own job-local `Stats`
    /// (`prefetch_job->stats`), merged here at join under the future's `get()`
    /// happens-before edge.
    mutable Stats stats;

    /// The in-flight prefetch's co-owned job (see the `prefetch_job` member). Bundles
    /// EVERYTHING a prefetch worker touches, so the worker reads/writes ONLY job state
    /// — never a shared `this->` member: the job-local `Stats`, the source-connection
    /// cluster it owns for the job's duration, the per-plan `pressure_level` that sizes
    /// its source blocks (it does no cache lookup or resident serve, so it needs no
    /// geometry snapshot), and the `reached_eof` latch it sets on a size-unknown short
    /// read (OR-ed into the executor's member at consume).
    /// Defined here, after `Stats` and `ConnState`.
    struct PrefetchJob
    {
        Stats stats;
        ConnState conn;
        MemoryPressureLevel pressure_level{};
        /// Whether the read that launched this prefetch held the live-connection lease
        /// (a wide gap). Set by the foreground at submit so the worker opens a kept-live
        /// connection vs a one-shot WITHOUT reading the shared `connection_lease`.
        bool leased = false;
        bool reached_eof = false;
    };

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
