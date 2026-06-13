#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/LiveConnectionLimit.h>
#include <IO/ReadPlanGeometry.h>
#include <IO/FetchMachine.h>

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
#include <array>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "config.h"
#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#endif

namespace DB
{

class PrefetchThreadPool;
class ReaderExecutorLog;
class FetchMachineRunner;

/// Reads a logical file (one or more `StoredObject`s mapped by `OffsetMap`)
/// through a fastest-first cache chain, falling back to the source. Tuned for
/// sequential scans: keeps one source connection alive across windows, reads
/// the next gap ahead on a `PrefetchThreadPool`, and shrinks its window/block
/// sizes under memory pressure. Owns its cache and decryption layers, so it is
/// NOT wrapped by the legacy async/decrypt/cache read buffers.
///
/// One instance per column-stream; not thread-safe beyond the machine handoff:
/// while a fetch machine is in flight the worker exclusively owns the machine
/// payload, and the foreground reclaims it only through the runner's
/// revoke/release edges. Served-byte counters are NOT shared: a worker
/// accumulates into the machine's own `Stats`, merged at collect/cancel.
class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_WINDOW_SIZE = 8 * 1024 * 1024; /// 8 MiB
    /// Gap bound for the live-connection bridge / seek-keep and `mergeRanges`:
    /// a forward gap up to this is skipped on the open GET instead of
    /// reopening. Near the bandwidth/request cost breakeven.
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 2 * 1024 * 1024; /// 2 MiB
    /// Drain bound: a live connection dropped within this of its right bound
    /// is read out first, so it returns to the pool reusable.
    static constexpr size_t DEFAULT_MAX_TAIL_FOR_DRAIN = 1 * 1024 * 1024; /// 1 MiB
    static constexpr size_t ROPE_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB per Rope node
    /// Look-ahead span over which residency is planned ONCE (plan-then-stream),
    /// amortising cache discovery across many windows. Planning is disabled
    /// when this is below `window_size`.
    static constexpr size_t DEFAULT_PLAN_LOOK_AHEAD = 64 * 1024 * 1024; /// 64 MiB

    /// Everything configurable beyond the data path itself: the executor is
    /// fully wired at construction, there are no post-construction setters.
    struct Options
    {
        size_t window_size = DEFAULT_WINDOW_SIZE;
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK;
        size_t block_size = ROPE_BLOCK_SIZE;
        String log_file_path;
        size_t max_tail_for_drain = DEFAULT_MAX_TAIL_FOR_DRAIN;
        size_t plan_look_ahead_window = DEFAULT_PLAN_LOOK_AHEAD;
        /// 0 = use `window_size` (override comes from
        /// `reader_executor_live_connection_min_read_bytes`).
        size_t live_connection_min_read_bytes = 0;
        std::shared_ptr<PrefetchThreadPool> prefetch_pool;
        std::shared_ptr<LiveConnectionLimit> buffer_limit;
        std::shared_ptr<ReaderExecutorLog> reader_executor_log;
    };

    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches,
        Options options);

    /// All-defaults overload (cannot be a default argument: `Options{}` in a
    /// member declaration would need the initializers in a complete-class
    /// context).
    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches);

    /// Out-of-line because `Connection` holds `unique_ptr<ReadBufferFromFileBase>`.
    ~ReaderExecutor();

    // ─── Read path ───────────────────────────────────────────────────────

    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    /// Advertise the read extent (from `ReadBuffer::setReadUntilPosition`,
    /// driven per mark range by `MergeTreeReaderStream::adjustRightMark`). The
    /// executor bounds its live source connection to it - so the borrowed
    /// connection is read to a known end and returned to the pool reusable -
    /// and keeps prefetches within it. `nullopt` clears it. Drains an in-flight
    /// prefetch when the extent changes.
    void setReadExtent(std::optional<size_t> logical_end);

    // ─── Random access (`readBigAt`) ─────────────────────────────────────

    /// A fresh executor for `[start_position, start_position + read_size)`,
    /// sharing immutable state but owning its own position / live connection.
    /// Shares `buffer_limit`; gets no `prefetch_pool`/log (a one-shot read
    /// can't amortise prefetch). `read_size` bounds every read, so the
    /// borrowed connection is fully drained and pool-reusable.
    std::unique_ptr<ReaderExecutor> makeTransientForReadAt(size_t start_position, size_t read_size) const;

    /// Roll a drained transient executor's stats into this (parent) executor.
    /// Thread-safe: concurrent `readBigAt` calls share one parent.
    void mergeTransientStats(const ReaderExecutor & transient);

    /// All current sources support concurrent `open`, so this is true whenever
    /// a source is configured; future non-reusable sources can opt out.
    bool canReadAt() const { return static_cast<bool>(source); }

    // ─── Decryption ──────────────────────────────────────────────────────

    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer (callable multiple times for layered encryption).
    /// No-op without SSL. Call `initDecryption` once after all layers.
    void addDecryptionLayer(String path, size_t buffer_size, KeyFinderFunc key_finder);

    /// Read the encryption headers (one per layer) and resolve keys. Must run
    /// before any read; no-op when no layers / no SSL.
    void initDecryption();

    /// Whether served payload must be decrypted on consume (by
    /// `PipelineReadBuffer` via `decryptInPlace`).
    bool needsDecryption() const { return data_start_offset > 0; }

    /// Decrypt in place at `logical_offset` using persistent per-layer CTR
    /// encryptors; CTR is position-addressable, so the consumer decrypts only
    /// the chunk it serves. Single-threaded per executor.
    void decryptInPlace(char * data, size_t size, size_t logical_offset);

    // ─── Introspection ───────────────────────────────────────────────────

    size_t getPosition() const { return position; }

    /// Logical object path for diagnostics; empty when no objects are configured.
    String getFileName() const { return log_file_path; }

    /// Logical file size (physical size minus encryption headers). Saturates
    /// to 0 when the objects sum to fewer bytes than the declared headers.
    size_t totalSize() const;

    /// True iff the underlying object had `StoredObject::UnknownSize`. Callers
    /// converting `totalSize` into an `optional` file size MUST consult this
    /// first - `totalSize` is meaningless in that case.
    bool hasUnknownSize() const { return offset_map.hasUnknownSize(); }

    /// Test-only probes of the machine state.
    bool hasInflightPrefetch() const { return machine != nullptr; }
    size_t inflightPrefetchSize() const { return machine ? machine->requested_range.size : 0; }
    size_t abandonedPrefetchCount() const { return abandoned_machines.size(); }

    /// Test-only: the current look-ahead plan geometry (null until the first
    /// plan is built), for validating `describePlan` against the live walk.
    std::shared_ptr<const ReadPlanGeometry> planGeometryForTest() const { return read_plan.geometry(); }

    /// Merge ranges separated by less than `min_gap`, to reduce request count.
    static VectorWithMemoryTracking<ByteRange> mergeRanges(const VectorWithMemoryTracking<ByteRange> & ranges, size_t min_gap);

private:
    // ─── Nested types ────────────────────────────────────────────────────

    /// Per-executor accumulating stats, flushed to ProfileEvents as they
    /// happen and logged at destruction. The foreground passes `this->stats`
    /// into the read path; a worker passes the machine's own `Stats` (merged
    /// at collect/cancel), so a worker never writes a shared counter.
    struct Stats
    {
        /// `add` is the only mutator and the single place a counter maps to
        /// its ProfileEvent, so the two can never drift apart.
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
            /// Source connections dropped before their right bound (not
            /// pool-reusable; the metric's `I`).
            IncompleteConnections,
            /// Source bytes that did not serve the request (alignment slack +
            /// bridged-gap bytes).
            OverReadBytes,
            /// Useful bytes delivered to read requests (cost denominator).
            RequestedBytes,
            CacheGetMicroseconds,
            CachePopulateMicroseconds,
            SourceReadMicroseconds,
            DecryptMicroseconds,
            PrefetchWaitMicroseconds,
            SyncReadMicroseconds,
            WorkMicroseconds,
            PrefetchHits,
            PrefetchCancelled,
            PrefetchPoolFull,
            /// Prefetches NOT submitted (next window fully resident);
            /// report-only, no ProfileEvent.
            PrefetchSkippedResident,
            PrefetchDiscardedRunning,
            PrefetchDiscardWaitMicroseconds,
            PrefetchIssuedSourceBytes,
            PrefetchIssuedCacheBytes,
            PrefetchWastedSourceBytes,
            PrefetchWastedCacheBytes,
            /// A machine wrapped up early at an interrupt point on request.
            MachineInterrupted,
            /// Collects that served a non-empty partial prefix of an
            /// interrupted fetch.
            PartialCollects,
            /// Deferred cache fills: scheduled / rejected by the queue /
            /// abandoned / failed (the step threw - logged, never the
            /// client's error).
            PutScheduled,
            PutPoolFull,
            PutAbandoned,
            PutFailed,
            /// Time a scheduled put step spent queued before running.
            PutWaitMicroseconds,
            /// Deferred promotes skipped (writers on loan / pool full / over
            /// the cap): optional work, regenerable by any later read.
            PromoteSkipped,
            NumCounters
        };

        /// Bump `c` AND emit its ProfileEvent (the one place events are
        /// incremented, so a worker in the submitter's thread group attributes
        /// to the query too).
        void add(Counter c, UInt64 value = 1);

        /// Read a counter for the final report; does not emit.
        UInt64 get(Counter c) const { return values[c]; }

        /// Roll another executor's / worker's stats into this aggregate
        /// WITHOUT re-emitting (each counter already hit ProfileEvents at its
        /// `add`).
        Stats & operator+=(const Stats & o);

    private:
        std::array<UInt64, NumCounters> values{};
    };

    /// RAII timer: on scope exit, add the elapsed microseconds to a `Stats`
    /// timing counter through `Stats::add`.
    class StatTimer
    {
    public:
        StatTimer(Stats & stats_, Stats::Counter counter_);
        ~StatTimer();

        StatTimer(const StatTimer &) = delete;
        StatTimer & operator=(const StatTimer &) = delete;

        UInt64 elapsedMicroseconds() const { return watch.elapsedMicroseconds(); }

    private:
        Stats & target;
        Stats::Counter counter;
        Stopwatch watch;
    };

    /// One (object-piece, tier) entry of the FOREGROUND-PRIVATE half of a
    /// plan: the provider/object identity, the read-only `planResidencyView`
    /// `view` (its hits own the held pinning read buffers; its misses carry
    /// NULL writers and are never dereferenced), and the AUTHORITATIVE
    /// `writers` opened over the aligned miss ranges. 1:1 POSITIONAL with
    /// `ReadPlanGeometry::entries`, provider-grouped fastest-first. A worker
    /// never indexes this, so the buffers are never shared across threads.
    struct BufEntry
    {
        ICacheProvider * provider = nullptr;
        StoredObject object;
        size_t object_file_offset = 0;
        CacheViewPtr view;
        VectorWithMemoryTracking<MissEntry> writers;
    };

    /// One look-ahead plan, the SOURCE OF TRUTH for the current read: the
    /// immutable geometry snapshot plus the held buffers. FOREGROUND-PRIVATE.
    /// Held across many windows; destroyed (write buffers finalize, deferred
    /// LRU bumps run) at the next `planResidencyWindow` / on seek.
    struct ReadPlan
    {
        const std::shared_ptr<const ReadPlanGeometry> & geometry() const { return geometry_snapshot; }

        /// Late-hit `CacheView`s held alive SOLELY for their destructors: the
        /// deferred LRU bump must land AFTER the write buffers in `bufs` are
        /// finalized. Declared BEFORE `bufs` so it is destroyed AFTER it
        /// (members destruct in reverse declaration order).
        VectorWithMemoryTracking<CacheViewPtr> deferred_lru_bumps;

        VectorWithMemoryTracking<BufEntry> bufs;

    private:
        friend class ReaderExecutor;  /// `planResidencyWindow` is the sole writer.
        std::shared_ptr<const ReadPlanGeometry> geometry_snapshot;
    };

    /// One kept-open source connection for a sequential cold scan. Reused
    /// across windows while the next read continues forward within its bound;
    /// a read past it reopens, so the connection is always read to its bound
    /// and returned to the pool drained and reusable. Owns the mechanics of
    /// reading/skipping/draining itself; the executor owns the policy and the
    /// stats. The global-limit lease lives in `ConnState::slot`.
    struct Connection
    {
        /// Read to its right bound - fully consumed and pool-reusable. Always
        /// false for an open-ended connection.
        bool atBound() const { return read_until && current_position >= *read_until; }

        /// Dropping now leaves the connection pool-reusable.
        bool isComplete(bool at_eof) const { return at_eof || atBound(); }

        /// The request was actually issued: at least one byte crossed the
        /// wire. A never-read connection returns to the pool untouched (the
        /// GET is issued lazily), so it must not count as incomplete.
        bool everTransferred() const { return current_position > opened_at; }

        /// Can be continued forward to object-local `next_local` without
        /// reopening: forward, within `min_gap`, not past the bound.
        bool canContinueTo(size_t next_local, size_t min_gap) const
        {
            return next_local >= current_position
                && next_local - current_position <= min_gap
                && (!read_until || next_local <= *read_until);
        }

        /// Read the pre-allocated `blocks` off the open connection into a Rope
        /// using `set()`/`next()`, advancing the frontier. `stop` (nullable)
        /// is polled before each block - a LIVE connection stops freely: it is
        /// saved with its frontier and continues later, nothing is forfeited.
        Rope readInto(VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks,
                      size_t logical_offset, const LoggerPtr & logger,
                      const MachineBase * stop);

        /// Discard up to `gap` bytes so the frontier advances over an
        /// already-cached hole (the bytes are over-read; the request is
        /// saved). Returns bytes skipped (< `gap` only at EOF).
        size_t skipForward(size_t gap, size_t block_bytes);

        /// If only a tail <= `max_tail` remains to the bound, read it out so
        /// the connection completes. Returns bytes drained (over-read,
        /// accounted by the caller).
        size_t drainTail(size_t max_tail, size_t block_bytes);

        size_t current_position = 0;
        /// Where the connection was opened: `current_position > opened_at`
        /// iff it ever transferred.
        size_t opened_at = 0;
        std::optional<size_t> read_until;
        std::unique_ptr<ReadBufferFromFileBase> buffer;
        /// The object this open connection streams - decides whether the next
        /// read can continue it or must reopen.
        String object_path;
    };

    /// The source-connection cluster a machine worker takes ownership of for
    /// its step. Threaded as a `ConnState &` (like `Stats & out_stats`) so a
    /// worker operates on its OWN cluster and the foreground on its own - the
    /// shared-`connection` use-after-free becomes a compile-time
    /// impossibility. Move-only.
    struct ConnState
    {
        /// Special members are out-of-line: the inline move would instantiate
        /// `optional<Connection>` where `Connection`'s buffer type is still
        /// incomplete. The move leaves the source genuinely EMPTY (plain
        /// `std::optional` move leaves it ENGAGED), which is what lets the
        /// foreground reclaim a job's cluster with a plain move.
        ConnState();
        ~ConnState();
        ConnState(const ConnState &) = delete;
        ConnState & operator=(const ConnState &) = delete;
        ConnState(ConnState && other) noexcept;
        ConnState & operator=(ConnState && other) noexcept;

        std::optional<Connection> connection;

        /// While streaming through a DiskCache segment, hold a bare ref to it
        /// so a mid-read eviction can't snap the next miss head back to the
        /// segment start. Re-pointed each window to the segment under the live
        /// frontier; dropped on seek/EOF/reset/plan rebuild.
        CacheWriter::CacheSegmentPin inflight_segment_pin;
    };

    /// The background read-ahead machine (`PrefetchJob` grown into a steppable
    /// context). One step today - a pure source fetch of the pre-bounded
    /// aligned gap window - then the `AwaitCollect` barrier; the foreground
    /// collects or cancels. Bundles EVERYTHING the worker touches, so it never
    /// reads a shared `this->` member.
    struct FetchMachine : MachineBase
    {
        /// Out-of-line: initializes `inflight_gauge` (metric symbol is in the .cpp).
        FetchMachine();

        /// LOGICAL requested read-ahead range (the space `position` works in).
        ByteRange requested_range;
        /// The PHYSICAL cache-aligned window the fetch step reads
        /// (`fetchWindowAt` widened); collect backfills the caches over it.
        ByteRange physical_window;
        std::shared_ptr<const ReadPlanGeometry> geometry;
        /// The advertised read extent at launch: the worker bounds its source
        /// connection with THIS, never the live `read_extent_end` member - a
        /// soft-cancelled machine must not race `setReadExtent`.
        std::optional<size_t> extent_snapshot;
        Stats stats;
        ConnState conn;
        /// Whether the launching plan held the live-connection lease, so the
        /// worker opens kept-live vs one-shot WITHOUT reading the shared lease.
        bool leased = false;
        bool reached_eof = false;
        /// The fetch step's product: raw PHYSICAL source bytes (short at EOF).
        Rope fetched;
        /// The put step's inputs (set at retrigger): writers BORROWED from
        /// `read_plan.bufs` - moved out so the put owns them exclusively, and
        /// RETURNED home by the reap (`writer_origins` records each one's
        /// index). A writer commonly spans many windows, so it must come back
        /// for the next window's fill. `fill_rope` is the assembled window
        /// (refcounted nodes shared with the served slice).
        VectorWithMemoryTracking<MissEntry> writers;
        VectorWithMemoryTracking<size_t> writer_origins;
        Rope fill_rope;
        /// One reschedule is granted to a `ParkedPoolFull` put before it is
        /// abandoned.
        bool put_rescheduled = false;
        /// Which byte counter the put step credits: fills count
        /// `BytesPushedToCacheAsync`, deferred promotes count `BytesPromoted`.
        Stats::Counter put_bytes_counter = Stats::BytesPushedToCacheAsync;
        /// Strategy-A pin taken by the PUT step over the partial segment it
        /// just filled, held until the reap: the foreground finalize runs
        /// BEFORE the deferred fill, so its `writerPinAt` finds a still-empty
        /// segment - without this, an eviction sweep between the fill landing
        /// and the next read would drop it.
        CacheWriter::CacheSegmentPin fill_pin;
        /// Queue-wait probe for the put step (into `PutWaitMicroseconds`).
        Stopwatch put_wait;
        /// `ReaderExecutorPrefetchInFlight` for this machine's lifetime.
        CurrentMetrics::Increment inflight_gauge;
    };

#if USE_SSL
    struct DecryptionLayer
    {
        String path;
        size_t buffer_size;
        KeyFinderFunc key_finder;
        /// Populated by `initDecryption`.
        String key;
    };
#endif

    // ─── Window serve path ───────────────────────────────────────────────

    /// The two tracks of `readNextWindow`, dispatched by `residentAt(cursor)`:
    /// `serveCacheBlock` streams a granular block of the resident run straight
    /// from the plan's held cache readers; `coverWindow` consumes an in-flight
    /// prefetch for the gap or reads it synchronously, fills the caches, and
    /// bounds the read to one plan gap.
    Rope serveCacheBlock(size_t position_phys, size_t to_read);
    Rope coverWindow(size_t position_phys, size_t to_read);

    /// The collect verb. With a machine in flight for this gap: if its step
    /// started/finished, COLLECT it (wait the release edge, reclaim its
    /// connection, backfill, finalize) into `rope` and return true; if still
    /// queued, revoke it and return false so the caller reads synchronously.
    bool tryCollectMachine(Rope & rope);

    /// Read one gap window synchronously from the source (the no-prefetch /
    /// cancelled path).
    Rope syncGapRead(ByteRange physical_window);

    /// Foreground assembler for `physical_window`: resident bytes from the
    /// plan, the rest from the source, then cache backfill + the Strategy-A
    /// pin. A prefetch worker does NOT come through here. Returns one
    /// contiguous run from the window start. `geometry` is the residency
    /// snapshot; `eof_latch` is the size-unknown EOF latch.
    Rope readPhysicalWindow(ByteRange physical_window, ConnState & conn,
        const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats);

    /// `readPhysicalWindow` + remap offsets to logical (subtract the
    /// encryption header). Payload decryption is deferred to the consumer, so
    /// unconsumed read-ahead is never decrypted.
    Rope readWindowLogical(ByteRange physical_window, ConnState & conn,
        const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats);

    /// Append every byte `geometry` reports resident in `physical_window` to
    /// `result` (recording it in `covered`), reading from the plan's held hit
    /// buffers. FOREGROUND-only. Fastest-tier-first is preserved by the
    /// `covered` guard. Also serves a grown committed prefix of a frozen miss
    /// and a self-populated complete page block. Does NOT re-plan.
    void serveResidentFromPlan(
        ByteRange physical_window, Rope & result, IntervalSet & covered,
        const ReadPlanGeometry & geometry, Stats & out_stats);

    /// Serve a clamped resident sub-range from a view's hit buffers, clamping
    /// each read to the buffer's live `readable()` and recording it for the
    /// deferred LRU bump. The caller checks `covers`.
    static Rope readHitFromView(CacheView & view, ByteRange clamped);

    /// A read-only fastest-tier-first sweep over the ranges still uncovered
    /// after the plan's held buffers: a sibling reader / promotion may have
    /// populated a gap since plan-build. Each fresh view moves into
    /// `read_plan.deferred_lru_bumps`; its writers are ignored. Run BEFORE the
    /// source backfill.
    void serveLateHits(ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats);

    // ─── Gap fetch + backfill ────────────────────────────────────────────

    /// Synchronous foreground gap read + backfill in one pass: late hits,
    /// re-credited committed prefixes, source read of the still-missing
    /// ranges, push into the plan's held write buffers. The prefetch path
    /// splits this into the worker's `fetchGapsFromSource` + the consume's
    /// `backfillBytes`. Returns true if any source read happened.
    ///
    /// `fetch_window` is the cache-ALIGNED window to read and cache;
    /// `requested_window` is what the caller asked for. They differ by the
    /// alignment slack, which is counted as over-read.
    bool fetchAndBackfillGaps(
        ByteRange fetch_window,
        ByteRange requested_window,
        Rope & result,
        IntervalSet & covered,
        ConnState & conn,
        bool & eof_latch,
        MemoryPressureLevel pressure_level,
        bool push_to_writers,
        Stats & out_stats);

    /// PURE source fetch: read the WHOLE `physical_window` from the source as
    /// one contiguous physical Rope (short at EOF), no cache/plan/pin. This is
    /// ALL a machine fetch step runs. `stop` (nullable) carries the machine's
    /// cooperative stop flag; the stop policy: a LIVE connection stops at the
    /// next block (saved, continues from its frontier later); a one-shot GET
    /// is never cut mid-response - the stop lands BETWEEN connections. A
    /// stop-short return has the same shape as an EOF-short one and must
    /// neither latch EOF nor throw (the flag is checked FIRST).
    Rope fetchGapsFromSource(ByteRange physical_window, bool from_prefetch, bool keep_live, ConnState & conn,
        bool & eof_latch, MemoryPressureLevel pressure_level, std::optional<size_t> read_extent,
        const MachineBase * stop, Stats & out_stats);

    /// Backfill for `physical_window` from `source_bytes` (already fetched).
    /// FOREGROUND-only: late hits, then append the still-missing ranges from
    /// `source_bytes` into `result`, then - when `push_to_writers` - push the
    /// assembled misses into the held write buffers. A machine collect passes
    /// false and defers that push to the put step.
    void backfillBytes(
        ByteRange physical_window, ByteRange requested_window, const Rope & source_bytes,
        Rope & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats);

    /// Shared assembly tail of both gap paths, run AFTER
    /// `recreditCommittedPrefixes` + `serveLateHits`: append `source_bytes`
    /// for the still-uncovered gaps into `result` (offset order, clamped to
    /// what the source delivered), account OVER-READ (source bytes that did
    /// not serve `requested_window`), and optionally push into the held write
    /// buffers.
    void assembleAndWriteBack(
        ByteRange fetch_window, ByteRange requested_window, const Rope & source_bytes,
        Rope & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats);

    /// Shared tail of an assembled window: re-point the Strategy-A pin to the
    /// partial segment under `pin_frontier` (cleared at EOF / no live
    /// connection), then slice `result` to `slice_window` and enforce the
    /// single-contiguous-run-from-the-window-start guarantee.
    Rope finalizeAssembledWindow(ByteRange slice_window, size_t pin_frontier, Rope & result,
        ConnState & conn, bool eof_latch) const;

    /// Push the assembled `result`'s miss bytes into the plan's held write
    /// buffers, fire-and-forget: a short/zero landing affects only the byte
    /// counter, never `result`. Writes only into the plan's authoritative
    /// `BufEntry::writers`. The SYNCHRONOUS write side; a machine collect
    /// defers the same work to a put step.
    void pushAssembledToWriteBuffers(ByteRange physical_window, const Rope & result, Stats & out_stats);

    /// The per-writer-list body shared by the sync push, the put step and the
    /// deferred promote: write `rope ∩ writer-range ∩ window` into each
    /// writer, counted into `bytes_counter`. `interrupt` (nullable) is polled
    /// between writers - the put step's stop point; remaining writers are left
    /// untouched for the caller's abandon path.
    void pushRopeToWriters(VectorWithMemoryTracking<MissEntry> & writers, ByteRange window,
        const Rope & rope, Stats::Counter bytes_counter, const std::atomic<bool> * interrupt, Stats & out_stats);

    /// Re-credit, BEFORE the source fetch, any committed prefix of a frozen
    /// miss that a writer has grown since plan-build: serve it from the write
    /// buffer's own `read`, marking it `covered` so only the truly-uncommitted
    /// tail drives the fetch.
    void recreditCommittedPrefixes(ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats);

    /// Read from source into the pre-allocated `blocks`. Reuses the open
    /// connection if it continues; otherwise opens kept-live when `keep_live`
    /// (decided by the caller - the foreground passes `bool(connection_lease)`,
    /// the worker its `leased` flag), else a one-shot. `blocks` is consumed;
    /// blocks that receive no data are released. `stop` is polled between
    /// blocks.
    Rope readFromSource(
        const StoredObject & object, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset,
        bool keep_live, ConnState & conn, std::optional<size_t> read_extent,
        const MachineBase * stop, Stats & out_stats);

    /// Allocate OwnedRopeBuffers covering `size` bytes, each <= `block_size`.
    /// `splits` (sorted, relative) forces block boundaries so user-window and
    /// over-read bytes land in separate buffers, releasable independently.
    static VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> allocateBlocks(
        size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits = {});

    // ─── Deferred puts / promotes ────────────────────────────────────────

    /// The retrigger verb: turn a just-collected machine into its PUT step.
    /// BORROW the overlapping writers from `read_plan.bufs` (joining any
    /// earlier put still holding them), hand it the assembled rope, schedule.
    /// Pool full -> parked in `put_machines` (reschedule once, then abandon);
    /// over `MAX_PUT_MACHINES` -> the new fill is skipped (droppable).
    void schedulePutStep(std::shared_ptr<FetchMachine> m, const Rope & assembled);

    /// Return a put machine's borrowed writers home and fold its stats in
    /// (logging a failed step - never the client's error).
    void reapPutMachine(FetchMachine & m);

    /// Reap finished put machines, give each parked one its single
    /// reschedule, abandon beyond that. `wait` joins running ones too (plan
    /// rebuild / destruction).
    void sweepPutMachines(bool wait);

    /// Join (wait + reap) every put machine whose window - or, with
    /// `writers_too`, whose borrowed writer ranges - intersects `window`,
    /// BEFORE the foreground touches those ranges. Window overlap protects a
    /// fetch from re-reading uncommitted bytes; writer overlap matters only
    /// to callers that need the writers home (a machine LAUNCH passes false,
    /// keeping the fetch/fill overlap).
    void joinPutMachinesOverlapping(ByteRange window, bool writers_too);

    /// The deferred promote: borrow the FASTER-tier writers overlapping
    /// `range` that are currently home into a put-only machine fed by the
    /// served rope slice (refcounted, no copy). STRICTLY optional with no
    /// ladder: any contention means the promote is SKIPPED outright
    /// (`PromoteSkipped`) - a later read can regenerate it - so a warm serve
    /// never waits. The pool-less executor keeps the synchronous `maybePromote`.
    void schedulePromoteStep(CacheTier from_tier, ByteRange range, const Rope & bytes, Stats & out_stats);

    /// Promote a range just served from `from_tier` up into every populatable
    /// faster cache (they all miss it, since `from_tier` was the fastest
    /// hit). Walks `read_plan.bufs` in chain order, breaking at the first
    /// entry with `provider->tier() == from_tier` (tier-equality, so a slower
    /// fs hit is never promoted to a faster fs). The committed-set makes
    /// out-of-order promote slices idempotent. `bytes`/`range` are physical,
    /// pre-decryption.
    void maybePromote(CacheTier from_tier, ByteRange range, const Rope & bytes, Stats & out_stats);

    // ─── Plan build ──────────────────────────────────────────────────────

    /// Query cache residency ONCE over the look-ahead span via the read-only
    /// `planResidencyView`, stash the geometry and the held buffers. While the
    /// plan is held, resident bytes stream straight from the held read buffers
    /// - no per-window `getOrSet`. Rebuilt lazily whenever the cursor leaves
    /// the planned span. Resets the in-flight pin before discarding the old
    /// plan.
    void planResidencyWindow(size_t physical_start);

    /// TRIM phase of the plan: the look-ahead span starting at
    /// `physical_start`, clamped to the physical file end and the advertised
    /// read extent. Empty when the start sits at/past a bound. The single
    /// place the plan is bounded.
    ByteRange boundedPlanSpan(size_t physical_start) const;

    /// Translate ONE tier's `planResidencyView` into its 1:1
    /// `GeometryEntry`/`BufEntry`. `extractResidentRuns` records the tier's
    /// hits (clamped to the plan span). `extractMissesAndOpenWriters` records
    /// its cache-aligned misses and opens the write buffers (populatable
    /// tiers only), PRUNING any miss cell fully covered by `upper_hits` (the
    /// union of faster tiers' hits) - that range already lives upstream.
    static void extractResidentRuns(const CacheView & view, ByteRange plan_range, GeometryEntry & geom_entry);
    static void extractMissesAndOpenWriters(
        ICacheProvider & cache, const CacheView & view,
        const StoredObject & object, size_t object_file_offset,
        const IntervalSet & upper_hits, GeometryEntry & geom_entry, BufEntry & buf_entry);

    /// Pin the partial segment under `frontier` from the first held write
    /// buffer whose `range` contains it and whose `pin` is non-null. Empty
    /// when nothing partial is there.
    CacheWriter::CacheSegmentPin writerPinAt(size_t frontier) const;

    // ─── Machine lifecycle ───────────────────────────────────────────────

    void maybeTriggerPrefetch();

    /// The cancel verb: drop the in-flight machine. `cancelled` is true for a
    /// real cancellation (seek / extent change), false for destructor cleanup.
    void cancelMachine(bool cancelled);

    void drainAbandonedMachines(bool wait_finished = false);

    // ─── Connection policy ───────────────────────────────────────────────

    /// Take a `buffer_limit` lease and record the outcome in the
    /// `ReaderExecutorBufferSlot{Acquired,Failed}` counters. Caller must hold
    /// a non-null `buffer_limit`.
    LiveConnectionSlot acquireSlotCounted();

    /// Acquire `connection_lease` before a wide source read (plan span
    /// > `window_size`), unless one is already held or this is a transient.
    void acquireLeaseIfWide();

    /// Return the live connection to the pool the moment it has been read to
    /// its right bound: it is fully drained and reusable.
    void releaseLiveConnectionAtBound(ConnState & conn) const;

    /// Account a connection about to be dropped: count it incomplete unless
    /// drained to its effective end. `at_eof` lets EOF drop sites treat a
    /// reached-EOF connection as complete.
    void accountLiveConnectionDrop(ConnState & conn, bool at_eof, Stats & out_stats) const;

    /// Close a live connection that will not be continued: drain its tail,
    /// account the drop, clear the connection + its segment pin. Does NOT
    /// touch the lease - an abandon-drop releases it, `readFromSource`'s
    /// close-to-reopen keeps it.
    void dropLiveConnection(ConnState & conn, Stats & out_stats) const;

    /// Decide an open live connection's fate before the next read at
    /// `next_physical`: keep it only while that read is a small bridgeable
    /// forward gap within its bound; otherwise drain its tail and drop it.
    /// Called after every cache-only serve. `eof_latch` is passed because
    /// this runs on both paths and a worker must not read the shared member.
    void maybeKeepLiveConnectionBefore(size_t next_physical, ConnState & conn, bool eof_latch, Stats & out_stats) const;

    /// Before dropping the live connection away from its bound, if only a
    /// small tail (<= `max_tail_for_drain`) remains, read it out so the
    /// connection completes. Drained bytes are charged to `out_stats`.
    bool maybeDrainLiveTail(ConnState & conn, Stats & out_stats) const;

    // ─── Sizing / bounds ─────────────────────────────────────────────────

    /// Effective window size for the next read: `effectiveBlockSize` when on
    /// the live path, otherwise `window_size` clamped down by `level` (the
    /// per-plan cached pressure level, not a fresh global query).
    size_t effectiveWindowSize(MemoryPressureLevel level) const;

    /// Effective per-block allocation size: `block_size` at normal memory,
    /// shrinks under pressure.
    size_t effectiveBlockSize(MemoryPressureLevel level) const;

    /// Read-ahead window for the next prefetch: the full window at
    /// Normal/Elevated, 0 (suppressed) at High/Critical - read-ahead is
    /// speculative, so once memory is tight it stops entirely.
    size_t effectivePrefetchWindowSize(MemoryPressureLevel level) const;

    /// Shrink `win_size` so the read does not pass `read_extent_end`.
    /// Saturates to 0 once `position` reaches the extent (recoverable:
    /// extending the extent resumes).
    size_t clampToExtent(size_t win_size) const;

    /// Trim a desired logical read size at `position` to the file end (when
    /// known) and the read extent - the per-read analogue of `boundedPlanSpan`.
    size_t boundedReadSize(size_t want) const;

    /// EOF: size known -> `position >= totalSize()`; size unknown -> the
    /// source's short return latched `reached_eof` (cleared by backward seek).
    bool atEnd() const
    {
        return reached_eof || (!offset_map.hasUnknownSize() && position >= totalSize());
    }

    // ─── Members ─────────────────────────────────────────────────────────

    /// Identity / configuration.
    std::shared_ptr<IFileBasedSourceReader> source;
    StoredObjects stored_objects;  /// retained for makeTransientForReadAt
    OffsetMap offset_map;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    /// Used only for `system.reader_executor_log`; cache identity is derived
    /// per-object by the providers themselves.
    String log_file_path;
    size_t window_size;
    size_t min_bytes_for_seek;
    size_t block_size;
    size_t max_tail_for_drain;
    /// Minimum gap reach (see `streamReach`) that warrants a live connection
    /// + lease. Defaults to `window_size`; `setLiveConnectionMinReadBytes`
    /// overrides.
    size_t live_connection_min_read_bytes;
    /// Look-ahead span for plan-then-stream; raised to at least `window_size`.
    size_t plan_look_ahead_window;

    /// Cursor state.
    size_t position = 0;
    /// Set when the source returned short AND the total size is unknown - the
    /// short return IS the EOF marker.
    bool reached_eof = false;
    /// Logical end of the advertised read region (`makeTransientForReadAt`
    /// one-shot extent, or `setReadExtent`). `nullopt` = read to the file end.
    std::optional<size_t> read_extent_end;

    /// The current look-ahead plan (source of truth; geometry snapshot null
    /// until the first plan is built).
    ReadPlan read_plan;

    /// Machines / pool.
    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    /// The machine driver over `prefetch_pool`: state writes, scheduling and
    /// the revoke/release edges live there; every policy decision stays here.
    /// Created in the constructor from `Options::prefetch_pool`; null without a pool.
    std::unique_ptr<FetchMachineRunner> runner;
    /// Single source of truth for "is a background machine in flight". The
    /// machine is co-owned with the pool job; the worker reads and writes ONLY
    /// the machine payload, and the foreground reclaims it through the
    /// runner's revoke/release edges. The machine-local `Stats` starts at
    /// zero, so its issued counters ARE this read-ahead's issued bytes - a
    /// discard attributes exactly them to wasted.
    std::shared_ptr<FetchMachine> machine;
    /// Cancelled machines whose queued step may still be picked up by the
    /// pool. The destructor waits on each; running calls sweep finished ones.
    VectorWithMemoryTracking<std::shared_ptr<FetchMachine>> abandoned_machines;
    /// Machines running (or parked at) their PUT step - the deferred cache
    /// fill of an already-served window, holding plan writers ON LOAN. Capped
    /// at `MAX_PUT_MACHINES` (beyond it the NEW fill is skipped). Swept by
    /// `sweepPutMachines`; joined before any foreground touch of the same
    /// ranges and unconditionally at plan rebuild / destruction.
    VectorWithMemoryTracking<std::shared_ptr<FetchMachine>> put_machines;
    static constexpr size_t MAX_PUT_MACHINES = 2;

    /// Connection state.
    /// The foreground's connection cluster. EMPTY while a machine is in
    /// flight - moved into `machine->conn` at launch and moved back at
    /// collect/revoke. Named distinctly from the read-path `ConnState & conn`
    /// parameter so the two never shadow.
    ConnState foreground_connection_state;
    std::shared_ptr<LiveConnectionLimit> buffer_limit;
    /// The executor's single live-connection lease. Taken lazily before a
    /// WIDE gap read (`streamReach` beyond `window_size`); released whenever
    /// no live connection remains. A worker is told whether its read is
    /// leased via the machine's `leased` flag, never reading this member.
    LiveConnectionSlot connection_lease;

    /// Logging / transient accounting.
    std::shared_ptr<ReaderExecutorLog> reader_executor_log;
    String creator_query_id;
    /// True on a `makeTransientForReadAt` executor: it does not emit its own
    /// ProfileEvents / log row (the parent reports via `mergeTransientStats`).
    bool is_transient = false;
    /// Serializes `mergeTransientStats`.
    std::mutex transient_stats_mutex;

#if USE_SSL
    VectorWithMemoryTracking<DecryptionLayer> decryption_layers;
    VectorWithMemoryTracking<FileEncryption::Header> decryption_headers;
    bool decryption_initialized = false;
    /// Persistent per-layer CTR encryptors, built lazily by `decryptInPlace`.
    VectorWithMemoryTracking<FileEncryption::Encryptor> payload_encryptors;
#endif
    size_t data_start_offset = 0;  /// N * Header::kSize (0 when no encryption)

    /// `mutable` so `const` read helpers can accumulate timings (stats are
    /// observability, not state). The foreground owns this aggregate; a
    /// worker accumulates into the machine's own `Stats`, merged here at
    /// collect/cancel under the runner's release edge.
    mutable Stats stats;

    CurrentMetrics::Increment active_metric;  /// the ReaderExecutorActive gauge

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
