#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/LongConnectionLimit.h>
#include <IO/ReadPlanGeometry.h>
#include <IO/PlanSchedule.h>
#include <IO/FetchMachine.h>
#include <IO/ContinuityTracker.h>

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
/// sequential scans: reads the next gap ahead on a `PrefetchThreadPool` and
/// shrinks its window/block sizes under memory pressure. Owns its cache and
/// decryption layers, so it is NOT wrapped by the legacy async/decrypt/cache
/// read buffers. Each source read is a one-shot bounded GET (the HTTP pool
/// preserves the socket); no GET stream is kept open across windows.
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
    /// Gap bound for `mergeRanges` / `describePlan`: gaps up to this are
    /// coalesced into one source request rather than read separately. Near the
    /// bandwidth/request cost breakeven.
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 2 * 1024 * 1024; /// 2 MiB
    /// Drain bound for the future long-connection rework; dormant for now.
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
        std::shared_ptr<PrefetchThreadPool> prefetch_pool;
        std::shared_ptr<LongConnectionLimit> long_connection_limit;
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

    /// Out-of-line: runs machine cleanup and the final stats / log flush.
    ~ReaderExecutor();

    // ─── Read path ───────────────────────────────────────────────────────

    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    /// Advertise the read extent (from `ReadBuffer::setReadUntilPosition`,
    /// driven per mark range by `MergeTreeReaderStream::adjustRightMark`). The
    /// executor bounds each one-shot source read and every prefetch within it,
    /// so the borrowed connection is read to a known end and returned to the
    /// pool reusable. `nullopt` clears it. Drains an in-flight prefetch when the
    /// extent changes.
    void setReadExtent(std::optional<size_t> logical_end);

    // ─── Random access (`readBigAt`) ─────────────────────────────────────

    /// A fresh executor for `[start_position, start_position + read_size)`,
    /// sharing immutable state but owning its own position. Gets no
    /// `prefetch_pool`/log (a one-shot read can't amortise prefetch).
    /// `read_size` bounds every read, so the borrowed connection is fully
    /// drained and pool-reusable.
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

    /// Test-only: the continuity estimator's predicted reach after the last plan
    /// feed, for verifying the wiring.
    size_t predictedReachForTest() const { return continuity_tracker.predictedReach(); }

    /// Test-only probes / drivers of the long-connection mechanics (not yet wired
    /// into the production read path).
    bool hasLongConnForTest() const { return long_conn.has_value(); }
    size_t longConnPositionForTest() const { return long_conn ? long_conn->current_position : 0; }
    size_t longConnBoundForTest() const { return long_conn ? long_conn->read_until : 0; }
    bool longConnServesForTest(const String & path) const { return long_conn && long_conn->servesObject(path); }
    bool longConnCanContinueForTest(size_t off, size_t want) const
    {
        return long_conn && long_conn->canContinue(off, want, min_bytes_for_seek);
    }
    bool shouldOpenLongForTest(size_t phys_off) const { return shouldOpenLong(phys_off); }
    size_t clampReachForTest(size_t reach, size_t phys_off) const { return clampReach(reach, phys_off); }
    void openLongForTest(size_t phys_offset, size_t reach);
    Rope serveFromLongForTest(size_t phys_offset, size_t want);
    void dropLongForTest() { dropLong(long_conn, stats); }
    UInt64 incompleteConnectionsForTest() const { return stats.get(Stats::IncompleteConnections); }
    UInt64 sourceRequestsForTest() const { return stats.get(Stats::SourceRequests); }
    bool machineHasLongConnForTest() const { return machine && machine->long_conn.has_value(); }

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

    /// One held source connection for a sequential run: a bounded GET opened ONCE
    /// (only by the foreground - a machine never opens one, it can only RECEIVE one
    /// at launch) and drained incrementally across windows while reads continue
    /// forward within its bound. A small forward gap is bridged by discarding it on
    /// the open stream (`skipForward`); a read it cannot continue reopens. Move-only
    /// so it can ride the `FetchMachine` payload as a SINGLE owner (foreground member
    /// <-> machine payload, never shared across threads). Offsets are OBJECT-LOCAL (a
    /// GET streams one object); `read_until` is the read-extent bound, set ONCE.
    struct LongConnection
    {
        std::unique_ptr<ReadBufferFromFileBase> buffer;
        String object_path;
        size_t opened_at = 0;
        size_t current_position = 0;
        size_t read_until = 0;
        LongConnectionSlot slot;

        /// Read to its bound - fully consumed and pool-reusable.
        bool atBound() const { return current_position >= read_until; }
        /// Dropping now leaves it pool-reusable.
        bool isComplete(bool at_eof) const { return at_eof || atBound(); }
        /// At least one byte crossed the wire. The GET is issued lazily, so a
        /// never-read connection returns to the pool untouched and must not count
        /// as incomplete.
        bool everTransferred() const { return current_position > opened_at; }
        bool servesObject(const String & path) const { return object_path == path; }
        /// Forward, within `near_gap`, and `[off, off+want)` stays inside the bound.
        bool canContinue(size_t off, size_t want, size_t near_gap) const
        {
            return off >= current_position
                && off - current_position <= near_gap
                && off + want <= read_until;
        }

        /// Read the pre-allocated `blocks` off the open stream into a Rope,
        /// advancing the frontier; `stop` (nullable) is polled between blocks.
        Rope readInto(VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks,
            size_t logical_offset, const MachineBase * stop);
        /// Discard up to `gap` bytes so the frontier advances over an already-cached
        /// hole (over-read; the request is saved). Returns bytes skipped (< `gap`
        /// only at EOF).
        size_t skipForward(size_t gap, size_t block_bytes);
        /// If only a tail <= `max_tail` remains to the bound, read it out so the
        /// connection completes. Returns bytes drained.
        size_t drainTail(size_t max_tail, size_t block_bytes);
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

        /// The explicit work of this plan (`describePlan`), computed once at
        /// build. Its `retrieves[*].into` are the authoritative fill targets:
        /// the deferred put borrows exactly the writers a retrieve designates,
        /// so slack is filled only into its owning lower tier and never
        /// promoted into a faster tier.
        PlanSchedule schedule;

    private:
        friend class ReaderExecutor;  /// `planResidencyWindow` is the sole writer.
        std::shared_ptr<const ReadPlanGeometry> geometry_snapshot;
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
        /// The long source connection CARRIED by this machine: moved in from the
        /// foreground at launch (a machine never opens one itself - the foreground is
        /// the sole opener), drained by the worker's fetch step instead of a one-shot
        /// GET, reclaimed by the foreground at collect, or accounted + released at reap
        /// if the machine is abandoned. Empty until the open path is wired in.
        std::optional<LongConnection> long_conn;
        Stats stats;
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
    Rope readPhysicalWindow(ByteRange physical_window,
        const ReadPlanGeometry & geometry, bool & eof_latch, Stats & out_stats);

    /// `readPhysicalWindow` + remap offsets to logical (subtract the
    /// encryption header). Payload decryption is deferred to the consumer, so
    /// unconsumed read-ahead is never decrypted.
    Rope readWindowLogical(ByteRange physical_window,
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
        bool & eof_latch,
        MemoryPressureLevel pressure_level,
        bool push_to_writers,
        Stats & out_stats);

    /// PURE source fetch: read the WHOLE `physical_window` from the source as
    /// one contiguous physical Rope (short at EOF), no cache/plan/pin. This is
    /// ALL a machine fetch step runs. `stop` (nullable) carries the machine's
    /// cooperative stop flag, polled BETWEEN connections only - a one-shot GET
    /// is never cut mid-response. A stop-short return has the same shape as an
    /// EOF-short one and must neither latch EOF nor throw (the flag is checked FIRST).
    /// `lc` (nullable) is the long connection to DRAIN if it can serve a piece - the
    /// worker passes its machine's payload, never the foreground's.
    Rope fetchGapsFromSource(ByteRange physical_window, bool from_prefetch,
        bool & eof_latch, MemoryPressureLevel pressure_level, std::optional<size_t> read_extent,
        std::optional<LongConnection> * lc, const MachineBase * stop, Stats & out_stats);

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

    /// Shared tail of an assembled window: re-point the Strategy-A pin
    /// (`inflight_segment_pin`) to the partial segment under `pin_frontier`
    /// (cleared at EOF), then slice `result` to `slice_window` and enforce the
    /// single-contiguous-run-from-the-window-start guarantee.
    Rope finalizeAssembledWindow(ByteRange slice_window, size_t pin_frontier, Rope & result, bool eof_latch);

    /// Push the assembled `result`'s miss bytes into the plan's held write
    /// buffers, fire-and-forget: a short/zero landing affects only the byte
    /// counter, never `result`. Writes only the writers the plan SCHEDULE
    /// designates as fill targets for this window, so a faster tier never
    /// receives slack bytes (`isScheduledFillTarget`). The SYNCHRONOUS write
    /// side; a machine collect defers the same work to a put step.
    void pushAssembledToWriteBuffers(ByteRange physical_window, const Rope & result, Stats & out_stats);

    /// The per-writer-list body shared by the put step and the parked-inline
    /// write: write `rope ∩ writer-range ∩ window` into each (already
    /// schedule-filtered) writer, counted into `bytes_counter`. `interrupt`
    /// (nullable) is polled between writers - the put step's stop point;
    /// remaining writers are left untouched for the caller's abandon path.
    void pushRopeToWriters(VectorWithMemoryTracking<MissEntry> & writers, ByteRange window,
        const Rope & rope, Stats::Counter bytes_counter, const std::atomic<bool> * interrupt, Stats & out_stats);

    /// Write `rope ∩ writer-range ∩ window` into ONE writer (the body of the
    /// loops above), counted into `bytes_counter`.
    void writeSliceToWriter(MissEntry & w, ByteRange window, const Rope & rope,
        Stats::Counter bytes_counter, Stats & out_stats);

    /// Whether the plan schedule designates `(entry, cell)` a fill target for a
    /// retrieve overlapping `window`. A cell holding the request is a target in
    /// every missing tier (promotion); a slack-only cell is a target only in
    /// its owning lower tier - never promoted into a faster tier.
    bool isScheduledFillTarget(ByteRange window, size_t entry, ByteRange cell) const;

    /// Re-credit, BEFORE the source fetch, any committed prefix of a frozen
    /// miss that a writer has grown since plan-build: serve it from the write
    /// buffer's own `read`, marking it `covered` so only the truly-uncommitted
    /// tail drives the fetch.
    void recreditCommittedPrefixes(ByteRange window, Rope & result, IntervalSet & covered, Stats & out_stats);

    /// Read from source into the pre-allocated `blocks`: DRAIN a held/carried long
    /// connection (`lc`, nullable) if it can serve this fetch, otherwise open a
    /// one-shot bounded connection, read the blocks, and let it close on return (the
    /// HTTP pool still preserves the socket). `blocks` is consumed; blocks that receive
    /// no data are released. `stop` (nullable) is the drain's interrupt point.
    Rope readFromSource(
        const StoredObject & object, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset,
        std::optional<size_t> read_extent, std::optional<LongConnection> * lc,
        const MachineBase * stop, Stats & out_stats);

    /// Allocate OwnedRopeBuffers covering `size` bytes, each <= `block_size`.
    /// `splits` (sorted, relative) forces block boundaries so user-window and
    /// over-read bytes land in separate buffers, releasable independently.
    static VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> allocateBlocks(
        size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits = {});

    // ─── Long connection ─────────────────────────────────────────────────

    /// Clamp the estimator's (unclamped) reach to a concrete physical end: the file
    /// end when the size is known (an unknown-size object has no end to clamp).
    size_t clampReach(size_t reach, size_t phys_off) const;

    /// Whether to open a long connection at physical `phys_off`: the estimator's
    /// predicted contiguous reach exceeds the current read window, a connection slot is
    /// configured (`reader_executor_use_long_connections`), and pressure is not
    /// High/Critical (the open is speculative, like prefetch).
    bool shouldOpenLong(size_t phys_off) const;

    /// The long-connection bound (object-local) for an open at physical `phys_offset`:
    /// the read extent clamped to the object end. See the definition for why the bound
    /// is the extent, not the predicted reach.
    size_t longConnectionBound(const StoredObject & object, size_t object_offset, size_t phys_offset) const;

    /// Foreground open hook: when `shouldOpenLong(phys_offset)` and a slot can be
    /// acquired, open a long connection over `object` (object-local `object_offset`),
    /// bounded at `longConnectionBound`, so the following source read - and the windows
    /// after it - drain it. A no-op when already held / not warranted / at capacity
    /// (then the read falls back to a one-shot).
    void openLongIfWarranted(const StoredObject & object, size_t object_offset,
        size_t phys_offset, size_t want, Stats & out_stats);

    /// Open a bounded GET over `object` at object-local `offset`, bounded at
    /// object-local `read_until`, taking the already-acquired `slot`; store it in
    /// `conn`. The ONLY place a long connection is opened, and only on the foreground
    /// - a machine never calls this.
    void openLong(std::optional<LongConnection> & conn, const StoredObject & object,
        size_t offset, size_t read_until, LongConnectionSlot slot, Stats & out_stats) const;

    /// Serve a read at object-local `offset` from `conn` (caller has checked
    /// `servesObject` + `canContinue`): bridge a forward gap by discarding it
    /// (over-read), `readInto` the blocks, then release the connection at its bound.
    Rope serveFromLong(std::optional<LongConnection> & conn, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks,
        size_t logical_offset, const MachineBase * stop, Stats & out_stats) const;

    /// If only a tail <= `max_tail_for_drain` remains to the bound, read it out so
    /// the connection completes and returns to the pool reusable. Returns true iff it
    /// drained but did not reach the bound (EOF inside the tail).
    bool maybeDrainLongTail(std::optional<LongConnection> & conn, Stats & out_stats) const;

    /// Close `conn`: drain a small tail, account a still-incomplete drop, reset.
    void dropLong(std::optional<LongConnection> & conn, Stats & out_stats) const;

    /// Account an incomplete-connection drop (`everTransferred` and not
    /// `isComplete`) for `conn`.
    void accountLongDrop(const std::optional<LongConnection> & conn, bool at_eof, Stats & out_stats) const;

    /// Reset `conn` if it reached its bound (a clean pool return).
    void releaseLongAtBound(std::optional<LongConnection> & conn) const;

    /// Move a long connection out of `src`, leaving `src` EMPTY. A plain `std::optional`
    /// move leaves the source ENGAGED (with a moved-from value), so every hand-off goes
    /// through this to keep the connection a single owner (and to stop a moved-from
    /// husk from being seen as a held connection or counted as an incomplete drop).
    static std::optional<LongConnection> takeLong(std::optional<LongConnection> & src);

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

    /// Feed the plan SCHEDULE's predicted source reads (the `Source::Remote`
    /// retrieves, in offset order, only past `continuity_fed_end`) into
    /// `continuity_tracker`, then advance the watermark. A Remote retrieve's range
    /// already spans bridged holes (<= `min_bytes_for_seek`) as over-read, and
    /// `near_gap == min_bytes_for_seek`, so feeding the range as one read counts
    /// that over-read exactly as a read-through would.
    void feedScheduleToContinuity(const PlanSchedule & schedule);

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

    // ─── Sizing / bounds ─────────────────────────────────────────────────

    /// Effective window size for the next read: `window_size` clamped down by
    /// `level` (the per-plan cached pressure level, not a fresh global query).
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

    /// Connection state. The Strategy-A pin holding the partial cache segment
    /// under the live fill frontier non-evictable across windows: re-pointed at
    /// each `finalizeAssembledWindow`, dropped on seek / EOF / plan rebuild.
    /// Foreground-only (a prefetch worker does a pure source fetch and never
    /// touches it). NOT long-connection state - a one-shot fill needs it too.
    CacheWriter::CacheSegmentPin inflight_segment_pin;
    /// Server-wide long-connection limit handle, shared with
    /// `makeTransientForReadAt`. Gates long-connection opens; dormant for now.
    std::shared_ptr<LongConnectionLimit> long_connection_limit;
    /// The held long source connection. Foreground hold (it rides the machine
    /// payload when a prefetch carries it - a later stage). Empty until the open
    /// path is wired in; Stage 1 introduces the type and mechanics only.
    std::optional<LongConnection> long_conn;

    /// Continuous-read pattern estimator, fed each plan's predicted source reads
    /// and every seek. Constructed with `near_gap == min_bytes_for_seek` so a
    /// bridged gap counts identically whether modeled as a read-through or a seek.
    /// Its prediction is snapshotted into `schedule.predicted_reach`; nothing acts
    /// on it yet.
    ContinuityTracker continuity_tracker;
    /// Highest physical offset already fed to `continuity_tracker` from a plan, so
    /// overlapping re-plans never double-feed. Reset to the target on seek.
    size_t continuity_fed_end = 0;

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
