#pragma once

#include <IO/ChainedBuffers.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/LongConnectionLimit.h>
#include <IO/CoverageMap.h>
#include <IO/PlanSchedule.h>
#include <IO/FetchMachine.h>
#include <IO/ContinuityTracker.h>

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/DequeWithMemoryTracking.h>
#include <base/types.h>
#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "config.h"
#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#include <IO/ReaderExecutorDecryptor.h>
#endif

namespace DB
{

class PrefetchThreadPool;
class ReaderExecutorLog;
class IFetchMachineRunner;
class ReaderExecutorInspector;

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
    /// Gap bound for `mergeRanges` / `buildSchedule`: a gap strictly smaller than
    /// this is coalesced (over-read) into one source request rather than read
    /// separately; a gap at or above it reopens, and if a faster tier holds it the
    /// bytes are filled down from there. Near the bandwidth/request cost breakeven.
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 2 * 1024 * 1024; /// 2 MiB
    /// Drain bound: if only a tail of at most this many bytes remains to a long
    /// connection's read bound, drain it so the connection completes pool-reusable
    /// (see `dropLong`) instead of being abandoned mid-response.
    static constexpr size_t DEFAULT_MAX_TAIL_FOR_DRAIN = 1 * 1024 * 1024; /// 1 MiB
    static constexpr size_t CHAINED_BUFFER_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB per ChainedBuffers node
    /// The fixed plan window: residency is planned ONCE over this span (plan-then-stream),
    /// amortising cache discovery across many serve windows; segment folding extends a plan
    /// out to the touched cell boundaries within it. Defaults to one `window_size` (the A/B
    /// showed a small plan is cost-equivalent to a large one - the long connection, not the
    /// window, carries the cost); raise it to plan further ahead. `read_extent_end` does not
    /// size the plan, so the plan survives mark-range advances and is reused.
    static constexpr size_t DEFAULT_PLAN_LOOK_AHEAD_MAX_WINDOW = DEFAULT_WINDOW_SIZE;
    /// A warranted long connection opens with at least this much range and never streams
    /// past the cap. The continuous-read prediction may under-predict at the start of a run
    /// and over-predict at its end; these bound the resulting GET so an over-prediction
    /// cannot run away into an unbounded over-read.
    static constexpr size_t DEFAULT_LONG_CONNECTION_OPEN_RANGE = 16 * 1024 * 1024; /// 16 MiB
    static constexpr size_t DEFAULT_LONG_CONNECTION_MAX_BOUND = 128 * 1024 * 1024; /// 128 MiB
    /// How far the in-order fill front runs AHEAD of the serve cursor (the cache-as-buffer
    /// lead): the single in-flight machine fetches up to this much into a disk (`FilesystemCache`)
    /// bottom tier, committing cells progressively, while the serve reads the committed prefix.
    /// Held cheaply on disk, so it is flat. Only a disk bottom tier exposes the partial-prefix
    /// read + frontier wait the run-ahead needs; a page-cache-only or bypass read stays one
    /// window. Distinct from the plan window (the geometry/pin horizon).
    static constexpr size_t DEFAULT_FILL_AHEAD_LEAD = 16 * 1024 * 1024;     /// 16 MiB (disk-backed bottom, flat)

    /// Everything configurable beyond the data path itself: the executor is
    /// fully wired at construction, there are no post-construction setters.
    struct Options
    {
        size_t window_size = DEFAULT_WINDOW_SIZE;
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK;
        size_t block_size = CHAINED_BUFFER_BLOCK_SIZE;
        String log_file_path;
        size_t max_tail_for_drain = DEFAULT_MAX_TAIL_FOR_DRAIN;
        /// Single fixed size for the plan window (see `DEFAULT_PLAN_LOOK_AHEAD_MAX_WINDOW`).
        /// The plan extends the request rightward to fold ALL affected cache segments
        /// (hits as well as misses) on every tier into the geometry, pins them, and reuses
        /// the plan across read-extent advances while the cursor stays inside the pinned span.
        size_t plan_look_ahead_max_window = DEFAULT_PLAN_LOOK_AHEAD_MAX_WINDOW;
        /// Long-connection sizing bounds (see `DEFAULT_LONG_CONNECTION_OPEN_RANGE` /
        /// `DEFAULT_LONG_CONNECTION_MAX_BOUND`).
        size_t long_connection_open_range = DEFAULT_LONG_CONNECTION_OPEN_RANGE;
        size_t long_connection_max_bound = DEFAULT_LONG_CONNECTION_MAX_BOUND;
        /// Fill-ahead lead for a disk bottom tier (see `DEFAULT_FILL_AHEAD_LEAD`).
        size_t fill_ahead_lead = DEFAULT_FILL_AHEAD_LEAD;
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

    /// Returns an empty ChainedBuffers at EOF.
    ChainedBuffers readNextWindow();

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

    /// All test-only observability (state probes + long-connection drivers) lives
    /// in the `ReaderExecutorInspector` friend, kept out of this production class.
    /// See `src/IO/tests/ReaderExecutorInspector.h`.
    friend class ReaderExecutorInspector;

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
            PrefetchDiscardedRunning,
            PrefetchIssuedSourceBytes,
            PrefetchWastedSourceBytes,
            /// A machine wrapped up early at an interrupt point on request.
            MachineInterrupted,
            /// Collects that served a non-empty partial prefix of an
            /// interrupted fetch.
            PartialCollects,
            /// A deferred cache fill whose put step threw - logged, never the
            /// client's error.
            PutFailed,
            /// Long source connections: opened, windows served from an open one,
            /// fallbacks to a one-shot when no slot was free, bytes served through them.
            LongConnectionOpened,
            LongConnectionHits,
            LongConnectionFallbacks,
            LongConnectionBytes,
            /// Number of `observeAndSchedule` calls = residency-plan (re)builds. The
            /// plan is reused across mark-range advances; it rebuilds only on a
            /// want_replan (the cursor leaves `plan_start..plan_end`). This sizes how
            /// short-lived the held cache readers are.
            Observations,
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
    /// `CoverageMap::entries`, provider-grouped fastest-first. A worker
    /// never indexes this, so the buffers are never shared across threads.
    struct BufEntry
    {
        ICacheProvider * provider = nullptr;
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
    /// GET streams one object); `read_until` is the predicted-reach bound (>= the read
    /// extent), set ONCE.
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
        /// At least one byte crossed the wire. The GET is issued lazily, so a
        /// never-read connection returns to the pool untouched and must not count
        /// as incomplete.
        bool everTransferred() const { return current_position > opened_at; }
        bool servesObject(const String & path) const { return object_path == path; }
        /// Forward and `[off, off+want)` stays inside the bound. A contiguous read
        /// (`off == current_position`) always continues - it is not a bridge. A
        /// forward HOLE is bridged (over-read on the open GET) only if STRICTLY smaller
        /// than `near_gap`; a hole of exactly `near_gap` reopens instead - over-reading
        /// it costs about as much, and a faster tier holding it fills it down.
        bool canContinue(size_t off, size_t want, size_t near_gap) const
        {
            return canStartServing(off, near_gap) && off + want <= read_until;
        }

        /// Whether the channel can START serving at `off` - forward and inside the bound -
        /// even if the read would cross `read_until`. `readFromSource` serves the prefix up to
        /// the bound (the channel then drains clean) and reopens for the remainder, so a held
        /// channel that `canStartServing` must NOT be dropped as un-continuable.
        bool canStartServing(size_t off, size_t near_gap) const
        {
            return off >= current_position
                && (off == current_position || off - current_position < near_gap)
                && off < read_until;
        }

        /// Read the pre-allocated `blocks` off the open stream into a ChainedBuffers,
        /// advancing the frontier; `stop` (nullable) is polled between blocks.
        ChainedBuffers readInto(VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks,
            size_t logical_offset, const MachineBase * stop);
        /// Discard up to `gap` bytes so the frontier advances over an already-cached
        /// hole (over-read; the request is saved). Returns bytes skipped (< `gap`
        /// only at EOF).
        size_t skipForward(size_t gap, size_t block_bytes);
        /// If only a tail <= `max_tail` remains to the bound, read it out so the
        /// connection completes. Returns bytes drained.
        size_t drainTail(size_t max_tail, size_t block_bytes);
    };

    /// Per-retrieve runtime status for the schedule-driven interpreter: the ENTIRE
    /// exec-time mutable surface the loop will branch on - no residency state, no
    /// per-window geometry. `NotLaunched` -> `InFlight` (machine scheduled or a sync
    /// read entered) -> `Ready` (bytes fetched, serve may proceed) -> `Done` (also
    /// written into every `into[]` cell, write handles released). Serve gates on
    /// `Ready`, never `Done`, so a slow cache write never stalls the user.
    enum class RetrievePhase
    {
        NotLaunched,
        InFlight,
        Ready,
        Done,
    };

    /// 1:1 with `PlanSchedule::retrieves`; lives on `ReadPlan`, dies with the plan.
    struct RetrieveStatus
    {
        RetrievePhase phase = RetrievePhase::NotLaunched;
        size_t fetched = 0;               /// bytes confirmed fetched (the `Ready` frontier)
        ChainedBuffers ready_bytes;                 /// banked fetched prefix for serve, drained as the cursor advances
    };

    /// One look-ahead plan, the SOURCE OF TRUTH for the current read: the
    /// immutable geometry snapshot plus the held buffers. FOREGROUND-PRIVATE.
    /// Held across many windows; destroyed (write buffers finalize, deferred
    /// LRU bumps run) at the next `observeAndSchedule` / on seek.
    struct ReadPlan
    {
        const std::shared_ptr<const CoverageMap> & geometry() const { return geometry_snapshot; }

        /// Late-hit `CacheView`s held alive SOLELY for their destructors: the
        /// deferred LRU bump must land AFTER the write buffers in `bufs` are
        /// finalized. Declared BEFORE `bufs` so it is destroyed AFTER it
        /// (members destruct in reverse declaration order).
        VectorWithMemoryTracking<CacheViewPtr> deferred_lru_bumps;

        VectorWithMemoryTracking<BufEntry> bufs;

        /// The explicit work of this plan (`buildSchedule`), computed once at
        /// build. Its `retrieves[*].into` are the authoritative fill targets:
        /// the deferred put borrows exactly the writers a retrieve designates,
        /// so slack is filled only into its owning lower tier and never
        /// promoted into a faster tier.
        PlanSchedule schedule;

        /// Per-retrieve runtime status, 1:1 with `schedule.retrieves`, allocated at
        /// plan build and reset on re-plan/seek. The schedule-driven interpreter maintains
        /// it (phase transitions) and serves from it (`depsSatisfied`, serve dispatch).
        VectorWithMemoryTracking<RetrieveStatus> retrieve_status;

        /// The step interpreter's loop authority - an index into `schedule.steps`,
        /// reconstructed from `position` on re-plan/seek.
        size_t cursor = 0;

        /// The launch interpreter's authority - the first `schedule.retrieves` index not
        /// yet launched/exhausted; advanced by `maybeLaunchAhead`. Reset on re-plan.
        size_t launch_frontier = 0;

        /// True iff `schedule.retrieves` contains a `Source::Remote` job. When false
        /// the plan is served entirely from cache tiers, so there is no source
        /// connection to open and `maybeLaunchAhead` skips its prefetch bookkeeping
        /// (after its look-ahead re-plan, which still discovers cold beyond the plan).
        bool has_remote_retrieves = false;

    private:
        friend class ReaderExecutor;  /// `observeAndSchedule` is the sole writer.
        std::shared_ptr<const CoverageMap> geometry_snapshot;
    };

    /// A NON-OWNING reference to one held write buffer in `read_plan.bufs`
    /// (`writer` is owned there by a `unique_ptr`). A put step records these
    /// instead of moving the writers out, so the shared `bufs` are written in
    /// place. The raw pointer stays valid while `read_plan` is not rebuilt - and
    /// every rebuild path drains the put lane first - so a put never outlives its
    /// views. `range` is the cell's miss range (the lane-overlap key).
    struct WriterView
    {
        CacheWriter * writer = nullptr;
        ByteRange range;
    };

    /// The background read-ahead machine: a steppable context. One step today - a
    /// pure source fetch of the pre-bounded aligned gap window - then the
    /// `AwaitCollect` barrier; the foreground collects or cancels. Bundles
    /// EVERYTHING the worker touches, so it never reads a shared `this->` member.
    struct FetchMachine : MachineBase
    {
        /// Out-of-line: initializes `inflight_gauge` (metric symbol is in the .cpp).
        FetchMachine();

        /// The PHYSICAL cache-aligned window the fetch step reads
        /// (`fetchWindowAt` widened); collect backfills the caches over it. The
        /// LOGICAL requested range (the space `position` works in) is this shifted
        /// down by `data_start_offset`.
        ByteRange physical_window;
        /// The plan's memory-pressure level, snapshotted at launch - the only
        /// geometry field the worker reads (sizes the fetch block / suppresses
        /// read-ahead). A future stage needing other geometry fields on the
        /// worker must re-add a snapshot rather than reach into shared state.
        MemoryPressureLevel pressure_snapshot{};
        /// The advertised read extent at launch: the worker bounds its source
        /// connection with THIS, never the live `read_extent_end` member - a
        /// soft-cancelled machine must not race `setReadExtent`.
        std::optional<size_t> extent_snapshot;
        /// The schedule retrieve this machine fulfills (index into the launch-time plan's
        /// `schedule.retrieves` / `retrieve_status`). Set at launch; read live by `machineFor`
        /// (is a machine running for this retrieve) and `reapPutMachine` (marks the retrieve
        /// Done). Meaningful only while this machine is the live in-flight handle of that
        /// plan; the re-plan barrier (`chassert(!machine)`) guarantees none straddles a rebuild.
        size_t retrieve_index = 0;
        /// The long source connection CARRIED by this machine: moved in from the
        /// foreground at launch (a machine never opens one itself - the foreground is
        /// the sole opener), drained by the worker's fetch step instead of a one-shot
        /// GET, reclaimed by the foreground at collect, or accounted + released at reap
        /// if the machine is abandoned. Empty when the foreground carried no connection
        /// into this launch.
        std::optional<LongConnection> long_conn;
        Stats stats;
        bool reached_eof = false;
        /// The fetch step's product: raw PHYSICAL source bytes (short at EOF).
        ChainedBuffers fetched;
        /// The fill step's inputs: NON-OWNING views of the writers this fill targets
        /// (the schedule's fill targets overlapping the window). The writers stay in the
        /// shared `read_plan.bufs`; the fill runs inline on the read thread, so referencing
        /// them in place is race-free. `fill_chain` is the assembled window (refcounted
        /// nodes shared with the served slice).
        VectorWithMemoryTracking<WriterView> writer_views;
        ChainedBuffers fill_chain;
        /// Set by the worker when a SIBLING is downloading some segment (this worker lost the
        /// election): it skipped fetching those, so `fetched` has holes there. At collect the
        /// foreground revokes to the synchronous path (which re-elects/waits on the sibling-led
        /// bytes on the query thread). False with no contention (the worker then leads - and
        /// fetches - the whole window).
        bool contended = false;
        /// Set when this machine is driven INLINE on the serve thread (a `LocalFetchMachineRunner`),
        /// as opposed to a pool worker reading ahead. The inline fetch "stops at the first loss":
        /// it fetches only the contiguous led PREFIX up to the first sibling-led segment, so the
        /// serve thread never blocks fetching a led run PAST a sibling-led hole (the caller's next
        /// read resolves the boundary). A pool worker keeps the full-window fetch.
        bool inline_serve = false;
        /// Strategy-A pin taken by the fill step over the partial segment it just
        /// filled, held until the reap: the foreground finalize runs BEFORE the fill,
        /// so its `writerPinAt` finds a still-empty segment - without this, an eviction
        /// sweep between the fill landing and the next read would drop it.
        CacheWriter::CacheSegmentPin fill_pin;
        /// `ReaderExecutorPrefetchInFlight` for this machine's lifetime.
        CurrentMetrics::Increment inflight_gauge;
    };

    // ─── Window serve path ───────────────────────────────────────────────

    /// Whether served payload is encrypted (`data_start_offset` is the header
    /// size, 0 when there is no encryption / no SSL).
    bool needsDecryption() const { return data_start_offset > 0; }

    /// Return a plaintext copy of `cipher` (or `cipher` unchanged when there is
    /// nothing to decrypt). Each node is copied into a fresh `OwnedChainedBuffer`
    /// and decrypted at its `logical_offset` - never in place, since the served
    /// nodes alias live page-cache / cache cells. CTR is position-addressable, so
    /// per-node decryption at each node's logical offset is exact.
    ChainedBuffers decryptWindow(ChainedBuffers && cipher);

    /// Decrypt `data` in place at `logical_offset` via the reentrant `decryptor`.
    /// Safe to call from a worker concurrently with the foreground.
    void decryptInPlace(char * data, size_t size, size_t logical_offset);

    /// The collect verb. With a machine in flight for this gap: if its step
    /// started/finished, COLLECT it (wait the release edge, reclaim its
    /// connection, backfill, finalize) into `chain` and return true; if still
    /// queued, revoke it and return false so the caller reads synchronously.
    bool tryCollectMachine(ChainedBuffers & chain);

    /// Serve a clamped resident sub-range from a view's hit buffers, clamping
    /// each read to the buffer's live `readable()` and recording it for the
    /// deferred LRU bump. The caller checks `covers`.
    static ChainedBuffers readHitFromView(CacheView & view, ByteRange clamped);

    /// A read-only fastest-tier-first sweep over the ranges still uncovered
    /// after the plan's held buffers: a sibling reader / promotion may have
    /// populated a gap since plan-build. Each fresh view moves into
    /// `read_plan.deferred_lru_bumps`; its writers are ignored. Run BEFORE the
    /// source backfill.
    void serveLateHits(ByteRange window, ChainedBuffers & result, IntervalSet & covered, Stats & out_stats);

    // ─── Gap fetch + backfill ────────────────────────────────────────────

    /// PURE source fetch: read the WHOLE `physical_window` from the source as
    /// one contiguous physical ChainedBuffers (short at EOF), no cache/plan/pin. This is
    /// ALL a machine fetch step runs. `stop` (nullable) carries the machine's
    /// cooperative stop flag, polled BETWEEN connections only - a one-shot GET
    /// is never cut mid-response. A stop-short return has the same shape as an
    /// EOF-short one and must neither latch EOF nor throw (the flag is checked FIRST).
    /// `lc` (nullable) is the long connection to DRAIN if it can serve a piece - the
    /// worker passes its machine's payload, never the foreground's.
    ChainedBuffers fetchGapsFromSource(ByteRange physical_window, bool from_prefetch,
        bool & eof_latch, MemoryPressureLevel pressure_level, std::optional<size_t> read_extent,
        std::optional<LongConnection> * lc, const MachineBase * stop, Stats & out_stats);

    /// The machine fetch step (runs on the worker thread): elect the FileCache downloader
    /// over the window's fill-target `writer_views`, fetch the LED runs from the source via
    /// the machine's own connection, and write+complete them INLINE on this thread (the
    /// downloader contract). Segments a sibling leads are SKIPPED and flagged via
    /// `m.contended` so the foreground revokes to the sync path at collect. Sets `m.fetched`
    /// to the led bytes (sparse - holes where a sibling leads). With no contention the worker
    /// leads the whole window, so this fetches it all, exactly like `fetchGapsFromSource`.
    void coordinatedPrefetch(FetchMachine & m);

    /// Shared assembly tail of both gap paths, run AFTER
    /// `recreditCommittedPrefixes` + `serveLateHits`: append `source_bytes`
    /// for the still-uncovered gaps into `result` (offset order, clamped to
    /// what the source delivered), account OVER-READ (source bytes that did
    /// not serve `requested_window`), and optionally push into the held write
    /// buffers.
    void assembleAndWriteBack(
        ByteRange fetch_window, ByteRange requested_window, const ChainedBuffers & source_bytes,
        ChainedBuffers & result, IntervalSet & covered, bool push_to_writers, Stats & out_stats);

    /// Shared tail of an assembled window: re-point the Strategy-A pin
    /// (`inflight_segment_pin`) to the partial segment under `pin_frontier`
    /// (cleared at EOF), then slice `result` to `slice_window` and enforce the
    /// single-contiguous-run-from-the-window-start guarantee.
    ChainedBuffers finalizeAssembledWindow(ByteRange slice_window, size_t pin_frontier, ChainedBuffers & result, bool eof_latch);

    /// Push the assembled `result`'s miss bytes into the plan's held write
    /// buffers, fire-and-forget: a short/zero landing affects only the byte
    /// counter, never `result`. Writes only the writers the plan SCHEDULE
    /// designates as fill targets for this window, so a faster tier never
    /// receives slack bytes (`isScheduledFillTarget`). The SYNCHRONOUS write
    /// side; a machine collect defers the same work to a put step.
    void pushAssembledToWriteBuffers(ByteRange physical_window, const ChainedBuffers & result, Stats & out_stats);

    /// The per-writer-list body shared by the put step and the parked-inline
    /// write: write `chain ∩ writer-range ∩ window` into each (already
    /// schedule-filtered) writer. `interrupt` (nullable) is polled between
    /// writers - the put step's stop point; remaining writers are left untouched
    /// for the caller's abandon path.
    /// `streaming`: the worker is filling the lead progressively and stays the segments'
    /// downloader across tiles - write via `CacheWriter::writeStreaming` (wake prefix readers,
    /// keep the downloader) instead of completing per write.
    void pushChainToWriters(const VectorWithMemoryTracking<WriterView> & views, ByteRange window,
        const ChainedBuffers & chain, Stats & out_stats, bool streaming = false);

    /// Write `chain ∩ writer-range ∩ window` into ONE writer (the body of the
    /// loops above).
    void writeSliceToWriter(CacheWriter * writer, ByteRange window, const ChainedBuffers & chain,
        Stats & out_stats, bool streaming = false);

    /// Whether the plan schedule designates `(entry, cell)` a fill target for a
    /// retrieve overlapping `window`. A cell holding the request is a target in
    /// every missing tier (promotion); a slack-only cell is a target only in
    /// its owning lower tier - never promoted into a faster tier.
    bool isScheduledFillTarget(ByteRange window, size_t entry, ByteRange cell) const;

    /// Re-credit, BEFORE the source fetch, any committed prefix of a frozen
    /// miss that a writer has grown since plan-build: serve it from the write
    /// buffer's own `read`, marking it `covered` so only the truly-uncommitted
    /// tail drives the fetch.
    /// `cache_credit` (unified foreground): when non-null, only the part of each served range
    /// that is ALREADY in the mask is counted as a cache hit; the rest is this serve's own source
    /// fetch transiting the cell (counted as `BytesFromSource`), so crediting it as a cache hit
    /// would double-count. Null (the legacy path) credits every served byte.
    void recreditCommittedPrefixes(ByteRange window, ChainedBuffers & result, IntervalSet & covered,
        Stats & out_stats, const IntervalSet * cache_credit = nullptr);

    /// Read from source into the pre-allocated `blocks`: DRAIN a held/carried long
    /// connection (`lc`, nullable) if it can serve this fetch, otherwise open a
    /// one-shot bounded connection, read the blocks, and let it close on return (the
    /// HTTP pool still preserves the socket). `blocks` is consumed; blocks that receive
    /// no data are released. `stop` (nullable) is the drain's interrupt point.
    ChainedBuffers readFromSource(
        const StoredObject & object, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t logical_offset,
        std::optional<size_t> read_extent, std::optional<LongConnection> * lc,
        const MachineBase * stop, Stats & out_stats);

    /// Allocate OwnedChainedBuffers covering `size` bytes, each <= `block_size`.
    /// `splits` (sorted, relative) forces block boundaries so user-window and
    /// over-read bytes land in separate buffers, releasable independently.
    static VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> allocateBlocks(
        size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits = {});

    // ─── Long connection ─────────────────────────────────────────────────

    /// Clamp the estimator's (unclamped) reach to a concrete physical end: the file
    /// end when the size is known (an unknown-size object has no end to clamp).
    size_t clampReach(size_t reach, size_t phys_off) const;

    /// The physical end a source connection opened at `phys_off` reaches before a cached run
    /// forces a reopen - the plan-geometry lookahead that sizes the long connection. See the
    /// definition: bridges resident runs strictly below `min_bytes_for_seek`.
    size_t scheduleLookaheadReach(size_t phys_off) const;

    /// The physical reach a long connection opened at `phys_off` actually gets (before any
    /// extent floor): `predictedReach` clamped to the file end, then clamped DOWN at the next
    /// wide cached run the plan shows. The single reach source shared by `shouldOpenLong` and
    /// `longConnectionBound` so the open trigger and the channel bound never disagree.
    size_t boundedReach(size_t phys_off) const;

    /// Whether to open a long connection at physical `phys_off`: the `boundedReach` forward
    /// reach runs past the current read extent (the right boundary
    /// where a short connection would stop), a connection slot is configured
    /// (`reader_executor_use_long_connections`), and pressure is not High/Critical (the
    /// open is speculative, like prefetch).
    bool shouldOpenLong(size_t phys_off) const;

    /// The long-connection bound (object-local) for an open at physical `phys_offset`:
    /// the forward reach, floored at the current read extent and capped at the object end,
    /// so a forward run extends the channel past the current right boundary. The reach is the
    /// `predictedReach` estimate clamped at the next wide cached run the plan shows. See the
    /// definition.
    size_t longConnectionBound(const StoredObject & object, size_t object_offset, size_t phys_offset) const;

    /// Foreground open hook: when `shouldOpenLong(phys_offset)` and a slot can be
    /// acquired, open a long connection over `object` (object-local `object_offset`),
    /// bounded at `longConnectionBound`, so the following source read - and the windows
    /// after it - drain it. A no-op when already held / not warranted / at capacity
    /// (then the read falls back to a one-shot).
    void openLongIfWarranted(const StoredObject & object, size_t object_offset,
        size_t phys_offset, Stats & out_stats);

    /// Open a bounded GET over `object` for the object-local range `[offset, read_end)`,
    /// taking the already-acquired `slot`; store it in `conn` (its `read_until` bound is
    /// `read_end`). The ONLY place a long connection is opened, and only on the foreground
    /// - a machine never calls this.
    void openLong(std::optional<LongConnection> & conn, const StoredObject & object,
        size_t offset, size_t read_end, LongConnectionSlot slot, Stats & out_stats) const;

    /// Serve a read at object-local `offset` from `conn` (caller has checked
    /// `servesObject` + `canContinue`): bridge a forward gap by discarding it
    /// (over-read), `readInto` the blocks, then release the connection at its bound.
    ChainedBuffers serveFromLong(std::optional<LongConnection> & conn, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks,
        size_t logical_offset, const MachineBase * stop, Stats & out_stats) const;

    /// Close `conn`: drain a small tail, account a still-incomplete drop, reset.
    void dropLong(std::optional<LongConnection> & conn, Stats & out_stats) const;

    /// Account an incomplete-connection drop (`everTransferred` and neither at
    /// EOF nor `atBound`) for `conn`.
    void accountLongDrop(const std::optional<LongConnection> & conn, bool at_eof, Stats & out_stats) const;

    /// Reset `conn` if it reached its bound (a clean pool return).
    void releaseLongAtBound(std::optional<LongConnection> & conn) const;

    /// Move a long connection out of `src`, leaving `src` EMPTY. A plain `std::optional`
    /// move leaves the source ENGAGED (with a moved-from value), so every hand-off goes
    /// through this to keep the connection a single owner (and to stop a moved-from
    /// husk from being seen as a held connection or counted as an incomplete drop).
    static std::optional<LongConnection> takeLong(std::optional<LongConnection> & src);

    // ─── Deferred puts / promotes ────────────────────────────────────────

    /// Record the window's fill-target writers (NON-OWNING views into `read_plan.bufs`) on the
    /// machine at LAUNCH, so the worker can write its led segments inline during the fetch.
    void collectFillTargets(FetchMachine & m);

    /// Turn a just-collected machine into its cache fill: using the `writer_views` recorded at
    /// launch, hand it the assembled chain and run the fill INLINE on the read thread (a failed
    /// fill logged, never thrown).
    void schedulePutStep(std::shared_ptr<FetchMachine> m, const ChainedBuffers & assembled);

    /// After the inline fill: fold the segment pin and stats in, and mark the retrieve
    /// `Done` once its whole job is fetched (logging a failed step - never the client's
    /// error).
    void reapPutMachine(FetchMachine & m);

    /// Promote a range just served from `from_tier` up into every populatable
    /// faster cache (they all miss it, since `from_tier` was the fastest
    /// hit). Walks `read_plan.bufs` in chain order, breaking at the first
    /// entry with `provider->tier() == from_tier` (tier-equality, so a slower
    /// fs hit is never promoted to a faster fs). The committed-set makes
    /// out-of-order promote slices idempotent. `bytes`/`range` are physical,
    /// pre-decryption.
    void maybePromote(CacheTier from_tier, ByteRange range, const ChainedBuffers & bytes, Stats & out_stats);

    /// Down-promote: a served upper-tier hit that the schedule marked for cross-cache fill (an
    /// `UpperCacheRead` retrieve) is written DOWN into its lower cell using the bytes just served -
    /// the cache-as-buffer mirror of `maybePromote`'s up-promote. The bytes are in hand (no re-read
    /// from a faster tier), so the lower segment completes ACROSS an embedded hit without a remote
    /// over-read of bytes a faster tier already holds. Only scheduled `UpperCacheRead` targets are
    /// filled - a bridged hole stays a remote over-read. `served_range`/`bytes` are physical.
    void downFillScheduledLower(ByteRange served_range, const ChainedBuffers & bytes, Stats & out_stats);

    /// Serve-front promote of a FETCHED range: the `Remote` fetch now fills only the bottom
    /// populatable tier (`writeTargetsFor`), so push its committed bytes UP into the faster
    /// tiers here, as the serve passes over `window` (physical). Reads the bottom tier's
    /// committed bytes and `maybePromote`s them; idempotent via the faster tiers' committed
    /// sets, so re-calling per served window is safe. The pc fill thus trails the serve
    /// cursor (its own budget) instead of riding the fetch front's lead.
    void promoteFetchedToUpper(ByteRange window, Stats & out_stats);

    // ─── Plan build ──────────────────────────────────────────────────────

    /// Query cache residency ONCE over the look-ahead span via the read-only
    /// `planResidencyView`, stash the geometry and the held buffers. While the
    /// plan is held, resident bytes stream straight from the held read buffers
    /// - no per-window `getOrSet`. Rebuilt lazily whenever the cursor leaves
    /// the planned span. Resets the in-flight pin before discarding the old
    /// plan.
    void observeAndSchedule(size_t physical_start);

    /// Schedule-cursor maintenance: `cursor` = index of the `schedule.steps` step whose
    /// `output` contains the current `position_phys`; the interpreter dispatches and serves
    /// from it. `reconstructCursor` re-derives it after a re-plan or seek; `advanceCursor`
    /// moves it forward as windows are served.
    size_t findStepContaining(size_t pos_phys) const;
    void reconstructCursor();
    void advanceCursor();

    /// Bring the plan and cursor up to date for serving at `position_phys`: collect an in-flight
    /// machine sitting at the consumed plan end, then (re)plan once the plan is fully consumed
    /// (cursor before `plan_start`, or at `plan_end` with the plan not already running to EOF).
    /// Never replans while a machine is in flight - that would re-probe residency and could see
    /// the worker's just-fetched gap as resident.
    void prepareCursor(size_t position_phys);

    /// The shared post-serve tail of `readNextWindow`: account the served window, advance the
    /// cursor, net out the over-read, drop the fill pin at EOF, launch the next read-ahead, and
    /// decrypt. Returns the plaintext window.
    ChainedBuffers finishWindow(ChainedBuffers chain);

    /// The schedule-driven interpreter: `readNextWindow` runs the schedule's already-planned
    /// jobs instead of re-deriving the next gap from the coverage map. `interpretStep`
    /// dispatches on `steps[cursor]`; `maybeLaunchAhead` launches the schedule's `Remote`
    /// jobs at a frontier.
    ChainedBuffers interpretStep(size_t position_phys);
    ChainedBuffers serveHitStep(const PlanSchedule::Step & step, size_t position_phys);
    ChainedBuffers serveRetrieveStep(const PlanSchedule::Step & step, size_t ri, size_t position_phys);
    /// Serve a populatable retrieve (one that fills a cache cell) from the committed cell -
    /// the cache is the buffer, so no in-memory bank is held. Drives the in-flight worker
    /// (or a foreground fallback) until the bottom cell covers the window, reads it back via
    /// `recreditCommittedPrefixes`, and promotes the served run up. A bypass gap keeps the bank.
    ChainedBuffers serveRetrievePopulatable(const PlanSchedule::Step & step, size_t ri, size_t position_phys);
    /// Read-only coverage of `window_phys` by the plan's held write buffers' committed ranges
    /// (the read-only twin of `recreditCommittedPrefixes`'s computation: no read, no stats). A
    /// byte a SIBLING downloaded is NOT in this executor's per-writer committed set, so it reads
    /// as uncovered here - which is what bounds the inline serve to its own led prefix.
    IntervalSet committedCoverage(ByteRange window_phys) const;
    /// Does the committed coverage span the whole window?
    bool committedCellCovers(ByteRange window_phys) const;
    /// The end of the CONTIGUOUS committed run from `window_phys.offset` (== offset when nothing
    /// is committed there). The inline populatable serve narrows to this prefix: the first
    /// sibling-led byte bounds it short, so the serve returns the led prefix as a short window.
    size_t committedCellPrefixEnd(ByteRange window_phys) const;
    /// Serve a populatable window from the cells: the committed prefix, then (when `allow_wait`)
    /// the still-uncommitted bytes the in-flight worker is downloading, waited on per-frontier
    /// via the worker's OWN target writers (`waitAndReadSiblingLed`). Accumulates into `out` /
    /// `covered`; the caller checks full coverage and falls back to a foreground fetch otherwise.
    void serveWindowFromCells(ByteRange window_phys, bool allow_wait, ChainedBuffers & out, IntervalSet & covered,
        Stats & out_stats, const IntervalSet * cache_credit = nullptr);
    ChainedBuffers serveStepFromBanked(const PlanSchedule::Step & step, RetrieveStatus & st, size_t position_phys) const;
    /// Inline serve for a window no prefetch machine and no committed cell covers: a not-prefetched
    /// bypass gap (pure source fetch, banked) or a populatable gap whose cursor segment a sibling
    /// executor leads (wait the sibling's disk cell to dedup, source-fetch any remainder). Banks the
    /// window in `ready_bytes` and serves it from there.
    ChainedBuffers serveWindowInline(size_t ri, size_t position_phys);
    void collectInFlightInto(size_t ri);
    void maybeLaunchAhead();
    /// Build the machine's runner-independent fetch step (see the definition). Shared by the
    /// pool runner and the future inline runner.
    std::function<StepResult()> makeFetchStep(FetchMachine & m);
    /// Build a machine for `window` and run it via `machine_runner` (pool = async read-ahead,
    /// local = inline foreground fetch); sets `machine` on success. Returns false on a pool queue
    /// reject (the connection is reclaimed). The sole machine builder, shared by both runners.
    bool launchMachineForWindow(size_t ri, ByteRange window, IFetchMachineRunner & machine_runner);
    void launchRetrieve(size_t ri);
    bool depsSatisfied(size_t ri) const;

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

    /// The fixed plan window: `max(window_size, plan_look_ahead_max_window)`.
    size_t effectivePlanCeiling() const;

    /// Whether the current plan already extends to the source end (known size). A
    /// margin-driven replan then only rebuilds the identical plan (and recreates the
    /// held cache readers), so the serve path suppresses it and streams within the
    /// held plan to EOF. Unknown-size sources learn EOF via a short read and keep
    /// replanning.
    bool planReachesEnd() const;

    /// Translate ONE tier's `planResidencyView` into its 1:1
    /// `GeometryEntry`/`BufEntry`. `extractResidentRuns` records the tier's
    /// hits (clamped to the plan span). `extractMissesAndOpenWriters` records
    /// its cache-aligned misses and opens the write buffers (populatable
    /// tiers only), PRUNING any miss cell fully covered by `upper_hits` (the
    /// union of faster tiers' hits) - that range already lives upstream.
    static void extractResidentRuns(const CacheView & view, ByteRange plan_range, size_t resident_clip_end, GeometryEntry & geom_entry);
    static void extractMissesAndOpenWriters(
        ICacheProvider & cache, const CacheView & view,
        const StoredObject & object, size_t object_file_offset,
        const IntervalSet & upper_hits, GeometryEntry & geom_entry, BufEntry & buf_entry);

    /// Pin the partial segment under `frontier` from the first held write
    /// buffer whose `range` contains it and whose `pin` is non-null. Empty
    /// when nothing partial is there.
    CacheWriter::CacheSegmentPin writerPinAt(size_t frontier) const;

    // ─── Machine lifecycle ───────────────────────────────────────────────

    /// Whether the single in-flight `machine` is currently serving retrieve `ri`. There is
    /// at most one machine at a time (the re-plan barrier asserts it), and it carries the
    /// `retrieve_index` it was launched for, so this is the "is a machine running for this
    /// retrieve" presence test the serve loop branches on.
    bool machineFor(size_t ri) const { return machine && machine->retrieve_index == ri; }

    /// The runner that drives the in-flight machine's revoke/release verbs at collect: the pool
    /// runner when read-ahead launched it, else the inline runner (no pool). The verbs branch on
    /// the machine's `current_step`, so a settled inline machine no-ops through either.
    IFetchMachineRunner & collectRunner() { return runner ? *runner : *local_runner; }

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

    /// Whether read-ahead prefetch runs at the given pressure level: true at
    /// Normal/Elevated, false (suppressed) at High/Critical - read-ahead is
    /// speculative, so once memory is tight it stops entirely.
    bool prefetchEnabled(MemoryPressureLevel level) const;

    /// How far the in-order fill front runs ahead of the serve cursor: the bottom populatable
    /// tier's lead. A disk-backed (`FilesystemCache`) bottom is flat (`fill_ahead_lead`); a
    /// RAM-backed (`PageCache`-only) bottom or a bypass gap cannot serve a prefix ahead, so the
    /// lead stays one (pressure-scaled) effective window.
    size_t fillAheadLead(MemoryPressureLevel level) const;

    /// Shrink `win_size` so the read does not pass `read_extent_end`.
    /// Saturates to 0 once `position` reaches the extent (recoverable:
    /// extending the extent resumes).
    size_t clampToExtent(size_t win_size) const;

    /// Trim a desired logical read size at `position` to the file end (when
    /// known) and the read extent - the per-read analogue of `boundedPlanSpan`.
    size_t boundedReadSize(size_t want) const;

    /// The advertised read extent (`setReadUntilPosition`) has been reached - no room left
    /// within it, though the file may continue. `readNextWindow` uses this (not the file end)
    /// to gate the (re)plan once EOF is handled separately.
    bool atExtent() const { return read_extent_end && position >= *read_extent_end; }

    bool atEnd() const
    {
        if (offset_map.hasUnknownSize())
            return reached_eof;
        return reached_eof || position >= totalSize();
    }

    /// The per-call read ceiling in LOGICAL bytes: bytes remaining to the read extent, or one
    /// window when the source size is unknown. `> 0` iff there is room to read at the cursor.
    /// (Was the `to_read` parameter.) Deliberately does NOT test `reached_eof`: when EOF latches
    /// with a machine still in flight, `readNextWindow`'s `atEnd()` branch drains that final
    /// window through `interpretStep`, which serves only while `readCeiling() > 0`.
    size_t readCeiling() const
    {
        return offset_map.hasUnknownSize() ? clampToExtent(window_size) : clampToExtent(totalSize() - position);
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
    /// Single fixed size for the plan window (Options).
    size_t plan_look_ahead_max_window;
    /// Long-connection sizing bounds (Options): open range floor and hard cap.
    size_t long_connection_open_range;
    size_t long_connection_max_bound;
    /// Fill-ahead lead for a disk bottom tier (Options).
    size_t fill_ahead_lead;

    /// Cursor state.
    size_t position = 0;
    /// Set when the source returned short AND the total size is unknown - the
    /// short return IS the EOF marker.
    bool reached_eof = false;
    /// Logical end of the advertised read region (`makeTransientForReadAt`
    /// one-shot extent, or `setReadExtent`). `nullopt` = read to the file end.
    std::optional<size_t> read_extent_end;

    /// Physical (file-offset) ranges fetched from the source BEYOND the window they served -
    /// alignment slack and the read-ahead's fetched-ahead bytes, both written to the cache.
    /// A range is REMOVED when the serve later delivers it (the run-ahead reads it back from the
    /// cache), so it never counts as waste; what remains at the end is the genuine over-read
    /// (`OverReadBytes`, emitted once in the destructor). FOREGROUND-PRIVATE: only the foreground
    /// `assembleAndWriteBack` adds and the serve removes; the worker never touches it.
    IntervalSet overread_pending;

    /// The current look-ahead plan (source of truth; geometry snapshot null
    /// until the first plan is built).
    ReadPlan read_plan;

    /// Machines / pool.
    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    /// The machine driver over `prefetch_pool`: state writes, scheduling and
    /// the revoke/release edges live there; every policy decision stays here.
    /// Created in the constructor from `Options::prefetch_pool`; null without a pool.
    /// Drives the read-ahead (pool) FETCH machines only.
    std::unique_ptr<IFetchMachineRunner> runner;
    /// Inline driver, always present: runs a foreground serve machine synchronously on the read
    /// thread. Also the fallback collect-runner when there is no pool - its verbs no-op on a
    /// settled inline machine (null `current_step`).
    std::unique_ptr<IFetchMachineRunner> local_runner;
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

    /// Connection state. The Strategy-A pin holding the partial cache segment
    /// under the live fill frontier non-evictable across windows: re-pointed at
    /// each `finalizeAssembledWindow`, dropped on seek / EOF / plan rebuild.
    /// Foreground-only (a prefetch worker does a pure source fetch and never
    /// touches it). NOT long-connection state - a one-shot fill needs it too.
    CacheWriter::CacheSegmentPin inflight_segment_pin;
    /// Server-wide long-connection limit handle, shared with
    /// `makeTransientForReadAt`. Gates long-connection opens (the
    /// `reader_executor_use_long_connections` setting).
    std::shared_ptr<LongConnectionLimit> long_connection_limit;
    /// The held long source connection. Foreground hold; it rides the machine
    /// payload when a prefetch carries it (`takeLong` at launch, reclaimed at
    /// collect). Empty when no long connection is currently open.
    std::optional<LongConnection> long_conn;

    /// Continuous-read pattern estimator, fed each plan's predicted source reads
    /// and every seek. Constructed with `near_gap == min_bytes_for_seek` so a
    /// bridged gap counts identically whether modeled as a read-through or a seek.
    /// `predictedReach` sizes the long source connection (see `longConnectionBound`).
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
    ReaderExecutorDecryptor decryptor;
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
