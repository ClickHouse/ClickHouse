#pragma once

#include <IO/ChainedBuffers.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/ResidencyIterator.h>
#include <IO/IntervalSet.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/LongConnectionLimit.h>
#include <IO/LongConnection.h>
#include <IO/ReaderExecutorStats.h>
#include <IO/CoverageMap.h>
#include <IO/PlanSchedule.h>
#include <IO/ReaderExecutorFetchMachine.h>
#include <IO/ReaderExecutorFillLane.h>
#include <IO/ReadContinuityTracker.h>

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
#include <atomic>
#include <limits>
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
class EncryptionHeaderCache;

/// Reads a logical file (one or more `StoredObject`s mapped by `OffsetMap`)
/// through a fastest-first cache chain, falling back to the source.
///
/// THE MODEL - a producer and a consumer over the cache as the pipe:
///
///                      lead (clamped, pressure-bounded)
///                ◄──────────────────────────────►
///   SOURCE ──► [ FILL LANE ] ──► CACHE CELLS + BANK ──► [ SERVE ] ──► caller
///               the producer       (the DISPLAY =         the consumer
///                                   the buffer)
///
/// The buffer is POSITIONAL (offset-indexed, not FIFO) and SHARED (sibling
/// executors read and write the same cells) - which is why seeks, contention,
/// and warm reads are ordinary cases. Planning is a ROLLING window: residency
/// is observed per tier by one ranged `lookAt` over the providers, the
/// job list derives from the geometry (`PlanSchedule`), and the plan then
/// EXTENDS forward and SLIDES the passed territory out as the cursor advances;
/// a near seek REUSES it, and only a far jump RESTARTS from scratch.
/// Execution interprets the schedule; the display carries the truth.
///
/// The PRODUCER is the `FillLane` + the fill flow, ONE body of code with two
/// anchors: AHEAD (`prefetch` - the wake rule and the launch scan at the
/// lane's `attempted_end` cursor) runs pool pieces; the PUMP (`pump` -
/// anchored at the consumer's window, may run BEHIND the cursor) heals on the serve thread
/// whatever the display cannot cover: it waits live writers, runs inline
/// pieces, and falls to the shared heal verb `bankDirectRead` - one bounded
/// cache-blind read for state no planned job can produce (a hung sibling
/// leader; a claimed-but-unreadable frontier, which the serve loop heals
/// directly). The lane owns everything on the fill side: the long connection
/// (lent to a pool piece for its flight, opens refused while lent), the
/// in-flight segment pin, the ahead cursor, and the one gate every cell
/// write lands through.
///
/// The CONSUMER serves windows off the DISPLAY - the one read surface (hit
/// views + live committed cells + the bank as the overflow holder) - pumping
/// the producer until the cursor is covered, and hands the served bytes to
/// the scheduled promotes (`runHandedFills`: served bytes fill the faster
/// cells the fetch skipped). It opens no source path of its own - healing
/// runs producer code.
///
/// CACHE-CHAIN POLICY - correctness for many tiers, performance for one.
/// With several populating caches the executor stays CORRECT but plans and
/// paces against the BOTTOM populating tier alone; upper tiers are a serve
/// bonus (a hit serves from its fastest holder), not a coordination target.
/// There is deliberately NO cross-tier down-fill: a faster tier's resident
/// range over a lower cell leaves the lower segment partial - a write past
/// it is refused into the bank (served, not persisted), the demand
/// read-through completes an interior hole from the source, and a tail hole
/// heals once the upper tier evicts and the range becomes a plain miss. One
/// populating tier - the production shape - has no such ranges and takes
/// the unimpaired path.
///
/// Tuned for sequential scans: one machine (the in-flight PIECE) ahead on a
/// `PrefetchThreadPool`, window/block sizes shrink under memory pressure.
/// Owns its cache and decryption layers, so it is NOT wrapped by the legacy
/// async/decrypt/cache read buffers. Each source read is a bounded GET; the
/// long connection coalesces them across pieces.
///
/// Vocabulary joins (names the narrative above does not spell out):
///   piece  -> one `FetchMachine` window;
///   F      -> `FillLane::attempted_end`, the producer's ahead cursor;
///   bank   -> `FillLane::bank`, the one overflow display cell;
///   restart-> a from-scratch plan build (`observeAndSchedule`): the first
///             read or a far seek; everything else extends/reuses the plan.
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
    /// Gap bound for `buildSchedule`'s bridge threshold and the producer's led-run
    /// merge (`mergeRanges`): a gap strictly smaller than this is coalesced
    /// (over-read) into one source request rather than read separately; a gap at or
    /// above it reopens. Near the bandwidth/request cost breakeven.
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 2 * 1024 * 1024; /// 2 MiB
    /// Drain bound: if only a tail of at most this many bytes remains to a long
    /// connection's read bound, drain it so the connection completes pool-reusable
    /// (see `dropLongConnection`) instead of being abandoned mid-response.
    static constexpr size_t DEFAULT_MAX_TAIL_FOR_DRAIN = 512 * 1024; /// 512 KiB
    static constexpr size_t CHAINED_BUFFER_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB per ChainedBuffers node
    /// The plan span: residency is probed ONCE over this whole span, so cache
    /// discovery amortises across many serve windows and the plan spans past the
    /// fill-ahead lead. Touched cells may overhang the span - fill-only work via
    /// the schedule's cell closure; the geometry itself never exceeds the span.
    /// The read bound does not size the plan, so the plan survives mark-range
    /// advances and is reused.
    static constexpr size_t DEFAULT_PLAN_LOOK_AHEAD_MAX_WINDOW = 4 * DEFAULT_WINDOW_SIZE; /// 32 MiB
    /// A warranted long connection opens with at least this much range and never streams
    /// past the cap. The continuous-read prediction may under-predict at the start of a run
    /// and over-predict at its end; these bound the resulting GET so an over-prediction
    /// cannot run away into an unbounded over-read.
    static constexpr size_t DEFAULT_LONG_CONNECTION_OPEN_RANGE = 8 * 1024 * 1024; /// 8 MiB
    static constexpr size_t DEFAULT_LONG_CONNECTION_MAX_BOUND = 128 * 1024 * 1024; /// 128 MiB
    /// How far the in-order fill front runs AHEAD of the serve cursor (the cache-as-buffer
    /// lead): the single in-flight machine fetches up to this much into the cells, committing
    /// progressively, while the serve reads the committed prefix. Cursor-anchored (`launchRetrieve`
    /// launches only inside `[cursor, cursor + lead)`) and self-limited by cell acceptance: a
    /// bottom that takes nothing stops the worker at one window of retained residue
    /// (`coordinatedPrefetch`). Distinct from the plan window (the geometry/pin horizon).
    static constexpr size_t DEFAULT_FILL_AHEAD_LEAD = 16 * 1024 * 1024;     /// 16 MiB

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
        /// The plan pins the touched segments within the span and is reused across
        /// read-extent advances while the cursor stays inside it; touched miss cells
        /// may overhang the span as fill-only work.
        size_t plan_look_ahead_max_window = DEFAULT_PLAN_LOOK_AHEAD_MAX_WINDOW;
        /// Long-connection sizing bounds (see `DEFAULT_LONG_CONNECTION_OPEN_RANGE` /
        /// `DEFAULT_LONG_CONNECTION_MAX_BOUND`).
        size_t long_connection_open_range = DEFAULT_LONG_CONNECTION_OPEN_RANGE;
        size_t long_connection_max_bound = DEFAULT_LONG_CONNECTION_MAX_BOUND;
        /// Fill-ahead lead for a disk bottom tier (see `DEFAULT_FILL_AHEAD_LEAD`).
        size_t fill_ahead_lead = DEFAULT_FILL_AHEAD_LEAD;
        std::shared_ptr<PrefetchThreadPool> prefetch_pool;
        std::shared_ptr<LongConnectionLimit> long_connection_limit;
        /// Null unless a random-object-key encrypted disk allowed caching this
        /// file's encryption headers (see `DiskEncrypted::prepareRead`).
        std::shared_ptr<EncryptionHeaderCache> encryption_header_cache;
        std::shared_ptr<ReaderExecutorLog> reader_executor_log;
        /// Drive read-ahead fetch machines on Silk fibers instead of the pool.
        bool use_fiber_runner = false;
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

    /// The external prefetch trigger (the buffer's `ReadBuffer::prefetch` hook,
    /// forwarded from `PipelineReadBuffer`): collect the in-flight machine,
    /// replan the look-ahead window from the current position, and launch the
    /// remote fetch jobs. Same driver that `readNextWindow`/`seek` run
    /// internally, exposed so a consumer can start read-ahead before it pulls
    /// the next window. A no-op when no async runner is attached.
    void prefetch();

    /// Feed the single READ BOUND (LOGICAL, monotone-max): every declaration of
    /// how far the read goes lands here - the per-range read-until (advanced per
    /// mark range by `MergeTreeReaderStream::adjustRightMark`) and the announced
    /// planned end (the last mark this reader will ever be assigned) alike.
    /// Prefetch and connection sizing never speculate past the bound; serving is
    /// never capped by it. `nullopt` (read-to-EOF) lifts the bound to the file
    /// end - the widest declaration wins, so it also forgets a planned end.
    /// Per-range EOF presentation lives in `PipelineReadBuffer::read_until`,
    /// not here.
    void setReadBound(std::optional<size_t> logical_end);

    /// The REQUEST MAP (LOGICAL, advisory): the exact ranges the caller's whole
    /// assignment will read. Empty/absent = the whole file. REPLACES the stored
    /// map (a task switch re-announces the new assignment). Inert for now:
    /// stored knowledge for demand-aware fill sizing and speculation.
    void setRequestMap(std::vector<std::pair<size_t, size_t>> ranges);  // STYLE_CHECK_ALLOW_STD_CONTAINERS

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
    void addDecryptionLayer(String path, KeyFinderFunc key_finder);

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

private:
    // ─── Nested types ────────────────────────────────────────────────────
    // (`Display` is declared further down at the display section; `FillLane`
    //  at the fill-lane section - each next to the verbs that use it.)

    /// The stats machinery lives in `IO/ReaderExecutorStats.h` (pure move); these
    /// aliases keep the ~100 in-class references and the test inspector's
    /// `ReaderExecutor::Stats::X` spellings compiling unchanged.
    using Stats = ReaderExecutorStats;
    using StatTimer = ReaderExecutorStatTimer;

    /// One (object-piece, tier) entry of the FOREGROUND-PRIVATE half of a
    /// plan: the provider/object identity, the read-only probe
    /// `view` (its hits own the held pinning read buffers; its misses carry
    /// NULL writers and are never dereferenced), and the AUTHORITATIVE
    /// `writers` opened over the aligned miss ranges. 1:1 POSITIONAL with
    /// `CoverageMap::entries`, provider-grouped fastest-first. A worker
    /// never indexes this, so the buffers are never shared across threads.
    struct PlanTier
    {
        ICacheProvider * provider = nullptr;
        /// The tier's plan-state object: hit readers (pinning resident segments)
        /// and miss cells, upgraded in place with write buffers for the cells the
        /// plan fills (the prune+upgrade step of `observeAndSchedule`).
        CacheViewPtr view;
    };

    /// One look-ahead plan, the SOURCE OF TRUTH for the current read: the
    /// immutable geometry snapshot plus the held buffers. FOREGROUND-PRIVATE.
    /// Held across many windows; destroyed (write buffers finalize, deferred
    /// LRU bumps run) at the next `observeAndSchedule` / on seek.
    struct ReadPlan
    {
        const std::shared_ptr<const CoverageMap> & geometry() const { return geometry_snapshot; }


        VectorWithMemoryTracking<PlanTier> tiers;

        /// The explicit work of this plan (`buildSchedule`), computed once at
        /// build. Its `retrieves[*].into` are the authoritative fill targets:
        /// the deferred put borrows exactly the writers a retrieve designates,
        /// so slack is filled only into its owning lower tier and never
        /// promoted into a faster tier.
        PlanSchedule schedule;

        /// The launch interpreter's authority - the first `schedule.retrieves` index not
        /// yet launched/exhausted; advanced by `prefetch`. Reset on re-plan.
        size_t launch_frontier = 0;

        /// True iff `schedule.retrieves` contains a `Source::Remote` job. When false
        /// the plan is served entirely from cache tiers, so there is no source
        /// connection to open and `prefetch` skips its prefetch bookkeeping
        /// (after its look-ahead re-plan, which still discovers cold beyond the plan).
        bool has_remote_retrieves = false;

    private:
        friend class ReaderExecutor;  /// `observeAndSchedule` is the sole writer.
        std::shared_ptr<const CoverageMap> geometry_snapshot;
    };

    using WriterView = ReaderExecutorWriterView;
    using FetchMachine = ReaderExecutorFetchMachine;
    using FillLane = ReaderExecutorFillLane;

    // ─── Window serve path ───────────────────────────────────────────────

    /// Whether served payload is encrypted (`data_start_offset` is the header
    /// size, 0 when there is no encryption / no SSL).
    bool needsDecryption() const { return data_start_offset > 0; }

    /// The ONLY logical<->physical converters. Everything inside the executor
    /// (plan, schedule, display, machines, the lane bank) is PHYSICAL
    /// (header-inclusive file coords); the consumer API (`position`,
    /// `read_bound`, `totalSize`, served windows) is LOGICAL (payload
    /// coords). Cross exactly here - raw `+/- data_start_offset` elsewhere is
    /// a bug. No byte below the header ever reaches a logical consumer, so a
    /// physical value smaller than the header is corrupt input, not a case.
    size_t toPhys(size_t logical) const { return logical + data_start_offset; }
    size_t toLogical(size_t phys) const
    {
        chassert(phys >= data_start_offset);
        return phys - data_start_offset;
    }

    /// Return a plaintext copy of `cipher` (or `cipher` unchanged when there is
    /// nothing to decrypt). Each node is copied into a fresh `OwnedChainedBuffer`
    /// and decrypted at its `logical_offset` - never in place, since the served
    /// nodes alias live page-cache / cache cells. CTR is position-addressable, so
    /// per-node decryption at each node's logical offset is exact.
    ChainedBuffers decryptWindow(ChainedBuffers && cipher);

    /// Decrypt `data` in place at `logical_offset` via the reentrant `decryptor`.
    /// Safe to call from a worker concurrently with the foreground.
    void decryptInPlace(char * data, size_t size, size_t logical_offset);

    /// Read the encryption headers (physical `[0, data_start_offset)`) through the
    /// cache chain: serve from the first tier holding them, else fetch from source
    /// and populate the incrementally-fillable tiers - the headers are the first
    /// bytes of the first cache cell, and skipping them would leave that cell's
    /// append-only prefix permanently uncommitted. Runs before any plan exists.
    ChainedBuffers fetchEncryptionHeader();

    /// Serve a clamped resident sub-range from a view's hit buffers, recording
    /// each read for the deferred LRU bump. A hit is readable in full. The
    /// result is contiguous from `clamped.offset` and may be short only at the
    /// TAIL (`Display::read` marks `range().size` bytes covered, so a mid-range
    /// hole would over-mark coverage).
    static ChainedBuffers readHitFromView(CacheView & view, ByteRange clamped);

    // ─── Gap fetch + backfill ────────────────────────────────────────────

    /// PURE source fetch: read the WHOLE `physical_window` from the source as
    /// one contiguous physical ChainedBuffers (short at EOF), no cache/plan/pin. This is
    /// ALL a machine fetch step runs. `stop` (nullable) carries the machine's
    /// cooperative stop flag: a one-shot GET is never cut mid-response (polled
    /// between connections), while a long-connection drain stops at block
    /// granularity. A stop-short return has the same shape as an
    /// EOF-short one and must neither latch EOF nor throw (the flag is checked FIRST).
    /// `lc` (nullable) is the long connection to DRAIN if it can serve a piece - the
    /// worker passes its machine's payload, never the foreground's.
    /// The IN-FETCH opener (`openLongConnectionIfWarranted` at each object-piece start) engages
    /// only when `lc` IS the lane's slot - the FOREGROUND context (the foreground and inline
    /// machines, which run on the serve thread). A pool worker passes its machine's own payload
    /// and never opens in-fetch; it carries what its LAUNCH gave it (`launchMachineForWindow`
    /// is the other opener site).
    ChainedBuffers fetchWindowFromSource(ByteRange physical_window, bool from_prefetch,
        bool & eof_latch, MemoryPressureLevel pressure_level, bool bound_advertised,
        std::optional<LongConnection> * lc, const MachineBase * stop, Stats & out_stats);

    /// The machine fetch step (runs on the worker thread): elect the FileCache downloader
    /// over the window's fill-target `writer_views`, fetch the LED runs from the source via
    /// the machine's own connection, and write+complete them INLINE on this thread (the
    /// downloader contract). Segments a sibling leads are SKIPPED and flagged via
    /// `m.contended` so the foreground revokes to the sync path at collect. Retains in
    /// `m.fetched` ONLY the residue no cell accepted (see the field doc), capped at one
    /// window; `m.fetched_end` records the fetch frontier.
    void coordinatedPrefetch(FetchMachine & m);

    /// Merge ranges separated by less than `min_gap` (the producer's led-run
    /// coalescing - reading through a small hole is cheaper than reopening).
    static VectorWithMemoryTracking<ByteRange> mergeRanges(const VectorWithMemoryTracking<ByteRange> & ranges, size_t min_gap);

    /// Sub-ranges of `range` not committed in ANY of the `views`' cells - the bytes that
    /// would be lost if dropped from memory. With no views (a bypass window) the whole
    /// `range` is uncommitted.
    static VectorWithMemoryTracking<ByteRange> uncommittedIn(
        const VectorWithMemoryTracking<WriterView> & views, ByteRange range);

    /// The per-writer-list body shared by the fetch step's per-tile commit and
    /// the collect's put step: write `chain ∩ writer-range ∩ window` into each
    /// (already schedule-filtered) writer.
    void pushChainToWriters(const VectorWithMemoryTracking<WriterView> & views, ByteRange window,
        const ChainedBuffers & chain, Stats & out_stats);

    /// Write `chain ∩ writer-range ∩ window` into ONE writer (the body of the
    /// loops above). Takes its own scoped `claim` over the target - a no-op-release
    /// nested claim when the calling worker already holds the cells' roles.
    void writeSliceToWriter(CacheWriter * writer, ByteRange window, const ChainedBuffers & chain,
        Stats & out_stats);

    /// Read from source into the pre-allocated `blocks`: DRAIN a held/carried long
    /// connection (`lc`, nullable) if it can serve this fetch, otherwise open a
    /// one-shot bounded connection, read the blocks, and let it close on return (the
    /// HTTP pool still preserves the socket). `blocks` is consumed; blocks that receive
    /// no data are released. `stop` (nullable) is the drain's interrupt point.
    ChainedBuffers readFromSource(
        const StoredObject & object, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t file_pos,
        bool bound_advertised, std::optional<LongConnection> * lc,
        const MachineBase * stop, Stats & out_stats);

    /// Allocate OwnedChainedBuffers covering `size` bytes, each <= `block_size`.
    static VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> allocateBlocks(size_t size, size_t block_size);

    // ─── Long connection ─────────────────────────────────────────────────

    /// Clamp the estimator's (unclamped) predicted end to a concrete physical end: the file
    /// end when the size is known (an unknown-size object has no end to clamp).
    size_t clampReach(size_t predicted_end, size_t phys_off) const;

    /// The physical end a source connection opened at `phys_off` reaches before a cached run
    /// forces a reopen - the plan-geometry lookahead that sizes the long connection. See the
    /// definition: bridges resident runs strictly below `min_bytes_for_seek`.
    size_t scheduleLookaheadReach(size_t phys_off) const;

    /// The physical reach a long connection opened at `phys_off` actually gets (before any
    /// extent floor): `predictedEnd` clamped to the file end, then clamped DOWN at the next
    /// wide cached run the plan shows. The single reach source shared by `shouldOpenLongConnection` and
    /// `longConnectionBound` so the open trigger and the channel bound never disagree.
    size_t boundedReach(size_t phys_off) const;

    /// Whether to open a long connection at physical `phys_off`: the `boundedReach` forward
    /// reach runs past the current read extent (the right boundary
    /// where a short connection would stop), a connection slot is configured
    /// (`reader_executor_use_long_connections`), and pressure is not High/Critical (the
    /// open is speculative, like prefetch).
    bool shouldOpenLongConnection(size_t phys_off) const;

    /// The long-connection bound (object-local) for an open at physical `phys_offset`:
    /// the forward reach, floored at the current read extent and capped at the object end,
    /// so a forward run extends the channel past the current right boundary. The reach is the
    /// `predictedEnd` estimate clamped at the next wide cached run the plan shows. See the
    /// definition.
    size_t longConnectionBound(const StoredObject & object, size_t object_offset, size_t phys_offset) const;

    /// Foreground open hook: when `shouldOpenLongConnection(phys_offset)` and a slot can be
    /// acquired, open a long connection over `object` (object-local `object_offset`),
    /// bounded at `longConnectionBound`, so the following source read - and the windows
    /// after it - drain it. A no-op when already held / not warranted / at capacity
    /// (then the read falls back to a one-shot).
    void openLongConnectionIfWarranted(const StoredObject & object, size_t object_offset,
        size_t phys_offset, Stats & out_stats);

    /// Open a bounded GET over `object` for the object-local range `[offset, read_end)`,
    /// taking the already-acquired `slot`; store it in `conn` (its `read_until` bound is
    /// `read_end`). The ONLY place a long connection is opened, and only on the foreground
    /// - a machine never calls this.
    void openLongConnection(std::optional<LongConnection> & conn, const StoredObject & object,
        size_t offset, size_t read_end, LongConnectionSlot slot, Stats & out_stats) const;

    /// Serve a read at object-local `offset` from `conn` (caller has checked
    /// `servesObject` + `canContinue`): bridge a forward gap by discarding it
    /// (over-read), `readInto` the blocks, then release the connection at its bound.
    ChainedBuffers serveFromLongConnection(std::optional<LongConnection> & conn, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks,
        size_t file_pos, const MachineBase * stop, Stats & out_stats) const;

    /// Close `conn`: drain a small tail, account a still-incomplete drop, reset.
    void dropLongConnection(std::optional<LongConnection> & conn, Stats & out_stats) const;

    /// Account an incomplete-connection drop (`consumedAnyBytes` and neither at
    /// EOF nor `atBound`) for `conn`.
    void accountLongConnectionDrop(const std::optional<LongConnection> & conn, bool at_eof, Stats & out_stats) const;

    /// Reset `conn` if it reached its bound (a clean pool return).
    void releaseLongAtBound(std::optional<LongConnection> & conn) const;

    // ─── Deferred puts / promotes ────────────────────────────────────────

    /// Record the window's fill-target writers (NON-OWNING views into `read_plan.tiers`) on the
    /// machine at LAUNCH, so the worker can write its led segments inline during the fetch.
    void collectFillTargets(FetchMachine & m);

    /// Turn a just-collected machine into its cache fill: using the `writer_views` recorded at
    /// launch, hand it the assembled chain and run the fill INLINE on the read thread (a failed
    /// fill logged, never thrown).
    void runPutStep(const FetchMachine & m, const ChainedBuffers & assembled);

    /// Run the scheduled PROMOTE jobs overlapping the just-served range (`HandedChain`):
    /// write served bytes UP into the faster cells the fetch deliberately skipped
    /// (`writeTargetsFor` fills only the bottom tier; the pc fill thus trails the serve
    /// cursor instead of riding the fetch lead). Their INPUT is the served bytes, so they
    /// are inherently serve-front jobs; the background never runs them ahead
    /// (`ahead_eligible`). The jobs' `into` cells carry the `[CF-promote]` no-same-tier
    /// rule as data. The writers' committed sets make every write idempotent.
    /// `served_range`/`bytes` are physical, pre-decryption.
    void runHandedFills(ByteRange served_range, const ChainedBuffers & bytes, Stats & out_stats);

    // ─── Plan build ──────────────────────────────────────────────────────

    /// Query cache residency ONCE over the look-ahead span via the read-only
    /// probe walk, stash the geometry and the held buffers. While the
    /// plan is held, resident bytes stream straight from the held read buffers
    /// - no per-window `getOrSet`. Rebuilt lazily whenever the cursor leaves
    /// the planned span. Resets the in-flight pin before discarding the old
    /// plan.
    void observeAndSchedule(size_t physical_start);

    /// The serve run whose `output` contains `pos_phys` (clamps to the last run past the
    /// materialized span). DERIVED per window from `position` - the model's two cursors are
    /// the serve position and the lane's ahead cursor; there is no third to maintain.
    size_t serveRunAt(size_t pos_phys) const;

    /// The plan scheduler - the ONE entry to `observeAndSchedule`/`extendPlan`. Collects an in-flight
    /// machine sitting at the consumed plan end, then (re)plans per the caller's role:
    ///   - `coverage_ahead == 0` (the serve front): only a fully consumed plan replans (the
    ///     position before `plan_start`, or at `plan_end` with the plan not running to EOF) -
    ///     plans are used to their last byte, no pre-emptive look-ahead on the consumer side.
    ///   - `coverage_ahead > 0` (`prefetch`): replan when coverage does not reach
    ///     `position_phys + coverage_ahead`, so the ahead launch always has scheduled jobs -
    ///     the producer's deliberately pre-emptive look-ahead extension.
    void preparePlan(size_t position_phys, size_t coverage_ahead = 0);

    /// The shared post-serve tail of `readNextWindow` and THE consumer exit: account the
    /// served window, drop the fill pin at EOF, launch the next read-ahead, rebase the
    /// chain from physical to logical, and decrypt. Returns the plaintext window.
    ChainedBuffers finishWindow(ChainedBuffers chain);

    /// The CONSUMER's serve, serve-first: try the display, and only when nothing is
    /// deliverable heal (claimed-but-unreadable) or pump (a hit run has nothing to pump -
    /// its unservable window is EOF). `prefetch` launches the schedule's `Remote` jobs
    /// at the lane cursor.
    ChainedBuffers serveWindow(size_t position_phys);
    /// The next source piece of a populatable retrieve, off the schedule: the cell's
    /// append-only floor walked across the job's `fetch_runs`, grid-bounded to the window.
    /// Empty when no scheduled source byte lies past the cell frontier.
    ByteRange nextScheduledPiece(size_t ri, ByteRange window_phys) const;
    /// One bounded cache-blind source read of `window`, banked into the lane. The heal verb
    /// for state no planned job can produce (hung sibling leader; staled committed truth) -
    /// job-independent, so it heals hit runs too.
    bool bankDirectRead(ByteRange window);
    // ─── The display: the one state surface where execution results appear ─────────

    /// The DISPLAY: everything the plan can serve RIGHT NOW, with live progress - the union of
    /// the three byte holders, read through one surface:
    ///   - resident HIT views (the plan's pinned facts),
    ///   - committed CELLS (the writers' live `committed()` sets - they grow as an in-flight
    ///     worker streams, so the display shows the fill front's CURRENT progress),
    ///   - the BANK (bytes a piece fetched that no cell could hold: bypass gaps).
    /// The serve looks at the display and either takes ready bytes or advances a job; a job's
    /// data progress IS the display state. Foreground-only, plan-scoped (all state it reads
    /// dies at re-plan).
    class Display
    {
    public:
        Display(const ReadPlan & plan_, FillLane & lane_, const std::shared_ptr<FetchMachine> & machine_)
            : plan(plan_), lane(lane_), machine(machine_) {}

        /// What is servable of `window_phys` right now (union of the three holders).
        IntervalSet coverage(ByteRange window_phys) const;
        bool covers(ByteRange window_phys) const;
        /// The end of the contiguous servable run from `window_phys.offset`.
        size_t frontier(ByteRange window_phys) const;
        /// Serve the servable bytes of `window_phys` into `out`/`covered`, fastest holder
        /// first under the shared `covered` guard, preserving each holder's attribution
        /// (hit: `CacheGetRequests` + tier bytes + read-latency histogram; cell: the recredit
        /// semantics; bank: consume-and-trim, no cache counters - the bytes were counted at
        /// fetch). Bytes are PHYSICAL (pre-decryption-shift).
        void read(ByteRange window_phys, ChainedBuffers & out, IntervalSet & covered, Stats & out_stats);
        /// Wait on cells a LIVE disk-tier writer is filling and read them (dedup on our own
        /// worker's or a sibling executor's download). Page cells are filled by promotion,
        /// not downloaded - never waited on.
        void wait(ByteRange window_phys, ChainedBuffers & out, IntervalSet & covered);
        /// Coverage by the plan's held write buffers' committed ranges only (no read, no
        /// stats). A byte a SIBLING downloaded is NOT in this executor's per-writer
        /// committed set, so it reads as uncovered here - it enters the display only
        /// through the bank.
        IntervalSet committedCoverage(ByteRange window_phys) const;

    private:
        /// Is `phys` servable by ANY holder - the one-byte gate that keeps an empty `read`
        /// costless (the serve-first cycle probes by serving).
        bool coversByte(size_t phys) const;
        /// Re-credit any committed prefix a concurrent reader (or this plan's own write)
        /// has grown since plan-build, serving it from the held write buffer's own read.
        void recreditCommittedPrefixes(ByteRange window, ChainedBuffers & result,
            IntervalSet & covered, Stats & out_stats);
        const ReadPlan & plan;
        FillLane & lane;
        /// The in-flight machine (may be null): its published residue preview is
        /// the display's fourth holder.
        const std::shared_ptr<FetchMachine> & machine;
    };


    /// The end of the CONTIGUOUS committed run from `window_phys.offset` (== offset when nothing
    /// is committed there). Feeds `jobFrontier`: the committed-cell frontier is the
    /// producer's launch-progress measure for a job.
    size_t committedCellPrefixEnd(ByteRange window_phys) const;
    /// The display-derived DATA progress of job `ri`: the first byte of its `fetch_runs` not
    /// yet committed to the cells (`range.end()` when all are). The serve-side piece derivation
    /// works from the same display state, so stopping a piece anywhere IS the migration handoff.
    /// (Within one plan the per-writer committed set is monotone; an eviction is healed at the
    /// next re-plan, whose fresh writers start empty.) A BYPASS job has no cell; its frontier
    /// is the lane's ahead cursor clamped into the job, until the bank is virtualized.
    size_t jobFrontier(size_t ri) const;
    /// The background launch POLICY frontier: `jobFrontier` advanced past bytes already
    /// ATTEMPTED (launched over / served inline) that can never enter this executor's own
    /// committed set - a refused cell write or a sibling-downloaded segment. Used by the launch
    /// scan, the lead launch, and the Ready->Done transition; the serve never reads it.
    size_t launchProgress(size_t ri) const;
    /// The PRODUCER's demand step: one unit of progress toward serving `window`. HEAL first
    /// (a claimed-but-unreadable cursor is producible only cache-blind - the sole production
    /// a job-less hit run has); then, for a job: join an in-flight machine (own or foreign),
    /// wait a sibling's live cell and bank its bytes, or run one source piece as an INLINE
    /// machine (the collect pins, puts, and overflow-banks what the cells refuse), with one
    /// bounded cache-blind read as the hung-sibling last resort. FALSE = nothing left to
    /// produce for this window - the consumer reads that as this extent's EOF.
    bool pump(std::optional<size_t> ri, ByteRange window);
    /// Pump step 1: wait on live writers over `window`, bank sibling bytes the committed
    /// cells do not hold, advance the attempted cursor to the contiguous display frontier.
    /// TRUE = the display can now serve the window's first byte.
    bool waitSiblingFills(ByteRange window);
    /// Interrupt the in-flight machine (bounds the join to its next tile) and collect it.
    void interruptAndCollectMachine();
    /// Block until the machine's published residue covers `phys` or the worker exits
    /// (`publish_done`). TRUE = the display can serve the byte from the preview now.
    bool waitPublishedTile(FetchMachine & m, size_t phys);
    /// Serve the contiguous servable prefix of `window` off the display and run the scheduled
    /// handed fills from the served bytes. The serve tail shared by the hit step and the
    /// banked bypass step; physical like all serve verbs (`finishWindow` rebases to logical).
    ChainedBuffers serveFromDisplay(ByteRange window);
    /// THE collect verb for the in-flight machine (of its own `retrieve_index`): a
    /// still-queued step is revoked (the caller reads synchronously); a started/finished
    /// one is joined - reclaim its connection, pin at the fetch frontier, put-retry the
    /// refused residue, bank what is still homeless, and advance the lane's attempted
    /// cursor to the fetch reach.
    void collectInFlightInto();
    /// Build the machine's runner-independent fetch step (see the definition). Shared by the
    /// pool runner and the future inline runner.
    std::function<StepResult()> makeFetchStep(FetchMachine & m);
    /// Build a machine for `window` and run it via `machine_runner` (pool = async read-ahead,
    /// local = inline foreground fetch); sets `machine` on success. Returns false on a pool queue
    /// reject (the connection is reclaimed). The sole machine builder, shared by both runners.
    bool launchMachineForWindow(size_t ri, ByteRange window, IFetchMachineRunner & machine_runner);
    void launchRetrieve(size_t ri);

    /// Feed the plan SCHEDULE's predicted source reads (the `Source::Remote`
    /// retrieves, in offset order) into `fetch_tracker`; the tracker skips
    /// spans an earlier overlapping plan already fed. A Remote retrieve's range
    /// already spans bridged holes (<= `min_bytes_for_seek`) as over-read, and
    /// `bridgeable_gap == min_bytes_for_seek`, so feeding the range as one read counts
    /// that over-read exactly as a read-through would.
    void feedScheduleToFetchTracker(const PlanSchedule & schedule);

    /// TRIM phase of the plan: the look-ahead span starting at
    /// `physical_start`, clamped to the physical file end ONLY - the plan is
    /// independent of `read_bound` (which clamps sizing), so it
    /// survives mark-range advances. Exception: a transient's span is its
    /// bounded request. Empty when the start sits at/past a bound. The single
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

    /// EXTEND: grow the live plan forward to the span a fresh plan at
    /// `position_phys` would target, keeping the held buffers, the bank and the
    /// fill-lane state. Observes only `[plan_end, target)`; publishes a NEW
    /// geometry snapshot (copy-on-extend - the published snapshot stays
    /// immutable) and rebuilds the schedule - a pure function of the geometry -
    /// over the full extended span, with the launch frontier skipping jobs
    /// wholly behind the cursor. Requires a live plan and no machine in flight
    /// (the same barrier as a rebuild).
    void extendPlan(size_t position_phys);

    /// One object-piece of a residency observation: the per-tier probed views
    /// (hit readers + miss entries, positional with `caches`) and the fold's
    /// per-tier geometry. `covered_end` is where the walk actually stopped -
    /// the last resolution's true extent may overshoot the requested span, and
    /// the plan keeps that coverage (`plan_end` derives from it).
    struct PieceObservation
    {
        StoredObject object;
        size_t object_file_offset = 0;
        size_t covered_end = 0;
        VectorWithMemoryTracking<CacheViewPtr> views;
        VectorWithMemoryTracking<GeometryEntry> folded;
    };

    /// ONE read-only residency observation of `span`: per object-piece, the
    /// chain iterator probes every tier (fastest first) and a forward `lookAt`
    /// walk folds the per-position resolutions into the per-tier geometry.
    static VectorWithMemoryTracking<PieceObservation> observeSpan(
        const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & caches_,
        const OffsetMap & offset_map_,
        ByteRange span,
        const IntervalSet * request_map_ = nullptr,
        std::optional<size_t> demand_ceiling_phys = std::nullopt);

    /// The stored request map when it can shape planning, else nullptr.
    const IntervalSet * requestMapForPlanning() const
    {
        return request_map_set && !request_map.empty() ? &request_map : nullptr;
    }

    /// The read bound as the demand ceiling for tiling (PHYSICAL), when declared.
    std::optional<size_t> boundCeilingPhys() const
    {
        if (!read_bound)
            return std::nullopt;
        return toPhys(*read_bound);
    }

    /// How far contiguous DEMAND reaches from `phys_pos` under the request map:
    /// narrow map holes (< `min_bytes_for_seek`) are bridged - reading through
    /// is cheaper than a reopen - and a wide hole stops speculation. Unmapped:
    /// no clamp.
    size_t demandReachPhys(size_t phys_pos) const
    {
        if (!request_map_set || request_map.empty())
            return std::numeric_limits<size_t>::max();
        return std::max(request_map.contiguousEnd(phys_pos, min_bytes_for_seek), phys_pos);
    }

    /// The start of the contiguous demand run containing `phys_pos` (narrow
    /// holes bridged): fills must not walk BACK across a wide demand hole to
    /// the global frontier. Unmapped: no clamp (0).
    size_t demandFloorPhys(size_t phys_pos) const
    {
        if (!request_map_set || request_map.empty())
            return 0;
        return std::min(request_map.contiguousStart(phys_pos, min_bytes_for_seek), phys_pos);
    }

    /// Translate an observation into 1:1 `GeometryEntry`/`PlanTier` pairs
    /// (pushed BOTH-or-NEITHER, cache-major fastest-first): the fold's prune is
    /// applied to the held view before the writer upgrade opens the
    /// survivors in place (`[CF-plan-rebuild]`); a bypass tier keeps its probed
    /// miss entries untouched; records that are neither resident nor a
    /// populatable gap are dropped.
    static void emitObservation(
        const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & caches_,
        VectorWithMemoryTracking<PieceObservation> & pieces,
        CoverageMap & geom,
        VectorWithMemoryTracking<PlanTier> & tiers);

    /// Pin the partial segment under `frontier` from the first held write
    /// buffer whose `range` contains it and whose `pin` is non-null. Empty
    /// when nothing partial is there.
    bool frontierInPartial(size_t frontier) const;

    // ─── Machine lifecycle ───────────────────────────────────────────────

    /// Whether the single in-flight `machine` is currently serving retrieve `ri`. There is
    /// at most one machine at a time (the re-plan barrier asserts it), and it carries the
    /// `retrieve_index` it was launched for, so this is the "is a machine running for this
    /// retrieve" presence test the serve loop branches on.
    bool machineFor(size_t ri) const { return machine && machine->retrieve_index == ri; }

    /// Whether the worker has RELEASED the in-flight machine (its step left
    /// Scheduled/Running: products staged, `waitReleased` would not block). A non-blocking
    /// probe - only a collect decision; the collect itself still joins via the runner,
    /// which is what establishes the happens-before edge over the payload.
    bool machineReleased() const
    {
        const MachineState s = machine->state.load(std::memory_order_acquire);
        return s != MachineState::Scheduled && s != MachineState::Running;
    }

    /// The runner that drives the in-flight machine's revoke/release verbs at collect: the async
    /// (pool or fiber) runner when read-ahead launched it, else the inline runner (no async
    /// runner). Each runner no-ops on a machine it didn't schedule, but by its own mechanism:
    /// the pool runner branches on the machine's `current_step` (null when settled/never
    /// scheduled by it); the inline runner's verbs no-op unconditionally (the step already ran
    /// and settled inside `schedule`, so there is never a handle to join or cancel - it does not
    /// look at `current_step` at all); the fiber runner branches on its own `inflight` pointer
    /// instead (it never sets `current_step`).
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

    /// How far the in-order fill front runs ahead of the serve cursor: one uniform lead
    /// (`fill_ahead_lead`), self-limited by cell acceptance - a bottom that accepts nothing
    /// hits the worker's one-window residue cap and stops (see `coordinatedPrefetch`).
    /// Pressure suppresses prefetch upstream (`prefetchEnabled`), not the lead depth.
    size_t fillAheadLead() const;

    /// Shrink `win_size` so the read does not pass `read_bound`.
    /// Saturates to 0 once `position` reaches the extent (recoverable:
    /// extending the extent resumes).
    size_t clampToBound(size_t win_size) const;

    /// PRODUCER-side allowance: physical bytes a fetch may take from `phys_from`,
    /// bounded by the file end and by the read bound - see the definition for
    /// the past-extent rationale.
    size_t prefetchAllowance(size_t phys_from) const;

    /// `max(extent, reach)` - except for a `readBigAt` transient, whose extent IS
    /// its request and is never crossed. The single statement of the transient
    /// rule, shared by `prefetchAllowance` and `longConnectionBound`.

    /// The advertised read extent (`setReadUntilPosition`) has been reached - no room left
    /// within it, though the file may continue. `readNextWindow` uses this (not the file end)
    /// to gate the (re)plan once EOF is handled separately.

    bool atEnd() const
    {
        if (offset_map.hasUnknownSize())
            return reached_eof;
        return reached_eof || position >= totalSize();
    }

    /// CONSUMER-side horizon in LOGICAL bytes: the most a serve could return from `position`
    /// right now (the file remainder, or one window when the size is unknown, clamped to the
    /// extent); zero = the extent is exhausted. The producer's bound is `prefetchAllowance`.
    /// Deliberately does NOT test `reached_eof`: when EOF latches with a machine still in
    /// flight, `readNextWindow` drains that final window through `serveWindow`, which serves
    /// only while `readCeiling() > 0`.
    size_t readCeiling() const
    {
        return offset_map.hasUnknownSize() ? clampToBound(window_size) : clampToBound(totalSize() - position);
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
    /// Global encryption-header cache; null disables caching (url / non-disk reads).
    std::shared_ptr<EncryptionHeaderCache> encryption_header_cache;
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
    /// THE single read bound (LOGICAL): monotone max of the declared planned
    /// end (`setPlannedReadEnd` - the true end of the whole assignment), the
    /// advancing per-range extent (`setReadBound`), and a transient's request
    /// end. Caps sizing, prefetch and the long connection; never gates
    /// service (per-range EOF lives in `PipelineReadBuffer::read_until`).
    /// `nullopt` = read to the file end.
    std::optional<size_t> read_bound;


    /// The current look-ahead plan (source of truth; geometry snapshot null
    /// until the first plan is built).
    ReadPlan read_plan;

    /// Machines / pool.
    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    /// The async read-ahead driver: a pool runner over `prefetch_pool`, or a Silk fiber runner
    /// when `Options::use_fiber_runner` is set; state writes, scheduling and the revoke/release
    /// edges live there; every policy decision stays here. Created in the constructor by
    /// `makeReadAheadRunner`; null when neither a pool nor the fiber runner was requested.
    /// Drives the read-ahead FETCH machines only.
    std::unique_ptr<IFetchMachineRunner> runner;
    /// Inline driver, always present: runs a foreground serve machine synchronously on the read
    /// thread. Also the fallback collect-runner when there is no async runner - its verbs no-op
    /// on a settled inline machine (null `current_step`).
    std::unique_ptr<IFetchMachineRunner> local_runner;
    /// The producer's lane (see `ReaderExecutorFillLane.h`): owns the long
    /// connection, the in-flight pin, the ahead cursor and the bank.
    FillLane fill_lane;
    /// The display (see the class doc): holds only back-references, safe to initialize here.
    Display display{read_plan, fill_lane, machine};
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

    /// Server-wide long-connection limit handle, shared with
    /// `makeTransientForReadAt`. Gates long-connection opens (the
    /// `reader_executor_use_long_connections` setting).
    std::shared_ptr<LongConnectionLimit> long_connection_limit;

    /// The stored request map (PHYSICAL intervals - converted at the feed;
    /// empty = absent = whole file). See `setRequestMap`.
    IntervalSet request_map;
    bool request_map_set = false;

    /// FETCH-trajectory estimator, fed each plan's predicted source reads and every
    /// seek. Constructed with `bridgeable_gap == min_bytes_for_seek` so a bridged gap
    /// counts identically whether modeled as a read-through or a seek.
    /// `predictedEnd` sizes the long source connection (see `longConnectionBound`).
    ReadContinuityTracker fetch_tracker;

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
