#pragma once

#include <Columns/ColumnNullable.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/DuplicateSpans.h>
#include <Interpreters/PartitionedHashJoin/SharedJoinTable.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

namespace DB
{

class TableJoin;

/** Partitioned hash join (`join_algorithm = 'partitioned_hash'`).
  *
  * `parallel_hash` probes one shared map, so once the build side outgrows last-level cache every
  * lookup is a cold miss. This build keeps ONE hash table for the whole right side but partitions its
  * construction: the cell buffer is split into `2^bits` contiguous ranges, a partition is the set of
  * build rows whose home cell lies in its range, and each range is filled by one worker while it stays
  * cache-resident. The probe side is never partitioned - a probe row hashes once and walks the one table
  * - so transient memory does not scale with probe cardinality and rows flow downstream immediately.
  *
  * The phases:
  *
  * - Fill accumulates right-side blocks per lane untouched. Per row it computes the map hash, saves the
  *   top 16 bits of the mixed hash as the row's route, and feeds a per-lane sketch. Nothing is inserted.
  * - The build barrier merges the sketches, sizes the table at the standard 50% max fill, and picks
  *   the partition count: the smallest power of two whose range fits private L2, at least one range per
  *   worker. The route's top `bits` name a row's partition, and by construction its home cell lies in
  *   that partition's range. The table may later double in place when a wrapping insert would take the
  *   last empty cell, or at a quiescent point when the fill would exceed 50%.
  * - Post-build scatters only the key columns plus a row locator into per-partition chunks (payload
  *   stays in the shared row store), then workers claim partitions largest-first and insert: a walk that
  *   stops at the range end, a private overflow buffer for the rows that would cross it, and after the
  *   barrier one thread drains the overflow with the global mask and wraparound. Duplicates of a key are
  *   stored inline, as an exact run, or as a newest-first chain of ranges (`SpanWriter`).
  * - Probe hashes each row once and walks the table from its home cell. Above the engagement threshold
  *   this runs as two passes per block - an AMAC find ring out of order, then an in-order pass over its
  *   results - and below it as the plain loop. Either way the emit, replication offsets, used flags and
  *   per-kind logic are the standard `HashJoin` machinery.
  *
  * Used flags are one per-offset space of `cells + 1` entries (offset 0 is the zero-value cell), exactly
  * the single-map layout `JoinUsedFlags` and the non-joined iteration expect.
  *
  * Shapes whose flags must be keyed per right-table row rather than per cell - multiple disjuncts -
  * run the standard `HashJoin` whole behind this interface. That regime is partition-agnostic and
  * rare, so it is not worth a partitioned build.
  */
class PartitionedHashJoin : public IJoin
{
public:
    PartitionedHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        size_t num_threads_,
        bool any_take_last_row_ = false,
        const StatsCollectingParams & stats_collecting_params_ = {},
        size_t max_bytes_before_external_join_ = 0);

    ~PartitionedHashJoin() override;

    /// Shapes outside this predicate must be planned onto another enabled algorithm rather than
    /// failing at execution time; see `tryCreateJoin` in `Planner/PlannerJoins.cpp`.
    static bool isSupported(const TableJoin & table_join);

    std::string getName() const override { return "PartitionedHashJoin"; }
    const TableJoin & getTableJoin() const override;

    bool addBlockToJoin(const Block & block, bool check_limits) override;
    bool addBlockToJoin(const Block & block, size_t num_rows, bool check_limits, size_t build_lane) override;
    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;
    JoinResultPtr joinBlock(Block block, size_t lane) override;

    /// Every parallel fill stream reports totals at its end-of-fill, so unlike the base class's
    /// unsynchronized default these need a guard - as in `ConcurrentHashJoin`.
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;

    /// The peak this build is heading for if every accumulated row ends up in the table: the row
    /// store and route words that are already allocated, plus the table and arena that are not yet.
    /// `SpillingHashJoin` compares this against the external-join threshold. Not `getTotalByteCount`,
    /// which is the currently allocated amount and feeds `max_bytes_in_join` and `EXPLAIN`.
    size_t predictedResidentBytes() const;

    /// Bytes the stored rows would take once loaded into a single in-memory join: the row store as it
    /// stands plus the ungrouped table and arena prediction from the barrier's exact totals. On the
    /// `MustSpill` path `SpillingHashJoin` divides this by the grace per-bucket cap to pick the initial
    /// bucket count, instead of letting `GraceHashJoin` discover it through 1 -> 2 -> 4 rehashes.
    size_t graceInMemoryEstimateBytes() const;

    StepAnalysisReport getAnalysisReport() const override;
    bool alwaysReturnsEmptySet() const override;

    /// The fill is per-lane plus a short mutexed append, so right-side streams may fill
    /// concurrently. The delegated path inserts into one `HashJoin`, which is not thread-safe.
    bool supportParallelJoin() const override { return !delegate_mode; }

    void onBuildPhaseFinish() override;
    bool hasPostBuildPhase() const override { return true; }
    void runPostBuildPhase() override;

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// The table's cells are independent, so the non-joined scan splits them into `num_streams`
    /// contiguous position ranges; stream 0 also emits the zero-value cell and the null-key rows. The
    /// delegated path stays single-stream, because `HashJoin` does not advertise the parallel regime.
    bool supportParallelNonJoinedBlocksProcessing() const override;

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block,
        const Block & result_sample_block,
        UInt64 max_block_size,
        size_t stream_idx,
        size_t num_streams) const override;

    bool isCloneSupported() const override;

    std::shared_ptr<IJoin>
    clone(const std::shared_ptr<TableJoin> & table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_) const override;

    std::shared_ptr<IJoin> cloneNoParallel(
        const std::shared_ptr<TableJoin> & table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_) const override;

    void setEnableLazyColumnsIndexing(bool value) override;

    /// What the tests assert the build on: the plan, the table geometry, the sketch, the ownership
    /// protocol's overflow and the duplicate layout.
    struct BuildStats
    {
        size_t bits = 0;
        size_t partitions = 0;
        /// MSB-first radix bits per scatter pass; more than one when the plan wants a fanout above a
        /// single pass's ceiling.
        std::vector<size_t> pass_bits;
        /// Final per-partition insertable row counts, so tests can assert two pass plans agree.
        std::vector<UInt64> partition_row_counts;
        double hll_estimate = 0;
        /// The one table: its buffer degree, cells and bytes. `predictions_exact` says the created
        /// buffer matched the plan's prediction.
        size_t table_size_degree = 0;
        size_t table_cells = 0;
        size_t ht_total_bytes = 0;
        bool predictions_exact = true;
        bool amac_build_engaged = false;
        /// Rows fed to the inserts, distinct keys the table ended with, and rows the owner walks handed to
        /// the serial drain because they reached their range end.
        UInt64 inserted_rows = 0;
        UInt64 distinct_keys = 0;
        UInt64 overflow_rows = 0;
        /// Distinct keys the drain claimed and rows it appended to keys the owners had already stored.
        UInt64 drain_claimed_keys = 0;
        UInt64 drain_appended_rows = 0;
        /// Duplicate storage written by the owner waves and by the drain.
        SpanWriter::Stats owner_duplicates;
        SpanWriter::Stats drain_duplicates;
        /// Contiguous build-block ranges the post-build scatter was split into. 1 means the whole
        /// build was scattered at once.
        size_t scatter_groups = 1;
        struct BlockRange
        {
            size_t begin = 0;
            size_t end = 0;
            /// Bytes the group was sized with (`chunkBytesForBlockRange` and the resident set at that
            /// moment). Zero when the plan did not size groups against a budget.
            size_t chunk_bytes = 0;
            size_t used_bytes = 0;
            /// `chunkBytesForBlockRange(begin, begin + 1)` at sizing time: the one-block overshoot
            /// R2.10 reserves at the previous boundary, and the bound `GroupSizedAfterGrowth` checks.
            size_t one_block_chunk_bytes = 0;
            /// `residentBytes` at the start of sizing, before predicted arena and uncommitted table
            /// are added. `GroupSizedAfterGrowth` bounds this plus the chunk.
            size_t resident_bytes = 0;
        };
        std::vector<BlockRange> scatter_group_ranges;
        /// Per-partition claimed buffer cells at publication, excluding the zero cell.
        std::vector<UInt64> claimed_per_partition;
        /// Peak logical occupancy of any one pass scratch (`PassScratch::usedBytes`).
        size_t scratch_used_high_water = 0;
        /// Table growth (section 4). Zero until a grow runs; `load_factor_grow_skipped` counts G2
        /// refusals under budget.
        UInt64 table_resizes = 0;
        UInt64 load_factor_grow_skipped = 0;
    };

    BuildStats getBuildStats() const;

    /// Shrinks the reserve safety factor so the table is undersized. Growth then restores the fill,
    /// or a G1 refusal throws `LOGICAL_ERROR` when the budget cannot pay for the doubling.
    void setReserveSafetyFactorForTests(double factor) { reserve_safety = factor; }
    void setReserveOverrideForTests(size_t reserve) { reserve_override_for_tests = reserve; }
    void setGrowBudgetForTests(size_t bytes) { grow_budget = bytes; }
    void setBeforeDrainHookForTests(std::function<void()> hook) { before_drain_hook_for_tests = std::move(hook); }
    /// G1: wrapping inserts call this before the walk of every row (11.3 `beforeWalk`).
    template <typename Target>
    bool g1BeforeClaim(Target & target);

    /// Boundary G2 projection (F7): the sketch term while the exact count is inside its safety band;
    /// once the count has passed the band, the max of the count and its linear extrapolation.
    static UInt64
    boundaryProjection(UInt64 claimed_total, UInt64 rows_inserted, UInt64 insertable, double hll_estimate, double reserve_safety);

    /// Pins both phases onto the sequential loops, so tests can cross-check the ring against them.
    void setAmacEnabledForTests(bool value) { amac_enabled = value; }

    /// Lowers the per-pass fanout ceiling, so the refine passes can be tested without a 500M-key
    /// build.
    void setMaxFanoutPerPassForTests(size_t value) { max_fanout_per_pass = value; }

    /// The descriptor cap binds only past a few hundred leaves, a build too large for a unit test.
    /// A tiny L1 makes it bind on a small one; the cap itself can be switched off for the control.
    void setL1CacheSizeForTests(size_t bytes) { l1_cache_bytes_for_tests = bytes; }
    void setCapPartitionsByL1DescriptorsForTests(bool value) { cap_partitions_by_l1_descriptors = value; }

    /// Forces the partition count (clamped to the table's degree), so plans of thousands of partitions
    /// can be executed on a build that fits a test.
    void setPartitionBitsForTests(size_t value) { forced_bits_for_tests = value; }

    /// The post-build memory verdict, taken once at the barrier from numbers that already exist.
    enum class PostBuildPlan
    {
        Fits, /// ungrouped scatter
        Grouped, /// in-memory scatter over block ranges
        MustSpill, /// even the resident data does not fit; the caller must switch to grace
    };
    PostBuildPlan planPostBuild();

    /// Terms `planPostBuild` used for the grouped/ungrouped verdict. Filled only on the partitioned
    /// path (`bits > 0` and a non-zero budget). `groups_est` is computed from the ungrouped floor
    /// (no header term), then passed into the grouped arena prediction.
    struct PostBuildGateTerms
    {
        size_t floor_bytes = 0;
        size_t floor_bytes_grouped = 0;
        size_t tables = 0;
        size_t chunk_all = 0;
        size_t groups_est = 1;
        size_t peak_ungrouped = 0;
        size_t grouped_floor = 0;
    };
    PostBuildGateTerms getPostBuildGateTermsForTests() const { return gate_terms; }
    size_t predictedArenaBytesForTests(bool grouped) const;
    size_t predictedDuplicateScratchBytesForTests(size_t rows_in_range, bool first_group) const;

    size_t getNumFillLanes() const;
    /// Drops per-block fill transients that GraceHashJoin re-derives from the stored block. Call
    /// once the switch is decided, before the drain, so they are not still allocated while grace
    /// is also allocating.
    void dropFillAuxiliary();
    /// Pops one stored block from `lane`. An empty Block means the lane is exhausted.
    Block releaseNextFillLaneBlock(size_t lane);
    /// Clears barrier transients so `releaseNextStoredBlock` can drain the row store one block at a
    /// time. After this the instance is only a source of stored blocks.
    void beginStoredBlockDrain();
    /// Pops one row-store block. An empty Block means the row store is gone.
    Block releaseNextStoredBlock();
    /// Feeds every remaining row-store block to `target` from up to `num_threads` workers. Call after
    /// `beginStoredBlockDrain`; `target.addBlockToJoin` must accept concurrent callers, which
    /// `GraceHashJoin` does.
    void drainStoredBlocksInto(IJoin & target);

private:
    friend class NotJoinedPartitioned;

    /// `HashJoin::data` is private and the non-joined filler is a friend of this class, not of it.
    const HashJoin::RightTableData & storedData() const { return *leaf_join->data; }

    /// One accumulated right-side block: the payload in row-store form, the prepared key columns,
    /// and the saved routes.
    struct FillBlock
    {
        Block stored;
        Columns keys_holder;
        ColumnRawPtrs key_columns;
        ColumnPtr null_map_holder;
        ConstNullMapPtr null_map = nullptr;
        /// The clause's right-side ON condition; rows it filters are not inserted, as in the
        /// standard build.
        JoinCommon::JoinMask join_mask;
        /// Null-key rows OR mask-filtered rows, materialized only when the mask actually filters -
        /// otherwise `skipData` returns the plain null map.
        PaddedPODArray<UInt8> skip_bytes;
        PaddedPODArray<UInt16> routes;
        size_t rows = 0;
        UInt32 block_no = 0; /// assigned at the build barrier

        const UInt8 * skipData() const
        {
            if (!skip_bytes.empty())
                return skip_bytes.data();
            return null_map ? null_map->data() : nullptr;
        }
    };

    /// One per fill thread, so appends and sketch updates never contend.
    struct FillLane
    {
        std::vector<FillBlock> blocks;
        DenseHyperLogLog hll;
    };

    /// Shared across the post-build stages: histogram, allocate, scatter, owner waves, drain.
    struct PostBuildContext;
    /// Out-of-line so `unique_ptr<PostBuildContext>` can be destroyed from TUs that only see the
    /// forward declaration (the constructor of this class lives in `PartitionedHashJoin.cpp`).
    struct PostBuildContextDeleter
    {
        void operator()(PostBuildContext * ctx) const;
    };

    FillLane & getFillLane();
    FillLane & getFillLane(size_t build_lane);
    bool addBlockToJoinImpl(const Block & source_block, bool check_limits, size_t build_lane);
    void decidePartitionPlan();
    void storeBlocksInRowStore();

    /// Both return whether every inserted key was unique, which drives the RightAny promotion.
    bool postBuildPartitioned();
    bool postBuildSinglePartition();
    void preparePostBuildContext();
    void runGroupStages(size_t block_begin, size_t block_end);
    size_t chunkBytesForBlockRange(size_t b0, size_t b1) const;

    /// Bytes the table and the duplicate storage will need for `rows` build rows holding `distinct`
    /// distinct keys. The post-build gate evaluates this with exact counts; the fill evaluates it with
    /// the running sketch estimate. `groups_est` is 1 for the ungrouped call and for the fill-phase
    /// gate; the grouped call receives the value `planPostBuild` computed from the ungrouped floor.
    size_t predictedTableAndArenaBytes(size_t rows, size_t distinct, bool grouped, size_t groups_est = 1) const;
    size_t predictedArenaBytes(size_t insertable_rows, bool grouped) const;
    size_t duplicateScratchBytesForRows(size_t rows_in_range, bool first_group) const;
    /// The scratch a block range needs at once: `workers` live partitions plus the drain's.
    size_t duplicateScratchBytesForRange(size_t rows_in_range, bool first_group) const;
    /// The table reserve the plan derives from a distinct estimate: safety factor, row clamp, and the
    /// saturation clamp above `2^31` estimated words.
    size_t reserveFor(size_t rows, double distinct_estimate) const;

    size_t liveDistinctEstimate() const;

    void measureGenericKeyBytes();
    void createSharedTable();
    void reduceWorkerHistogram();
    void resetWorkerHistogram(PostBuildContext & ctx);
    void histogramWorker(PostBuildContext & ctx, size_t worker) const;
    void allocateWorker(PostBuildContext & ctx, size_t worker) const;
    void scatterWorker(PostBuildContext & ctx, size_t worker);

    /// Splits every current bucket into `2^refine_bits` sub-buckets by the next route-word slice
    /// below the `bits_done` earlier passes consumed, group-major. After the last pass a row's
    /// partition is `route >> (16 - bits)` - the same partition a single-pass plan would give it.
    void refinePassWave(PostBuildContext & ctx, size_t refine_bits, size_t bits_done, std::atomic<UInt64> & stage_thread_us);

    /// The owner wave: workers claim partitions largest-first and fill their ranges (section 4.3 of
    /// the design); then the capacity guard and the serial drain of every partition's overflow.
    void ownerWaveWorker(PostBuildContext & ctx, size_t worker);
    bool tableHasZero() const;
    UInt64 claimedBufferCells() const;
    UInt64 claimedTotal() const;
    void drainOverflow(PostBuildContext & ctx);
    enum class GrowReason : UInt8
    {
        G1,
        G2,
    };
    size_t residentBytes() const;
    size_t liveChunkBytes() const;
    void grow(UInt64 occupied, UInt64 projected, GrowReason reason, size_t extra_reserved = 0);
    template <typename Table>
    void growSharedTable(Table & table, UInt64 occupied, UInt64 projected, GrowReason reason, size_t extra_reserved = 0);
    void maybeGrowForLoadFactor(UInt64 projected, size_t extra_reserved = 0);
    /// Finishes one pass's scratch into spans. Returns the scratch's logical occupancy just before
    /// the finish (0 when the scratch was empty), so callers can fold a per-worker high water.
    size_t finishPassScratch(PassScratch & scratch, SpanWriter & writer);
    template <typename Table>
    void verifyPublishedTable(const Table & table) const;
    /// Accounts and pre-faults one partition's cell range (see `RangeCommittedBuffer`).
    void commitRange(size_t partition);
    /// Sets the table's distinct-key count from the owners' and the drain's claims.
    void publishTableSize(const PostBuildContext & ctx);
    void finishBuildPhase(bool all_values_unique);

    /// Inserts one compact section of `rows` rows into partition `partition`'s range on behalf of
    /// `worker`, or - when `partition` is `single_partition` - into the whole table from the stored
    /// blocks, which is the only path where `skip_bytes` applies. Row i's stored ref is `locators[i]`,
    /// the decoded `narrow_locators[i]`, or `RowRef(block_no, i)` when neither is set.
    static constexpr size_t single_partition = std::numeric_limits<size_t>::max();
    void insertPartitionSection(
        PostBuildContext & ctx,
        size_t worker,
        size_t partition,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        const UInt64 * locators,
        const UInt32 * narrow_locators_data,
        UInt32 block_no,
        const UInt8 * skip_bytes);

    /// Sizes the flag space to `cells + 1` for the shapes that keep right-side flags.
    void reinitUsedFlags();

    /// Decided once, after the table is sized and before the inserts, on the same heuristics that
    /// enable the software prefetch.
    void decideAmacEngagement();

    /// `MapsShape` is the standard shape the (kind, strictness) pair dispatches to; the shared table is
    /// its partitioned counterpart, holding identical cells.
    JoinResultPtr probeDispatch(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape>
    JoinResultPtr probeImpl(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType>
    size_t sharedJoinRightColumns(const Map & table, AddedColumnsType & added_columns, const ScatteredBlock & block, size_t lane);

    /// Per-probe-stream scratch, pooled on the join and reused across blocks: the find pass's results.
    /// `found_word` holds the matched cell's mapped value by value - a `RowRef` or `RowRefList` is an
    /// 8-byte word that is never 0 for a match, so 0 encodes a miss - which is what keeps the second pass
    /// from touching the cell again after it has left the cache. ASOF does not fit a word and stores the
    /// mapped pointer's bits instead. `found_offset` is the used-flags offset.
    struct ProbeScratch
    {
        PaddedPODArray<UInt64> found_word;
        PaddedPODArray<UInt64> found_offset;
    };

    /// The pipeline-carried lane index binds one lock-free slot per probe stream. Lanes outside the
    /// table, and the lane-less entry points, fall back to the mutexed pool - correct, just slower.
    static constexpr size_t invalid_lane = std::numeric_limits<size_t>::max();

    std::unique_ptr<ProbeScratch> acquireProbeScratch(size_t lane);
    void releaseProbeScratch(std::unique_ptr<ProbeScratch> scratch, size_t lane);

    std::shared_ptr<TableJoin> table_join;
    SharedHeader right_sample_block;
    const bool any_take_last_row;
    const size_t num_threads;
    /// Zero disables the gate; post-build is the ungrouped scatter.
    const size_t max_bytes_before_external_join;
    /// Same as the constructor budget unless a test lifts or tightens it for a G1/G2 refusal.
    size_t grow_budget = 0;
    std::optional<size_t> reserve_override_for_tests;
    std::function<void()> before_drain_hook_for_tests;

    /// Owns everything the emit machinery needs: block preparation, the saved block sample, the
    /// shared row store, the used flags, the output samples. Its own map stays empty and the shared
    /// table replaces it - except on the delegated path, where it runs the join whole.
    std::unique_ptr<HashJoin> leaf_join;

    /// Set for the shapes that need per-row used flags; see the class comment.
    const bool delegate_mode;

    /// Which `HashJoin::MapsVariant` alternative is active; the shared table mirrors it.
    const size_t maps_variant_index;

    /// `IJoin::totals` is private, so the guarded overrides keep their own copy.
    std::mutex totals_mutex;
    Block totals;

    /// `lanes` owns the per-lane state and the barrier iterates it. The slot table resolves a
    /// pipeline-carried lane index without a lock: one mutexed emplace on a lane's first block, then
    /// atomic loads. It is sized once and never resized, so the fast path cannot race a rehash.
    /// Lane-less callers keep the thread-id map.
    /// Mutable because `predictedResidentBytes` is a `const` query that still has to refresh the
    /// cached distinct estimate under this lock. Shared with the per-lane sketch `add`, exclusive
    /// for the merge: a torn register would persist into the barrier's estimate.
    mutable SharedMutex fill_mutex;
    std::deque<FillLane> lanes;
    std::unordered_map<std::thread::id, FillLane *> lane_by_thread;
    std::vector<std::atomic<FillLane *>> fill_lane_slots;
    std::atomic<size_t> accumulated_rows{0};
    std::atomic<size_t> accumulated_bytes{0};

    /// Fill-phase distinct estimate for `predictedResidentBytes`. Merging every lane on every block
    /// would cost `lanes * 8 KiB`, so the value is reused until the row count has grown by a
    /// sixteenth. A slightly stale value only delays the switch by one refresh interval.
    mutable std::atomic<size_t> cached_distinct_estimate{0};
    mutable std::atomic<size_t> distinct_estimate_at_rows{0};

    size_t bits = 0;
    size_t partitions = 1;
    /// MSB-first slices of the route word, summing to `bits`.
    std::vector<size_t> pass_bits;
    /// `partitioned_hash_join_max_fanout_per_pass`; tests lower it to force refine passes.
    size_t max_fanout_per_pass;
    /// `partitioned_hash_join_cap_partitions_by_l1_descriptors`.
    bool cap_partitions_by_l1_descriptors;
    std::optional<size_t> l1_cache_bytes_for_tests;
    std::optional<size_t> forced_bits_for_tests;
    double hll_estimate = 0;
    double reserve_safety = 1.2; /// covers the sketch error (~1.15% at precision 13)
    /// The table's buffer degree, fixed at the barrier: `2^size_degree` cells, `2^bits` ranges.
    size_t size_degree = 0;
    /// Cross-run distinct-key statistics are published for join reordering and the runtime filters,
    /// never consumed for sizing: the table cannot grow, and a cached count is data-independent.
    StatsCollectingParams stats_collecting_params;
    std::vector<FillBlock> build_blocks; /// concatenated lanes, row-store block numbers assigned
    /// When every block and row number fits 16 bits the scattered locator column packs into
    /// `(block_no << 16) | row_no` and is decoded at insert, halving the largest scatter transient.
    bool narrow_locators = false;

    /// The one table. `build_arenas` hold the string keys and the duplicate spans the cells point at,
    /// so they must outlive it: one arena per build worker plus one for the drain.
    std::unique_ptr<SharedJoinMaps> shared_maps;
    std::deque<Arena> build_arenas;
    size_t ht_total_bytes = 0; /// the table's buffer bytes (drives the prefetch heuristics)

    std::unique_ptr<ThreadPool> post_build_pool;
    std::unique_ptr<PostBuildContext, PostBuildContextDeleter> post_build_ctx;
    /// Exact per-partition insertable row counts from the full-build histogram.
    std::vector<UInt64> total_bucket_rows;
    /// Prepared key-column bytes across the whole build, measured once at the gate. Zero unless
    /// the keys are variable-length, which is when they are copied into the arena.
    size_t generic_key_bytes = 0;
    PostBuildPlan post_build_plan = PostBuildPlan::Fits;
    /// Number of groups the budget implies, from the ungrouped floor (section 5.1 / 11.7). 1 until
    /// `planPostBuild` computes it, and 1 on the fill-phase and ungrouped paths.
    size_t groups_est = 1;
    PostBuildGateTerms gate_terms;
    /// After `beginStoredBlockDrain` the row store is being drained and this instance must not be
    /// used except for `releaseNextStoredBlock`.
    bool stored_blocks_released = false;
    /// Set by `preparePostBuildContext` when the histogram already covers the whole build at the
    /// pass-1 width, so the ungrouped path does not scan the routes twice.
    bool histogram_covers_full_build = false;

    bool build_phase_finished = false;

    /// The test override and the engagement decision taken before the owner wave.
    bool amac_enabled = true;
    bool amac_build_engaged = false;

    std::mutex probe_scratch_mutex;
    std::vector<std::unique_ptr<ProbeScratch>> probe_scratch_pool;
    /// One parked scratch per probe lane, owned when non-null. Acquire exchanges it out, release
    /// CASes it back; a miss goes through the pool.
    std::vector<std::atomic<ProbeScratch *>> probe_scratch_slots;

    BuildStats stats;

    LoggerPtr log;
};

}
