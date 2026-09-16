#pragma once

#include <Columns/ColumnNullable.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/DuplicateSpans.h>
#include <Interpreters/PartitionedHashJoin/HashJoinTable.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <deque>
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
  * `parallel_hash` probes one shared map; once the build side outgrows the last-level cache, every
  * lookup is a cold miss. This join keeps ONE hash table for the whole right side but builds it in
  * partitions: the cell buffer is split into `2^bits` contiguous ranges, a partition is the set of
  * build rows whose home cell lies in one range, and one worker fills each range while it stays
  * cache-resident. The probe side is never partitioned. A probe row hashes once and walks the one
  * table, so transient memory does not scale with probe cardinality and rows flow downstream
  * immediately.
  *
  * The phases:
  *
  * - Fill accumulates right-side blocks per lane untouched. Per row it computes the map hash, saves
  *   the top 16 bits of its placement word (`hashJoinTablePlacement`) as the row's route, and feeds a
  *   per-lane sketch (a HyperLogLog) with a mix of the hash. Nothing is inserted.
  * - The build barrier merges the sketches, sizes the table at the standard 50% max fill, and picks
  *   the partition count: the smallest power of two whose range fits private L2, at least one range per
  *   worker. The route's top `bits` name a row's partition, and by construction its home cell lies in
  *   that partition's range. The table may later double in place when a wrapping insert would take the
  *   last empty cell, or between waves when the projected fill would exceed 50%.
  * - Post-build scatters only the key columns plus a row locator into per-partition chunks; the
  *   payload stays in the shared row store. Workers claim partitions largest-first and insert. A walk
  *   stops at its range end; rows that would cross it go to a private overflow buffer, and after the
  *   barrier one thread drains the overflow with the global mask and wraparound. Duplicates of a key
  *   are stored inline, as an exact span, or as a newest-first chain of spans (`SpanWriter`).
  * - Probe hashes each row once and walks the table from its home cell. Above the prefetch threshold
  *   this runs as two passes per block - an AMAC find ring out of order, then an in-order pass over its
  *   results - and below it as the plain loop. AMAC (asynchronous memory access chaining) keeps a ring
  *   of in-flight lookups whose cache misses overlap. Either way the emit, replication offsets, used
  *   flags and per-kind logic are the standard `HashJoin` machinery.
  *
  * Used flags are one per-offset space of `cells + 1` entries (offset 0 is the zero-value cell), exactly
  * the single-map layout `JoinUsedFlags` and the non-joined iteration expect.
  *
  * Joins whose flags must be keyed per right-table row rather than per cell - multiple disjuncts - run
  * the standard `HashJoin` whole behind this interface (`delegate_mode`). That case does not depend on
  * partitioning and is rare, so it is not worth a partitioned build.
  */
class PartitionedHashJoin : public IJoin
{
public:
    /// `build_rows_hint_` is the planner's right-side row estimate, when it has one. Below
    /// `parallel_hash_join_threshold` the join builds on one fill thread: the pipeline keeps the
    /// `hash` shape, the table is sized from the hint and grows like `hash`'s, and every block is
    /// inserted as it arrives, so nothing is left for the barrier.
    PartitionedHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        size_t num_threads_,
        bool any_take_last_row_ = false,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        size_t max_bytes_before_external_join_ = 0,
        std::optional<size_t> build_rows_hint_ = {});

    ~PartitionedHashJoin() override;

    /// Shapes outside this predicate must be planned onto another enabled algorithm rather than
    /// failing at execution time; see `tryCreateJoin` in `Planner/PlannerJoins.cpp`.
    static bool isSupported(const TableJoin & table_join);

    std::string getName() const override { return "PartitionedHashJoin"; }
    const TableJoin & getTableJoin() const override;

    /// `worker_id` indexes the fill lanes; an id past the lane table takes the thread-keyed lane.
    bool addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;
    JoinResultPtr joinBlock(Block block, size_t lane) override;

    /// Every parallel fill stream reports totals at its end-of-fill, so unlike the base class's
    /// unsynchronized default these need a guard, as the parallel `HashJoin` layout has.
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;

    StepAnalysisReport getAnalysisReport() const override;
    bool alwaysReturnsEmptySet() const override;

    /// The fill is per-lane plus a short mutexed append, so right-side streams may fill
    /// concurrently. The delegated path inserts into one `HashJoin`, which is not thread-safe, and
    /// a build estimated small keeps the narrow pipeline on purpose.
    bool supportParallelJoin() const override { return !delegate_mode && !single_fill_thread; }
    /// Probe blocks are joined whole, never scattered across slots, and the result caps its own blocks.
    bool emitsSizedOutputBlocks() const override { return true; }

    void onBuildPhaseFinish() override;
    bool hasPostBuildPhase() const override { return true; }
    void runPostBuildPhase() override;

    /// The planner reads the matched count of the previous run to decide on the row store, so it is
    /// published at destruction as the other hash joins publish theirs.
    void onProbePhaseFinish(std::optional<size_t> matched_right_rows) override
    {
        hash_table_matches = matched_right_rows;
        probe_phase_finished = true;
    }

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

    /// Counters and geometry of one build: the plan, the table, the sketch, the overflow drained and
    /// the duplicate storage. Read after `runPostBuildPhase`.
    struct BuildStats
    {
        size_t bits = 0;
        size_t partitions = 0;
        /// MSB-first radix bits per scatter pass; more than one when the plan wants a fanout above a
        /// single pass's ceiling.
        std::vector<size_t> pass_bits;
        /// Final per-partition insertable row counts.
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
            /// The chunk this range was sized with (`chunkBytesForBlockRange`). Zero when the plan did
            /// not size ranges against a budget.
            size_t chunk_bytes = 0;
            /// `chunkBytesForBlockRange(begin, begin + 1)` at sizing time: the one block by which a range
            /// may overshoot the headroom, since a range is never empty.
            size_t one_block_chunk_bytes = 0;
            /// `residentBytes` when the range was sized, before the predicted arena and the uncommitted
            /// table were added. `resident_bytes + chunk_bytes` must stay within the budget plus one block.
            size_t resident_bytes = 0;
        };
        std::vector<BlockRange> scatter_group_ranges;
        /// Per-partition claimed buffer cells at publication, excluding the zero cell.
        std::vector<UInt64> claimed_per_partition;
        /// Peak logical occupancy of any one pass scratch (`PassScratch::usedBytes`).
        size_t scratch_used_high_water = 0;
        /// Table growth. Zero until a grow runs; `load_factor_grow_skipped` counts the load-factor grows
        /// refused under the budget.
        UInt64 table_resizes = 0;
        UInt64 load_factor_grow_skipped = 0;
        /// Stored blocks whose fixed-width payload went into a row store.
        UInt64 row_store_blocks = 0;
    };

    BuildStats getBuildStats() const;

    /// Shrinks the reserve safety factor so the table is undersized. Growth then restores the fill, or
    /// the grow at the last free cell throws `LOGICAL_ERROR` when the budget cannot pay for the doubling.
    void setReserveSafetyFactorForTests(double factor) { reserve_safety = factor; }
    void setReserveOverrideForTests(size_t reserve) { reserve_override_for_tests = reserve; }
    void setGrowBudgetForTests(size_t bytes) { grow_budget = bytes; }
    /// The grow budget from the overflow drain on; a test uses it to lift the budget for the drain only.
    void setGrowBudgetForDrainForTests(size_t bytes) { grow_budget_for_drain_for_tests = bytes; }

    /// Projection at a block-range boundary: the sketch estimate while the exact count is inside its
    /// safety band; once the count has passed the band, the larger of the count and its linear
    /// extrapolation.
    static UInt64
    boundaryProjection(UInt64 claimed_total, UInt64 rows_inserted, UInt64 insertable, double hll_estimate, double reserve_safety);

    /// Wrapping inserts call this before every row's walk; it grows the table when the walk would take
    /// the last free cell. Public only because the insert target in the build translation unit calls it.
    template <typename Target>
    bool growBeforeLastFreeCell(Target & target);

    /// Pins both phases onto the sequential loops, so tests can cross-check the ring against them.
    void setAmacEnabledForTests(bool value) { amac_enabled = value; }

    /// The L1 partition cap binds only past a few hundred partitions, a build too large for a unit test;
    /// a tiny L1 makes it bind on a small one.
    void setL1CacheSizeForTests(size_t bytes) { l1_cache_bytes_for_tests = bytes; }

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

    size_t predictedArenaBytesForTests(bool grouped) const;
    size_t predictedDuplicateScratchBytesForTests(size_t rows_in_range, bool first_group) const;

private:
    friend class NotJoinedPartitioned;

    /// `HashJoin::data` is private and the non-joined filler is a friend of this class, not of it.
    const HashJoin::RightTableData & storedData() const { return *hash_join->data; }
    /// The inner join is built with one worker: this join stores the blocks itself, one thread at a time.
    HashJoin::StoredBlocksList & storedBlocks() const { return hash_join->data->workers.front().columns; }
    HashJoin::NullmapList & storedNullmaps() const { return hash_join->data->workers.front().nullmaps; }

    /// One accumulated right-side block: the payload in stored form (row store plus columnar
    /// remainder, a full selector), the prepared key columns, and the saved routes.
    struct FillBlock
    {
        StoredBlock stored;
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

        /// Drops the prepared keys, masks and routes once the rows are inserted or scattered; the stored
        /// payload stays. Returns the route bytes freed, for the byte count.
        size_t releaseInputs()
        {
            const size_t freed_route_bytes = routes.allocated_bytes();
            keys_holder.clear();
            key_columns.clear();
            null_map_holder.reset();
            null_map = nullptr;
            join_mask = JoinCommon::JoinMask();
            skip_bytes = {};
            routes = {};
            return freed_route_bytes;
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
    FillLane & getFillLane(size_t worker_id);
    void decidePartitionPlan();
    void storeBlocksInRowStore();
    /// Moves one fill block's stored form into the inner `HashJoin`'s block list and saves its null-key and
    /// filtered rows for RIGHT/FULL output.
    void storeBlockInRowStore(FillBlock & fill);
    /// The saved-block form of one stored block, for the drains that hand blocks to another join.

    /// Both return whether every inserted key was unique, which drives the RightAny promotion.
    bool postBuildPartitioned();
    bool postBuildSinglePartition();
    /// The single-partition insert in three steps, so the single fill thread can run the middle one
    /// per block as it arrives: create the table of `reserve` cells with its context and arenas,
    /// insert one block, finish the scratch and publish. `postBuildSinglePartition` runs all three.
    void beginSinglePartitionInsert(size_t reserve);
    void insertSingleLaneBlock(FillBlock & fill);
    bool finishSinglePartitionInsert();
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
    /// The buffer degree for `reserve` cells; throws past 2^32 cells.
    size_t sizeDegreeFor(size_t reserve) const;
    /// The barrier's sketch estimate, floored at one so an empty build never sizes a zero-byte table.
    size_t distinctEstimate() const { return std::max<size_t>(static_cast<size_t>(std::llround(hll_estimate)), 1); }
    /// Rows the partitioned inserts will see: the sum of the exact per-partition counts.
    UInt64 insertableRows() const;
    /// Bytes still held by the saved routes.
    size_t routeBytes() const;


    /// How the key columns are scattered: fixed-width keys by their raw bytes, anything else
    /// (`String`, `LowCardinality`, ...) through `ColumnsScatter`, with an 8-byte hash word per column
    /// in the chunk.
    struct KeyLayout
    {
        std::vector<size_t> fixed_widths;
        bool generic = false;
        size_t scatteredKeyWidth() const;
    };
    /// Read off the first build block; empty when there is none.
    KeyLayout keyLayout() const;
    /// Total bytes of the prepared key columns over every build block, while the blocks still hold them.
    size_t keyColumnBytes() const;
    static std::unique_ptr<ThreadPool> makePostBuildPool(size_t workers);

    void measureGenericKeyBytes();
    void createHashJoinTable();
    /// The partition floor's memory guard: the scatter transient it introduces has to fit the spill
    /// budget next to what is resident already (the post-build gate's ungrouped peak).
    bool partitionFloorFitsMemory(size_t floor_bits, size_t floor_degree) const;
    void reduceWorkerHistogram();
    void resetWorkerHistogram(PostBuildContext & ctx);
    void histogramWorker(PostBuildContext & ctx, size_t worker) const;
    void allocateWorker(PostBuildContext & ctx, size_t worker) const;
    void scatterWorker(PostBuildContext & ctx, size_t worker);

    /// Splits every current bucket into `2^refine_bits` sub-buckets by the next slice of the route
    /// below the `bits_done` earlier passes consumed, bucket-major. After the last pass a row's
    /// partition is `route >> (16 - bits)` - the same partition a single-pass plan would give it.
    void refinePassWave(PostBuildContext & ctx, size_t refine_bits, size_t bits_done, std::atomic<UInt64> & stage_thread_us);

    /// The owner wave: workers claim partitions largest-first and fill their ranges; then the capacity
    /// guard and the serial drain of every partition's overflow.
    void ownerWaveWorker(PostBuildContext & ctx, size_t worker);
    bool tableHasZero() const;
    UInt64 claimedBufferCells() const;
    UInt64 claimedTotal() const;
    void drainOverflow(PostBuildContext & ctx);
    /// Why the table doubles: the walk is about to take the last free cell (this grow cannot be
    /// refused), or the projected fill exceeds the load factor (refused under a tight budget).
    enum class GrowReason : UInt8
    {
        LastFreeCell,
        LoadFactor,
    };
    size_t residentBytes() const;
    void grow(UInt64 occupied, UInt64 projected, GrowReason reason, size_t extra_reserved = 0);
    template <typename Table>
    void growHashJoinTable(Table & table, UInt64 occupied, UInt64 projected, GrowReason reason, size_t extra_reserved = 0);
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
    void joinRightColumns(const Map & table, AddedColumnsType & added_columns, const ScatteredBlock & block, size_t lane);

    /// Per-probe-stream scratch, pooled on the join and reused across blocks: the find pass's results.
    /// `found_word` is the matched cell's mapped value by value (see `amac_mapped_fits_word`; 0 is a
    /// miss; ASOF stores the mapped pointer's bits instead). `found_offset` is the used-flags offset.
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
    /// Same as the constructor budget unless a test lifts or tightens it to force or refuse a grow.
    size_t grow_budget = 0;
    std::optional<size_t> grow_budget_for_drain_for_tests;
    std::optional<size_t> reserve_override_for_tests;

    /// Owns everything the emit machinery needs: block preparation, the saved block sample, the
    /// shared row store, the used flags, the output samples. Its own map stays empty and the shared
    /// table replaces it - except on the delegated path, where it runs the join whole.
    std::unique_ptr<HashJoin> hash_join;

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
    /// Lane-less callers keep the thread-id map. Shared with the per-lane sketch `add`, exclusive for
    /// the merge: a torn register would persist into the barrier's estimate.
    SharedMutex fill_mutex;
    std::deque<FillLane> lanes;
    std::unordered_map<std::thread::id, FillLane *> lane_by_thread;
    std::vector<std::atomic<FillLane *>> fill_lane_slots;
    std::atomic<size_t> accumulated_rows{0};
    std::atomic<size_t> accumulated_bytes{0};
    /// The row store layout is derived from the first block, as `HashJoin` does.
    std::once_flag row_store_init_flag;

    size_t bits = 0;
    size_t partitions = 1;
    /// MSB-first slices of the route word, summing to `bits`.
    std::vector<size_t> pass_bits;
    /// `partitioned_hash_join_max_fanout_per_pass`.
    size_t max_fanout_per_pass;
    /// `partitioned_hash_join_cap_partitions_by_l1_descriptors`.
    bool cap_partitions_by_l1_descriptors;
    /// `parallel_hash_join_threshold`: from this many build rows on, the insert phase gets at least one
    /// partition per worker, as `parallel_hash` gets one table per slot. Below it, an estimated build
    /// runs on one fill thread.
    size_t parallel_hash_join_threshold;
    std::optional<size_t> build_rows_hint;
    bool single_fill_thread = false;
    std::optional<size_t> l1_cache_bytes_for_tests;
    std::optional<size_t> forced_bits_for_tests;
    double hll_estimate = 0;
    /// Reserve factor over the sketch estimate. Also the multiplicity band below which the arena
    /// prediction treats the build as unique (`predictedTableAndArenaBytes`); that second use needs the
    /// wide margin, the ~1.15% sketch error alone would not.
    double reserve_safety = 1.2;
    /// The table's buffer degree, fixed at the barrier: `2^size_degree` cells, `2^bits` ranges.
    size_t size_degree = 0;
    /// Distinct-key statistics for the next run of this query (join reordering, runtime filters). Never
    /// read to size this build: the sketch sizes the table and a grow corrects it, and a cached count
    /// would not depend on the data.
    StatsCollectingParams stats_collecting_params;
    /// The matched-row statistics the planner's row store decision reads.
    StatsCollectingParams match_stats_collecting_params;
    /// Empty when the probe did not count matches, so nothing is published for that run.
    std::optional<size_t> hash_table_matches;
    bool probe_phase_finished = false;
    std::vector<FillBlock> build_blocks; /// concatenated lanes, row-store block numbers assigned
    /// When every block and row number fits 16 bits the scattered locator column packs into
    /// `(block_no << 16) | row_no` and is decoded at insert, halving the largest scatter transient.
    bool narrow_locators = false;

    /// The one table. `build_arenas` hold the string keys and the duplicate spans the cells point at,
    /// so they must outlive it: one arena per build worker plus one for the drain.
    std::unique_ptr<HashJoinTableMaps> table_maps;
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
    /// Number of block ranges the budget implies, from the ungrouped floor. 1 until `planPostBuild`
    /// computes it, and 1 on the fill-phase and ungrouped paths.
    size_t groups_est = 1;
    /// Set by `preparePostBuildContext` when the histogram already covers the whole build at the
    /// pass-1 width, so the ungrouped path does not scan the routes twice.
    bool histogram_covers_full_build = false;

    bool build_phase_finished = false;

    /// `amac_enabled` is the switch; `amac_build_engaged` the decision taken before the owner inserts.
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
