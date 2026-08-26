#pragma once

#include <Columns/ColumnNullable.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/PartitionedJoinMaps.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

namespace DB
{

class TableJoin;

/// A leaf map's cell buffer and grower mask, extracted once after the builds into one contiguous
/// 16-bytes-per-leaf array. A row's cell address then costs one L1 load here instead of chasing
/// `leaf_map_ptrs[leaf]` and the map header - three dependent loads. The fixed-size map types keep
/// the zero entry; their probe never takes the descriptor path.
struct LeafMapDesc
{
    const void * buf = nullptr;
    size_t mask = 0;
};

/** Partitioned hash join (`join_algorithm = 'partitioned_hash'`).
  *
  * `parallel_hash` probes one shared map, so once the build side outgrows last-level cache every
  * lookup is a cold miss. This partitions the build side into per-partition tables small enough to
  * stay cache-resident, and leaves the probe side unpartitioned - a probe row is routed to one leaf
  * and looked up there, with no probe-side shuffle and no probe buffering, so transient memory does
  * not scale with probe cardinality and rows flow downstream immediately.
  *
  * The phases:
  *
  * - Fill accumulates right-side blocks per lane untouched. Per row it computes one 32-bit route
  *   word, saves its top 16 bits, and feeds a per-lane sketch. Nothing is inserted yet.
  * - The build barrier merges the sketches and picks the partition count: the smallest power of two
  *   whose worst-case per-leaf bucket array fits private L2. Small builds and the fixed-size map
  *   types degenerate to a single leaf through the same code path.
  * - Post-build scatters only the key columns plus an 8-byte row locator into per-partition chunks
  *   (payload stays in the shared row store), then workers claim leaves largest-first and insert
  *   sequentially. Each leaf's map is created exact-reserved right before its inserts, so the
  *   allocator can recycle the chunks of already-consumed leaves instead of holding every table and
  *   every transient at once.
  * - Probe recomputes each row's route word and looks the key up in that leaf. Above the engagement
  *   threshold this runs as two passes per block - an AMAC find ring out of order, then an in-order
  *   pass over its results - and below it as the plain routed loop. Either way the emit,
  *   replication offsets, used flags and per-kind logic are the standard `HashJoin` machinery.
  *
  * Used flags span all leaves in one per-offset space, leaf L's offsets shifted by `flag_base[L]`,
  * so `JoinUsedFlags` and the non-joined iteration keep their single-map semantics.
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

    /// The peak this build is heading for if every accumulated row ends up in leaf tables: the row
    /// store and route words that are already allocated, plus the tables and arena that are not yet.
    /// `SpillingHashJoin` compares this against the external-join threshold. Not `getTotalByteCount`,
    /// which is the currently allocated amount and feeds `max_bytes_in_join` and `EXPLAIN`.
    size_t predictedResidentBytes() const;

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

    /// Leaves are independent, so the non-joined scan strides over them: stream `i` visits the
    /// leaves where `leaf % num_streams == i`. The delegated path stays single-stream, because
    /// `HashJoin` does not advertise the parallel regime.
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

    /// What the tests assert the build on: the buffer-size prediction, the sketch, and the
    /// growth-past-reserve behaviour.
    struct BuildStats
    {
        size_t bits = 0;
        size_t partitions = 0;
        /// MSB-first radix bits per scatter pass; more than one when the L2 rule wants a fanout
        /// above a single pass's ceiling.
        std::vector<size_t> pass_bits;
        /// Final per-leaf insertable row counts, so tests can assert two pass plans agree.
        std::vector<UInt64> leaf_row_counts;
        double hll_estimate = 0;
        /// Total predicted hash-table buffer bytes across all leaves.
        size_t ht_total_bytes = 0;
        /// Leaves whose map outgrew its create-time buffer - a distinct-estimate shortfall.
        /// Correct, but unplanned, so it is counted rather than passed over.
        UInt64 leaf_growths = 0;
        /// Growths that cancelled an insert ring mid-run.
        UInt64 amac_ring_growths = 0;
        bool amac_build_engaged = false;
        UInt64 leaf_rows = 0;
        bool predictions_exact = true;
        /// A warm run: the build took its distinct-key counts from the statistics cache and the
        /// fill skipped the per-row sketch feed.
        bool distinct_estimate_reused = false;
        /// Empty when the join shape tracks no right-side used flags.
        std::vector<UInt64> flag_base;
        /// Contiguous build-block ranges the post-build scatter was split into. 1 means the whole
        /// build was scattered at once.
        size_t scatter_groups = 1;
    };

    BuildStats getBuildStats() const;

    /// Shrinks the reserve safety factor so the maps must grow, which SQL cannot force reliably.
    void setReserveSafetyFactorForTests(double factor) { reserve_safety = factor; }

    /// Pins both phases onto the sequential loops, so tests can cross-check the ring against them.
    void setAmacEnabledForTests(bool value) { amac_enabled = value; }

    /// Lowers the per-pass fanout ceiling, so the refine passes can be tested without a 500M-key
    /// build.
    void setMaxFanoutPerPassForTests(size_t value) { max_fanout_per_pass = value; }

    /// The post-build memory verdict, taken once at the barrier from numbers that already exist.
    enum class PostBuildPlan
    {
        Fits, /// ungrouped scatter
        Grouped, /// in-memory scatter over block ranges
        MustSpill, /// even the resident data does not fit; the caller must switch to grace
    };
    PostBuildPlan planPostBuild();

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

    /// Shared across the post-build stages: histogram, allocate, scatter, leaf builds.
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
    bool postBuildSingleLeaf();
    void preparePostBuildContext();
    void runGroupStages(size_t block_begin, size_t block_end);
    size_t chunkBytesForBlockRange(size_t b0, size_t b1) const;

    /// Bytes the leaf tables and the duplicate-list arena will need for `rows` build rows holding
    /// `distinct` distinct keys. The post-build gate evaluates this with exact counts; the fill
    /// evaluates it with the running sketch estimate.
    size_t predictedTableAndArenaBytes(size_t rows, size_t distinct) const;
    size_t predictedArenaBytes(size_t insertable_rows) const;

    size_t liveDistinctEstimate() const;

    void measureGenericKeyBytes();
    void sizeLeafHashTables();
    void reduceWorkerHistogram();
    void resetWorkerHistogram(PostBuildContext & ctx);
    void histogramWorker(PostBuildContext & ctx, size_t worker) const;
    void allocateWorker(PostBuildContext & ctx, size_t worker) const;
    void scatterWorker(PostBuildContext & ctx, size_t worker);

    /// Splits every current bucket into `2^refine_bits` sub-buckets by the next route-word slice
    /// below the `bits_done` earlier passes consumed, group-major. After the last pass a row's leaf
    /// is `route >> (16 - bits)` - the same leaf a single-pass plan would give it, and the one the
    /// probe derives.
    void refinePassWave(PostBuildContext & ctx, size_t refine_bits, size_t bits_done, std::atomic<UInt64> & stage_thread_us);
    void leafBuildWorker(PostBuildContext & ctx, size_t worker);
    void finishBuildPhase(bool all_values_unique);

    /// Inserts one compact section into one leaf. Row i's stored ref is `locators[i]`, the decoded
    /// `narrow_locators[i]`, or `RowRef(block_no, i)` when neither is set - the single-leaf path,
    /// which is also the only one where `skip_bytes` applies.
    void insertLeafSection(
        PartitionedJoinMaps & maps,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        const UInt64 * locators,
        const UInt32 * narrow_locators_data,
        UInt32 block_no,
        const UInt8 * skip_bytes,
        Arena & pool,
        bool & all_values_unique);

    /// Derives the flag bases from the final bucket counts and reinitializes the whole flag space.
    void computeFlagBaseAndReinitUsedFlags();

    /// Once after the builds, so the probe does not rebuild a pointer table per `joinBlock`.
    void collectLeafMapPointers();

    /// Decided once, after the tables are sized and before the inserts, on the same heuristics that
    /// enable the software prefetch.
    void decideAmacEngagement();

    /// `MapsShape` is the standard shape the (kind, strictness) pair dispatches to; the leaf maps
    /// are its partitioned counterpart, holding identical cells.
    JoinResultPtr probeDispatch(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape>
    JoinResultPtr probeImpl(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType>
    size_t routedJoinRightColumns(AddedColumnsType & added_columns, const ScatteredBlock & block, size_t lane);

    /// Per-probe-stream scratch, pooled on the join and reused across blocks: the per-row leaf ids
    /// and the find pass's results. `found_word` holds the matched cell's mapped value by value - a
    /// `RowRef` or `RowRefList` is an 8-byte word that is never 0 for a match, so 0 encodes a miss -
    /// which is what keeps the second pass from touching the cell again after it has left the cache.
    /// ASOF does not fit a word and stores the mapped pointer's bits instead. `found_offset` is the
    /// used-flags offset, already shifted into the shared space.
    struct ProbeScratch
    {
        PaddedPODArray<UInt16> leaf_ids;
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

    /// Owns everything the emit machinery needs: block preparation, the saved block sample, the
    /// shared row store, the used flags, the output samples. Its own map stays empty and the leaf
    /// maps replace it - except on the delegated path, where it runs the join whole.
    std::unique_ptr<HashJoin> leaf_join;

    /// Set for the shapes that need per-row used flags; see the class comment.
    const bool delegate_mode;

    /// Which `HashJoin::MapsVariant` alternative is active; the leaf maps mirror it.
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
    /// `ColumnsScatter::MAX_FANOUT_PER_PASS`, overridable by tests.
    size_t max_fanout_per_pass;
    double hll_estimate = 0;
    double reserve_safety = 1.2; /// covers the sketch error (~1.15% at precision 13) and per-leaf spread
    /// Cross-run distinct-key statistics, keyed as the other algorithms key theirs but in a
    /// dedicated cache that keeps the per-partition breakdown rather than only a total. Given a
    /// previous run's counts the fill skips the sketch feed entirely and the leaf sizing uses the
    /// real per-partition counts instead of a uniform rescale, folded or split when this build's
    /// plan bits differ from the cached ones.
    StatsCollectingParams stats_collecting_params;
    std::optional<PartitionedHashJoinEntry> cached_stats;
    std::vector<FillBlock> build_blocks; /// concatenated lanes, row-store block numbers assigned
    /// When every block and row number fits 16 bits the scattered locator column packs into
    /// `(block_no << 16) | row_no` and is decoded at insert, halving the largest scatter transient.
    bool narrow_locators = false;

    /// Each leaf owns its exact-reserved buffer. `build_arenas` hold the string keys and
    /// duplicate-list nodes the cells point at, so they must outlive the maps.
    std::vector<PartitionedJoinMaps> leaf_maps;
    /// Type-erased: the probe casts an entry back through the same `data->type` and maps-variant
    /// pair that stored it.
    std::vector<const void *> leaf_map_ptrs;
    std::vector<LeafMapDesc> leaf_map_descs;
    /// Leaf L's flags live at `[flag_base[L], flag_base[L + 1])` of the shared space, that span
    /// being the leaf's bucket count plus one for the map's zero-value cell.
    std::vector<UInt64> flag_base;
    std::deque<Arena> build_arenas;
    size_t ht_total_bytes = 0; /// total predicted hash-table bytes (drives the prefetch heuristics)

    std::unique_ptr<ThreadPool> post_build_pool;
    std::unique_ptr<PostBuildContext, PostBuildContextDeleter> post_build_ctx;
    /// Exact per-leaf insertable row counts from the full-build histogram, used to size reserves.
    std::vector<UInt64> total_bucket_rows;
    /// Prepared key-column bytes across the whole build, measured once at the gate. Zero unless
    /// the keys are variable-length, which is when they are copied into the arena.
    size_t generic_key_bytes = 0;
    PostBuildPlan post_build_plan = PostBuildPlan::Fits;
    /// After `beginStoredBlockDrain` the row store is being drained and this instance must not be
    /// used except for `releaseNextStoredBlock`.
    bool stored_blocks_released = false;
    /// Set by `preparePostBuildContext` when the histogram already covers the whole build at the
    /// pass-1 width, so the ungrouped path does not scan the routes twice.
    bool histogram_covers_full_build = false;

    bool build_phase_finished = false;

    /// The test override, the engagement decision taken before the leaf-build wave, and the
    /// ring-growth counter.
    bool amac_enabled = true;
    bool amac_build_engaged = false;
    std::atomic<UInt64> amac_ring_growths{0};

    std::mutex probe_scratch_mutex;
    std::vector<std::unique_ptr<ProbeScratch>> probe_scratch_pool;
    /// One parked scratch per probe lane, owned when non-null. Acquire exchanges it out, release
    /// CASes it back; a miss goes through the pool.
    std::vector<std::atomic<ProbeScratch *>> probe_scratch_slots;

    BuildStats stats;

    LoggerPtr log;
};

}
