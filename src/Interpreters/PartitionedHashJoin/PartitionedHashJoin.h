#pragma once

#include <Columns/ColumnNullable.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/HashJoinClause.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/SharedMutex.h>

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

/** Partitioned hash join (`join_algorithm = 'partitioned_hash'`).
  *
  * `parallel_hash` probes one shared map. Once the build side outgrows the last-level cache,
  * every lookup is a cold miss. This join keeps one hash table for the whole right side and
  * builds it in partitions. The cell buffer is `2^bits` contiguous ranges. A partition is the
  * set of build rows whose home cell lies in one range. The route's top `bits` name a row's
  * partition.
  *
  * A worker fills one range while that range stays in cache. The probe side is never
  * partitioned. A probe row hashes once and walks the one table. Extra memory does not grow
  * with the probe side. Probe rows are joined and passed on at once. Nothing on the probe side
  * is buffered.
  *
  * Fill stores right-side blocks per lane and records a 16-bit route. A HyperLogLog sketch sizes a
  * cold build; a cached distinct count lets a warm build skip the sketch.
  * Nothing is inserted yet. The barrier sizes the table at 50% max fill and picks the partition
  * count. The partition count is the smallest power of two whose range fits private L2, at least
  * one range per worker.
  *
  * Post-build scatters keys and row locators. Workers insert. One thread drains overflow that
  * wrapped past a range end.
  *
  * The table doubles in place when a wrapping insert would take the last empty cell. It also
  * doubles between waves when the projected fill would exceed 50%. Duplicates of a key are
  * stored inline, as an exact span, or as a newest-first chain of spans (`SpanWriter`).
  *
  * Probe looks up from the home cell. Above the prefetch threshold, AMAC (asynchronous memory
  * access chaining) keeps a ring of in-flight lookups whose cache misses overlap. Emit, used
  * flags and per-kind logic are the standard `HashJoin` machinery.
  *
  * `HashJoinClause` owns the table and the build. This class owns the block store, fill lanes,
  * used flags and the probe. Used flags are `cells + 1` entries (offset 0 is the zero-value cell).
  * That is the layout `JoinUsedFlags` and the non-joined scan expect.
  *
  * Several ON disjuncts need used flags per right-table row, not per cell. Those joins run a
  * standard `HashJoin` behind this interface (`delegate_mode`) instead of a partitioned build.
  */
class PartitionedHashJoin : public IJoin
{
public:
    /// `build_rows_hint_` is the planner's right-side row estimate, when it has one. Below
    /// `parallel_hash_join_threshold` the join builds on one fill thread. The pipeline keeps the
    /// `hash` shape. The table is sized from the hint and grows like `hash`'s. Every block is
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

    /// Every parallel fill stream reports totals at its end-of-fill. Unlike the base class's
    /// unsynchronized default, these need a guard, as the parallel `HashJoin` layout has.
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;

    /// The peak this build is heading for: the row store and routes already allocated, plus the table
    /// and arena still to come. `SpillingHashJoin` compares it with the external-join threshold, while
    /// `getTotalByteCount` reports what is allocated now. With `at_barrier` the fill is complete, so a
    /// single fill thread's table has a doubling ahead only when the claimed count exceeds the maximum fill.
    size_t predictedResidentBytes(bool at_barrier = false) const;

    /// Bytes the stored rows would take in one in-memory join: the row store as it stands, plus the table
    /// and arena predicted without grouping from the barrier's exact totals. On the `MustSpill` path
    /// `SpillingHashJoin` divides it by the grace per-bucket capacity to size the initial bucket count.
    size_t graceInMemoryEstimateBytes() const;

    StepAnalysisReport getAnalysisReport() const override;
    bool alwaysReturnsEmptySet() const override;

    /// The fill is per-lane plus a short mutexed append, so right-side streams may fill
    /// concurrently. The delegated path inserts into one `HashJoin`, which is not thread-safe, and
    /// a build estimated small keeps the narrow pipeline on purpose.
    bool supportParallelJoin() const override { return !delegate_mode && !single_fill_thread; }
    /// Probe blocks are joined whole, never scattered across slots, and the result caps its own blocks.
    bool emitsSizedOutputBlocks() const override { return true; }

    /// One fill thread inserting as it goes: the rows live in the stored blocks and the table, never
    /// in fill lanes, so a spill switch drains the stored blocks.
    bool isSingleLaneBuild() const { return single_fill_thread; }

    void onBuildPhaseFinish() override;
    bool hasPostBuildPhase() const override { return true; }
    void runPostBuildPhase() override;

    /// The planner reads the matched count of the previous run to decide on the row store. It is
    /// published at destruction, as the other hash joins publish theirs.
    void onProbePhaseFinish(std::optional<size_t> matched_right_rows) override
    {
        hash_table_matches = matched_right_rows;
        probe_phase_finished = true;
    }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// The table's cells are independent, so the non-joined scan splits them into `num_streams`
    /// contiguous position ranges. Stream 0 also emits the zero-value cell and the null-key rows. The
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

    /// See `HashJoinClause::BuildStats`. Valid after `runPostBuildPhase`.
    using BuildStats = HashJoinClause::BuildStats;
    BuildStats getBuildStats() const;

    void setReserveSafetyFactorForTests(double factor) { clause.setReserveSafetyFactorForTests(factor); }
    void setReserveOverrideForTests(size_t reserve) { clause.setReserveOverrideForTests(reserve); }
    void setAmacEnabledForTests(bool value) { clause.setAmacEnabledForTests(value); }
    void setL1CacheSizeForTests(size_t bytes) { clause.setL1CacheSizeForTests(bytes); }
    void setPartitionBitsForTests(size_t value) { clause.setPartitionBitsForTests(value); }
    void setGrowBudgetForTests(size_t bytes) { clause.setGrowBudgetForTests(bytes); }
    void setGrowBudgetForDrainForTests(size_t bytes) { clause.setGrowBudgetForDrainForTests(bytes); }
    void setLiveEstimateGateEnabledForTests(bool value) { live_estimate_gate_enabled_for_tests = value; }
    size_t getCachedLiveDistinctEstimateForTests() const { return cached_distinct_estimate.load(std::memory_order_relaxed); }
    size_t predictedArenaBytesForTests(bool grouped) const { return clause.predictedArenaBytesForTests(grouped); }
    size_t predictedDuplicateScratchBytesForTests(size_t rows_in_range, bool first_group) const
    {
        return clause.predictedDuplicateScratchBytesForTests(rows_in_range, first_group);
    }

    /// The post-build memory verdict, taken once at the barrier from numbers that already exist. A
    /// delegated build always fits: its table is built. A single fill thread's resident set is compared
    /// with the budget. A partitioned build asks the clause (`HashJoinClause::planPostBuild`).
    using PostBuildPlan = HashJoinClause::PostBuildPlan;
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
    /// Feeds every remaining row-store block to `target` from up to `num_threads` workers. Call after
    /// `beginStoredBlockDrain`; `target.addBlockToJoin` must accept concurrent callers, as
    /// `GraceHashJoin` does.
    void drainStoredBlocksInto(IJoin & target);
    double getFillSketchEstimateForTests();

private:
    friend class NotJoinedPartitioned;

    /// `HashJoin::data` is private and the non-joined filler is a friend of this class, not of it.
    const HashJoin::RightTableData & storedData() const { return *hash_join->data; }
    /// The inner join is built with one worker: this join stores the blocks itself, one thread at a time.
    HashJoin::StoredBlocksList & storedBlocks() const { return hash_join->data->workers.front().columns; }
    HashJoin::NullmapList & storedNullmaps() const { return hash_join->data->workers.front().nullmaps; }

    using FillBlock = HashJoinClause::FillBlock;

    /// One per fill thread, so appends and sketch updates never contend.
    struct FillLane
    {
        std::vector<FillBlock> blocks;
        DenseHyperLogLog hll;
    };

    FillLane & getFillLane();
    FillLane & getFillLane(size_t worker_id);
    /// Moves one fill block's stored form into the inner `HashJoin`'s block list and saves its null-key and
    /// filtered rows for RIGHT/FULL output.
    void storeBlockInRowStore(FillBlock & fill);
    /// The saved-block form of one stored block, for the drains that hand blocks to another join.
    Block storedBlockToBlock(StoredBlock && stored) const;
    size_t liveDistinctEstimate() const;
    void finishBuildPhase(bool all_values_unique);
    /// Sizes the flag space to `cells + 1` for the shapes that keep right-side flags.
    void reinitUsedFlags();

    /// `MapsShape` is the standard shape the (kind, strictness) pair dispatches to; the shared table is
    /// its partitioned counterpart, holding identical cells.
    JoinResultPtr probeDispatch(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape> // NOLINT(readability-identifier-naming)
    JoinResultPtr probeImpl(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType> // NOLINT(readability-identifier-naming)
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
    /// Zero disables the post-build memory gate and the grow budget of the clause.
    const size_t max_bytes_before_external_join;

    /// Owns everything the emit machinery needs: block preparation, the saved block sample, the
    /// shared row store, the used flags, the output samples. Its own map stays empty and the shared
    /// table replaces it - except on the delegated path, where it runs the join whole.
    std::unique_ptr<HashJoin> hash_join;

    /// Set for the shapes that need per-row used flags; see the class comment.
    const bool delegate_mode;

    /// `IJoin::totals` is private, so the guarded overrides keep their own copy.
    std::mutex totals_mutex;
    Block totals;

    /// `lanes` owns the per-lane state and the barrier iterates it. The slot table resolves a
    /// pipeline-carried lane index without a lock: one mutexed emplace on a lane's first block, then
    /// atomic loads. It is sized once and never resized, so the fast path cannot race a rehash.
    /// Lane-less callers keep the thread-id map.
    /// Mutable because `predictedResidentBytes` is a `const` query that still has to refresh the
    /// cached distinct estimate under this lock. Spill-enabled fills take a shared lock across
    /// each block's sketch update; the live estimate takes an exclusive lock to observe full blocks.
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
    bool live_estimate_gate_enabled_for_tests = true;

    std::optional<size_t> build_rows_hint;
    /// An estimated build below `parallel_hash_join_threshold` runs on one fill thread, which inserts
    /// into the table as the blocks arrive.
    bool single_fill_thread = false;
    /// The previous build's count is captured before parallel fill starts when spilling is off.
    /// Spill-enabled fills still need sketches for the live memory estimate.
    std::optional<size_t> cached_distinct_keys;
    /// Distinct-key statistics for this and the next run of the query.
    StatsCollectingParams stats_collecting_params;
    /// The matched-row statistics the planner's row store decision reads.
    StatsCollectingParams match_stats_collecting_params;
    /// Empty when the probe did not count matches, so nothing is published for that run.
    std::optional<size_t> hash_table_matches;
    bool probe_phase_finished = false;
    std::vector<FillBlock> build_blocks; /// concatenated lanes, row-store block numbers assigned
    /// After `beginStoredBlockDrain` the row store is being drained and this instance must not be
    /// used except for `releaseNextStoredBlock`.
    bool stored_blocks_released = false;

    bool build_phase_finished = false;
    /// Stored blocks whose fixed-width payload went into a row store.
    UInt64 row_store_blocks = 0;

    std::mutex probe_scratch_mutex;
    std::vector<std::unique_ptr<ProbeScratch>> probe_scratch_pool;
    /// One parked scratch per probe lane, owned when non-null. Acquire exchanges it out, release
    /// CASes it back; a miss goes through the pool.
    std::vector<std::atomic<ProbeScratch *>> probe_scratch_slots;

    LoggerPtr log;

    /// The one clause's table and its build, over this join's store, fill blocks and byte count.
    HashJoinClause clause;
};

}
