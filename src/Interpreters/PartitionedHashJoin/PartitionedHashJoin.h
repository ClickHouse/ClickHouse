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
  * The table and everything that builds it - routing, plan, scatter, inserts, growth - live in `HashJoinClause`,
  * one per ON clause (one today); this class owns the block store, the fill lanes, the used flags and the probe.
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
    using BuildStats = HashJoinClause::BuildStats;
    BuildStats getBuildStats() const;

    /// The test hooks of the clause's build; see `HashJoinClause`.
    void setReserveSafetyFactorForTests(double factor) { clause.setReserveSafetyFactorForTests(factor); }
    void setReserveOverrideForTests(size_t reserve) { clause.setReserveOverrideForTests(reserve); }
    void setAmacEnabledForTests(bool value) { clause.setAmacEnabledForTests(value); }
    void setL1CacheSizeForTests(size_t bytes) { clause.setL1CacheSizeForTests(bytes); }
    void setPartitionBitsForTests(size_t value) { clause.setPartitionBitsForTests(value); }

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
    /// Lane-less callers keep the thread-id map. Shared with the per-lane sketch `add`, exclusive for
    /// the merge: a torn register would persist into the barrier's estimate.
    SharedMutex fill_mutex;
    std::deque<FillLane> lanes;
    std::unordered_map<std::thread::id, FillLane *> lane_by_thread;
    std::vector<std::atomic<FillLane *>> fill_lane_slots;
    std::atomic<size_t> accumulated_rows{0};
    std::atomic<size_t> accumulated_bytes{0};

    std::optional<size_t> build_rows_hint;
    /// An estimated build below `parallel_hash_join_threshold` runs on one fill thread, which inserts
    /// into the table as the blocks arrive.
    bool single_fill_thread = false;
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
