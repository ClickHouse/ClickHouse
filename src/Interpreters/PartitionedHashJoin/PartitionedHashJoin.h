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
#include <Storages/TableLockHolder.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

namespace DB
{

class MatchedRowsStats;
class TableJoin;
class MatchedRowsStats;

/** Partitioned hash join: the join behind `join_algorithm = 'hash'` (and its alias `parallel_hash`).
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
  * Fill stores right-side blocks per lane and records a 16-bit route plus a HyperLogLog sketch.
  * Nothing is inserted yet. The barrier sizes the table at 50% max fill and picks the partition
  * count. The partition count is the smallest power of two whose range fits private L2, at least
  * one range per worker.
  *
  * Post-build scatters keys and row locators. Workers insert. One thread drains overflow that
  * wrapped past a range end.
  *
  * A join with several disjuncts (`ON a OR b`) holds one clause per disjunct over the one store, as
  * `HashJoin` holds one map per disjunct. The fill routes every row to every clause. The barrier builds
  * the tables, at once when no memory budget applies and one after another under a budget. The probe
  * walks the clauses in order. A right row reached through several keys is emitted once
  * (`KnownRowsHolder`).
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
  * Several disjuncts, or a mixed non-equi ON condition on a RIGHT or FULL join, make a right row
  * reachable through several keys. Such a row needs a used flag per right-table row, not per cell.
  * Those joins keep the flags per row (`used_flags_per_row`), attached to the stored blocks. Their
  * non-joined scan walks the stored blocks instead of the table, as `HashJoin` does.
  *
  * The Join table engine (`StorageJoin`) runs this join in a third mode, `join_table_mode`: one
  * single-partition table, created empty with the join and filled one block at a time under the
  * storage's write lock through `HashJoinTable::emplace`, so it grows as the rows arrive and is
  * probe-ready between inserts; the rows of a key are chained with the appendable `RowRefList`
  * `Batch`. There is no build phase. A query gets an instance of its own whose `hash_join` reuses
  * the storage's stored blocks and which shares the table and its arena by pointer
  * (`shareJoinTable`), with used flags of its own sized to the table. `joinGet` is a one-block probe
  * of the storage's instance.
  */
class PartitionedHashJoin : public IJoin
{
public:
    /// `build_rows_hint_` is the planner's right-side row estimate, when it has one. Below
    /// `parallel_hash_join_threshold`, and whenever the query has one thread, the join builds on one
    /// fill thread and the pipeline keeps the `hash` shape. The table is sized from the distinct-key
    /// count a previous run left in the hash table statistics cache, or starts small without one, and
    /// grows like `hash`'s. Every block is inserted as it arrives, so nothing is left for the barrier.
    /// One thread would pay the histogram and scatter passes of the partitioned build and gain nothing
    /// from them.
    PartitionedHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        size_t num_threads_,
        bool any_take_last_row_ = false,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        size_t max_bytes_before_external_join_ = 0,
        std::optional<size_t> build_rows_hint_ = {});

    /// The Join table engine's instance; see the class comment. Single-threaded by construction: the
    /// storage serializes the inserts with its write lock.
    struct JoinTableTag
    {
    };
    PartitionedHashJoin(JoinTableTag, std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_);

    ~PartitionedHashJoin() override;

    /// Makes this Join table instance a query's view of `source`, the storage's. `hash_join` reuses the
    /// storage's stored blocks (the saved sample, the row store and the null maps come with them); the
    /// table and its arena are shared by pointer; the used flags of this instance are sized to the
    /// table. The caller holds the storage's read lock and hands it to `setLock`, so the table cannot
    /// change while this instance reads it.
    void shareJoinTable(const PartitionedHashJoin & source);

    /// Keeps the storage's read lock for this instance's lifetime, as `HashJoin::setLock` does.
    void setLock(TableLockHolder holder) { storage_join_lock = std::move(holder); }

    /// A query's instance of a Join table is probed through `FilledJoinStep`: there is no right stream
    /// to fill and no `NonJoinedBlocksTransform` to run the parallel non-joined regime in.
    JoinPipelineType pipelineType() const override
    {
        return shared_from_join_table ? JoinPipelineType::FilledRight : JoinPipelineType::FillRightFirst;
    }
    bool isParallelNonJoinedProcessingEnabled() const override
    {
        return !shared_from_join_table && supportParallelNonJoinedBlocksProcessing();
    }

    /// `joinGet` over the storage's instance, with the contract of `HashJoin::joinGet`. The key types
    /// and the result type are checked first. Then the keys are probed as one block with `LEFT ANY`
    /// semantics, and the requested column comes back with a default for every key not found.
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;
    ColumnWithTypeAndName joinGet(const Block & block, const Block & block_with_columns_to_add);

    /// `OPTIMIZE TABLE` on a Join table: compacts the columns of the stored blocks.
    void shrinkStoredBlocksToFit();

    /// The join kinds and strictnesses this join serves; everything else is routed before the
    /// algorithm loop of `tryCreateJoin` in `Planner/PlannerJoins.cpp`.
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
    /// The distinct-key count, or a cheap bound when it cannot change the row-limit check.
    size_t rowCountForLimit(size_t max_rows) const;

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
    /// concurrently. A build estimated small keeps the narrow pipeline on purpose.
    bool supportParallelJoin() const override { return !single_fill_thread; }
    /// Probe blocks are joined whole, never scattered across slots, and the result caps its own blocks.
    bool emitsSizedOutputBlocks() const override { return true; }
    /// The left side is streamed through once; the find pass may run out of order, the emit pass walks
    /// the block in input order.
    bool preservesLeftBlockOrder() const override { return true; }

    /// One fill thread inserting as it goes: the rows live in the stored blocks and the table, never
    /// in fill lanes, so a spill switch drains the stored blocks.
    bool isSingleLaneBuild() const { return single_fill_thread; }

    void onBuildPhaseFinish() override;
    /// A Join table's join has no build phase at all.
    bool hasPostBuildPhase() const override { return !join_table_mode; }
    void runPostBuildPhase() override;

    /// The matched-row statistics `EXPLAIN ANALYZE` reports, when the query collects them.
    const MatchedRowsStats * getMatchStats() const { return matched_rows_stats.get(); }

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
    /// contiguous position ranges. Stream 0 also emits the zero-value cell and the null-key rows. With
    /// per-row flags the streams split the stored blocks instead.
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

    /// `joinPipelinesByShards` clones one join per primary-key layer, each filled by one stream and
    /// probed by one, and never installs `NonJoinedBlocksTransform`: a clone that still advertised the
    /// parallel non-joined regime would skip unmatched right rows of a RIGHT/FULL join.
    std::shared_ptr<IJoin> cloneNoParallel(
        const std::shared_ptr<TableJoin> & table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_) const override;

    /// This instance holds part of the right side: a `GraceHashJoin` bucket, or a primary-key shard of
    /// `joinPipelinesByShards`. Its table cannot stand in for the whole build side. The exact runtime
    /// filter over a fixed table drops every probe row whose key the table lacks, so a partial build
    /// does not publish it. The shard clones mark themselves; a grace bucket is marked by its owner.
    void markPartialBuild() { partial_build = true; }
    /// Same as `clone`, for a side swap: the caller has the estimate of the new build side.
    std::shared_ptr<IJoin> cloneWithBuildRowsHint(
        const std::shared_ptr<TableJoin> & table_join_, SharedHeader right_sample_block_, std::optional<size_t> build_rows_hint_) const;

    void setEnableLazyColumnsIndexing(bool value) override;

    /// See `HashJoinClause::BuildStats`, per clause. Valid after `runPostBuildPhase`.
    using BuildStats = HashJoinClause::BuildStats;
    BuildStats getBuildStats(size_t clause_idx = 0) const;

    void setReserveSafetyFactorForTests(double factor)
    {
        for (auto & clause : clauses)
            clause.setReserveSafetyFactorForTests(factor);
    }
    void setReserveOverrideForTests(size_t reserve)
    {
        for (auto & clause : clauses)
            clause.setReserveOverrideForTests(reserve);
    }
    void setAmacEnabledForTests(bool value)
    {
        for (auto & clause : clauses)
            clause.setAmacEnabledForTests(value);
    }
    void setL1CacheSizeForTests(size_t bytes)
    {
        for (auto & clause : clauses)
            clause.setL1CacheSizeForTests(bytes);
    }
    /// A forced partition plan is a partitioned build, which a one-thread join would otherwise skip.
    void setPartitionBitsForTests(size_t value)
    {
        for (auto & clause : clauses)
            clause.setPartitionBitsForTests(value);
        single_fill_thread = false;
    }
    void setGrowBudgetForTests(size_t bytes)
    {
        for (auto & clause : clauses)
            clause.setGrowBudgetForTests(bytes);
    }
    void setGrowBudgetForDrainForTests(size_t bytes)
    {
        for (auto & clause : clauses)
            clause.setGrowBudgetForDrainForTests(bytes);
    }
    size_t predictedArenaBytesForTests(bool grouped) const { return clauses.front().predictedArenaBytesForTests(grouped); }
    size_t predictedDuplicateScratchBytesForTests(size_t rows_in_range, bool first_group) const
    {
        return clauses.front().predictedDuplicateScratchBytesForTests(rows_in_range, first_group);
    }

    /// The post-build memory verdict, taken once at the barrier from numbers that already exist. A
    /// single fill thread's resident set is compared with the budget. A partitioned build asks every
    /// clause (`HashJoinClause::planPostBuild`) and takes the worst answer.
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

    /// Every right block stored so far, for an algorithm that takes them over during the fill
    /// (`JoinSwitcher`, `GraceHashJoin`): the fill lanes, or the row store of a single fill thread. With
    /// `restructure` the blocks come back in the right input's structure. Only before the build phase
    /// finished, and nothing but destruction may follow.
    BlocksList releaseJoinedBlocks(bool restructure);
    /// The structure the right blocks are stored in.
    const Block & savedBlockSample() const;
    /// Right rows stored so far. `getTotalRowCount` reports the distinct keys once the table is built.
    size_t getRightTableRowCount() const;

private:
    friend class NotJoinedPartitioned;
    /// Reads a Join table's rows straight out of the table and the stored blocks.
    friend class JoinSource;

    PartitionedHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        size_t num_threads_,
        bool any_take_last_row_,
        const HashJoinStatsCollectingParams & stats_collecting_params_,
        size_t max_bytes_before_external_join_,
        std::optional<size_t> build_rows_hint_,
        bool join_table_mode_);

    /// `HashJoin::data` is private and the non-joined filler is a friend of this class, not of it.
    const HashJoin::RightTableData & storedData() const { return *hash_join->data; }
    /// This join stores the blocks itself, one thread at a time.
    HashJoin::StoredBlocksList & storedBlocks() const { return hash_join->data->columns; }
    HashJoin::NullmapList & storedNullmaps() const { return hash_join->data->nullmaps; }

    using FillBlock = HashJoinClause::FillBlock;

    /// One per fill thread, so appends and sketch updates never contend. The mutex guards `hll` alone.
    /// The lane's filler holds it across a block's hash pass. A sketch merge takes it lane by lane, so
    /// a merge never stalls the other lanes.
    struct FillLane
    {
        explicit FillLane(size_t num_clauses) : hll(num_clauses) { }

        std::vector<FillBlock> blocks;
        /// One sketch per clause, indexed like `clauses`.
        std::vector<DenseHyperLogLog> hll;
        mutable std::mutex hll_mutex;
    };

    FillLane & getFillLane();
    FillLane & getFillLane(size_t worker_id);
    /// Moves one fill block's stored form into the inner `HashJoin`'s block list and saves its null-key and
    /// filtered rows for RIGHT/FULL output. Returns whether a saved null map refers to the block.
    bool storeBlockInRowStore(FillBlock & fill);
    /// A block that nothing refers to is not kept, as `HashJoin` does not keep it: `ANY` tables see
    /// their repeated keys re-inserted without growing. Join-engine mode only.
    void dropLastStoredBlock();
    /// The saved-block form of one stored block, for the drains that hand blocks to another join.
    Block storedBlockToBlock(StoredBlock && stored) const;
    /// Frees the stored blocks of a large build from several threads, at destruction; see the definition.
    void destroyStoredBlocksInParallel();
    /// The fill-phase distinct estimate of one clause; refreshing it refreshes every clause's.
    size_t liveDistinctEstimate(size_t clause_idx) const;
    /// The per-block check of `max_rows_in_join` and `max_bytes_in_join` while the fill is running.
    bool checkFillLimits();
    /// The other clauses' tables and arenas, built or predicted; see `HashJoinClause::setBytesReservedElsewhere`.
    size_t bytesReservedForOtherClauses(size_t clause_idx) const;
    /// Every clause's table and arenas.
    size_t tablesAndArenasBytes() const;
    /// Reads the previous run's distinct-key count into `cached_distinct_keys` and counts it as a
    /// preallocation. False when the cache has no entry for this join or the entry exceeds
    /// `max_size_to_preallocate_for_joins`.
    bool readDistinctKeysFromStatisticsCache();
    void finishBuildPhase(bool all_values_unique);
    /// Sizes the flag space to `cells + 1` for the shapes that keep right-side flags.
    void reinitUsedFlags();
    /// The pool of one clause's post-build waves. It is created on first use after the barrier and sized
    /// to the smaller of the thread count and the block count. One per clause, so the clauses can build at
    /// once; released with the scratch.
    ThreadPool & postBuildPool(size_t clause_idx);

    /// `MapsShape` is the standard shape the (kind, strictness) pair dispatches to; the shared table is
    /// its partitioned counterpart, holding identical cells. With `join_get_columns` the block carries
    /// the keys under the right-side names and the result is the `joinGet` output of those columns.
    JoinResultPtr probeDispatch(Block block, size_t lane);

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape> // NOLINT(readability-identifier-naming)
    JoinResultPtr probeImpl(Block block, size_t lane, const Block * join_get_columns = nullptr);

    /// Returns the number of probe rows processed: all of them, unless a mixed ON condition stops the
    /// block at `max_joined_block_rows`, as the standard join does.
    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType> // NOLINT(readability-identifier-naming)
    size_t joinRightColumns(const Map & table, AddedColumnsType & added_columns, const ScatteredBlock & block, size_t lane);

    /// The probe of a join with several ON clauses (`ON a OR b`), over one table per clause: the
    /// multi-map loop of `HashJoin`, with the used flags kept per right-table row.
    template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsShape, typename KeyGetter, typename Map, typename AddedColumnsType> // NOLINT(readability-identifier-naming)
    size_t joinRightColumns(const std::vector<const Map *> & tables, AddedColumnsType & added_columns, const ScatteredBlock & block);

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
    /// shared row store, the used flags, the output samples. Its own maps stay empty and the clauses'
    /// tables replace them.
    std::unique_ptr<HashJoin> hash_join;

    /// The Join table engine's mode; see the class comment.
    const bool join_table_mode;
    /// The used flags are keyed per right-table row instead of per cell (`HashJoin::needUsedFlagsForPerRightTableRow`);
    /// see the class comment.
    const bool used_flags_per_row;
    /// Whether the shape keeps used flags at all (`MapGetter::flagged`); with `used_flags_per_row` every
    /// stored block then carries one flag per row.
    bool allocate_per_row_flags = false;
    /// A query's instance after `shareJoinTable`.
    bool shared_from_join_table = false;
    /// Cleared on the clones of `cloneNoParallel`.
    bool parallel_non_joined_allowed = true;
    /// See `markPartialBuild`.
    bool partial_build = false;
    /// The storage's read lock, see `setLock`.
    TableLockHolder storage_join_lock;

    /// `IJoin::totals` is private, so the guarded overrides keep their own copy.
    std::mutex totals_mutex;
    Block totals;

    /// `lanes` owns the per-lane state and the barrier iterates it. The slot table resolves a
    /// pipeline-carried lane index without a lock: one mutexed emplace on a lane's first block, then
    /// atomic loads. It is sized once and never resized, so the fast path cannot race a rehash.
    /// Lane-less callers keep the thread-id map.
    /// Mutable because `predictedResidentBytes` is a `const` query that still has to refresh the
    /// cached distinct estimate under this lock. The sketches themselves are under their lane's lock.
    mutable std::mutex fill_mutex;
    std::deque<FillLane> lanes;
    std::unordered_map<std::thread::id, FillLane *> lane_by_thread;
    std::vector<std::atomic<FillLane *>> fill_lane_slots;
    std::atomic<size_t> accumulated_rows{0};
    std::atomic<size_t> accumulated_bytes{0};
    /// Some fill call asked for the limit checks, so the built table is checked against them too.
    std::atomic<bool> limits_requested{false};
    /// Fill-phase distinct estimates for `predictedResidentBytes`, one per clause. Merging every lane
    /// on every block would cost `lanes * clauses * 8 KiB`. The values are therefore reused until the
    /// row count has grown by a sixteenth. A slightly stale value only delays the switch by one refresh
    /// interval.
    mutable std::vector<std::atomic<size_t>> cached_distinct_estimates;
    mutable std::atomic<size_t> distinct_estimate_at_rows{0};

    std::optional<size_t> build_rows_hint;
    /// An estimated build below `parallel_hash_join_threshold`, and every build of a one-thread query, runs
    /// on one fill thread, which inserts into the table as the blocks arrive.
    bool single_fill_thread = false;
    /// The distinct-key count a previous run of this query left in the hash table statistics cache,
    /// read once when the table is sized (`readDistinctKeysFromStatisticsCache`); the clause then sizes
    /// from it as an exact estimate.
    std::optional<size_t> cached_distinct_keys;
    /// Distinct-key statistics. The count this build publishes serves the next run of this query: join
    /// reordering, runtime filters, and this join's table size. The previous run's count, when the cache
    /// has one, sizes this build's table.
    StatsCollectingParams stats_collecting_params;
    /// The matched-row statistics the planner's row store decision reads.
    StatsCollectingParams match_stats_collecting_params;
    /// The matched-row counts `EXPLAIN ANALYZE` reports, only when the query asks for them. The left
    /// side is counted per probe block from the block's outputs. The right side comes from the
    /// non-joined rows, or with `matches = 1` from the row refs the probe records.
    std::unique_ptr<MatchedRowsStats> matched_rows_stats;
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

    /// See `postBuildPool`; indexed like `clauses`.
    std::vector<std::unique_ptr<ThreadPool>> post_build_pools;

    /// One per ON clause, indexed like `TableJoin::getClauses`: each holds its table and its build, all
    /// over this join's store, fill blocks and byte count. A deque, because the clause is neither copyable
    /// nor movable (reference members).
    std::deque<HashJoinClause> clauses;
};

}
