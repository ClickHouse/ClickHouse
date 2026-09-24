#pragma once

#include <Columns/ColumnNullable.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/DuplicateSpans.h>
#include <Interpreters/PartitionedHashJoin/HashJoinTable.h>
#include <Common/Arena.h>
#include <Common/Logger.h>
#include <Common/PODArray.h>
#include <Common/ThreadPool_fwd.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{

class TableJoin;

/** Hash table of one ON clause of `PartitionedHashJoin`, and the build that fills it.
  * That build covers routing, the barrier plan, histogram, scatter, insert, drain, growth,
  * and the counters of one build.
  *
  * Uses the join's `HashJoin` helper (map type, key sizes, stored blocks) and the join's fill
  * blocks. The join owns the store, fill lanes, used flags and the probe. One instance per ON
  * clause: a join with several disjuncts holds one per clause over the same store.
  */
class HashJoinClause
{
public:
    /// One clause's view of a fill block: its prepared key columns, null map and ON mask, and the
    /// routes its table build saved. Prepared by `prepareInput`, consumed and released by this clause
    /// alone, so a later clause still finds its own keys after an earlier one scattered.
    struct Input
    {
        Columns keys_holder;
        ColumnRawPtrs key_columns;
        ColumnPtr null_map_holder;
        ConstNullMapPtr null_map = nullptr;
        /// The clause's right-side ON condition; rows it filters are not inserted, as in the
        /// standard build.
        JoinCommon::JoinMask join_mask;
        /// Null-key rows OR mask-filtered rows. Materialized only when the mask actually filters.
        /// Otherwise `skipData` returns the plain null map.
        PaddedPODArray<UInt8> skip_bytes;
        PaddedPODArray<UInt16> routes;

        const UInt8 * skipData() const
        {
            if (!skip_bytes.empty())
                return skip_bytes.data();
            return null_map ? null_map->data() : nullptr;
        }

        /// Drops the prepared keys, masks and routes once the rows are inserted or scattered. Returns
        /// the route bytes freed, for the byte count.
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

    /// One right-side block: the stored payload, shared by every clause, and one `Input` per clause.
    struct FillBlock
    {
        StoredBlock stored;
        /// Indexed like the join's clauses.
        std::vector<Input> clauses;
        /// The block's used flags when the join keeps them per right-table row: one per row, zeroed by
        /// the fill thread. The store hands them to the join's flags.
        JoinStuff::JoinUsedFlags::UsedFlagsForColumns per_row_flags;
        size_t rows = 0;
        UInt32 block_no = 0; /// assigned at the build barrier

        /// Every clause's inputs at once, for the paths that abandon the build. The stored payload stays.
        size_t releaseInputs()
        {
            size_t freed_route_bytes = 0;
            for (auto & input : clauses)
                freed_route_bytes += input.releaseInputs();
            return freed_route_bytes;
        }

        /// Bytes the saved routes still hold. A released array reports its padding through
        /// `allocated_bytes`, so only the arrays that still have rows count.
        size_t routeBytes() const
        {
            size_t bytes = 0;
            for (const auto & input : clauses)
                if (!input.routes.empty())
                    bytes += input.routes.allocated_bytes();
            return bytes;
        }
    };

    /// Counters of one build, read after the build finished.
    struct BuildStats
    {
        size_t bits = 0;
        size_t partitions = 0;
        /// MSB-first radix bits per scatter pass; more than one when the plan wants a fanout above a
        /// single pass's ceiling.
        std::vector<size_t> pass_bits;
        /// Final per-partition insertable row counts (insertable = null-key and ON-filtered rows excluded).
        std::vector<UInt64> partition_row_counts;
        double hll_estimate = 0;
        /// `predictions_exact` is true when the created buffer matched the plan's prediction.
        size_t table_size_degree = 0;
        size_t table_cells = 0;
        size_t ht_total_bytes = 0;
        bool predictions_exact = true;
        bool amac_build_engaged = false;
        UInt64 inserted_rows = 0;
        UInt64 distinct_keys = 0;
        /// Rows that wrapped past their range end and went to the serial drain.
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
        /// Stored blocks whose fixed-width payload went into a row store; counted by the join.
        UInt64 row_store_blocks = 0;
    };

    /// `hash_join_` is the schema helper; `clause_idx_` names this clause among `table_join_.getClauses()`,
    /// the helper's `key_sizes` and every fill block's `clauses`; `build_blocks_` is the join's fill list;
    /// freed route bytes go back into `accumulated_bytes_`. `max_bytes_before_external_join_` is the memory
    /// budget of the post-build gate and the grow budget; zero disables both.
    HashJoinClause(
        HashJoin & hash_join_,
        const TableJoin & table_join_,
        size_t clause_idx_,
        bool any_take_last_row_,
        size_t num_threads_,
        size_t max_bytes_before_external_join_,
        std::vector<FillBlock> & build_blocks_,
        std::atomic<size_t> & accumulated_bytes_,
        LoggerPtr log_);
    ~HashJoinClause();

    /// The fill's key preparation for this clause, into `fill.clauses[clause_idx]`: the key columns as
    /// the probe side prepares them in `JoinOnKeyColumns`, the merged null map, the right-side ON mask,
    /// and the skip bytes when the mask filters. `materialized` is the right block after
    /// `HashJoin::materializeColumnsFromRightBlock`.
    void prepareInput(const Block & materialized, FillBlock & fill) const;

    /// One map hash per insertable row. The top 16 bits of the placement word are the route.
    /// The top 32 bits of its mix are fed to `sketch`.
    void computeRoutes(FillBlock & fill, DenseHyperLogLog & sketch) const;
    void computeRoutes(FillBlock & fill) const;

    /// The barrier's decision for `rows` build rows, from the distinct estimate set with
    /// `setDistinctEstimate`: the table degree, the partition count and the scatter passes.
    void decidePartitionPlan(size_t rows);
    /// `exact` identifies a previous build's cached distinct count, which gets no reserve safety factor.
    void setDistinctEstimate(double estimate, bool exact = false)
    {
        hll_estimate = estimate;
        estimate_is_exact = exact;
    }
    double hllEstimate() const { return hll_estimate; }
    /// The barrier's distinct estimate, floored at one so an empty build never sizes a zero-byte table.
    size_t distinctEstimate() const { return std::max<size_t>(static_cast<size_t>(std::llround(hll_estimate)), 1); }

    /// The post-build memory verdict for a partitioned build of `rows` rows, taken once at the barrier from
    /// numbers that already exist. The join answers for the single-fill builds itself.
    enum class PostBuildPlan
    {
        Fits, /// ungrouped scatter
        Grouped, /// in-memory scatter over block ranges
        MustSpill, /// even the resident data does not fit the budget
    };
    /// `pool` is the join's post-build pool; the gate's histogram wave runs on it.
    PostBuildPlan planPostBuild(size_t rows, ThreadPool & pool);

    /// Builds the table from the fill blocks after the barrier, its waves on `pool`, the join's post-build
    /// pool. Returns whether every inserted key was unique, which drives the RightAny promotion.
    bool postBuild(size_t rows, ThreadPool & pool);
    /// `HashJoin`'s post-build conversion: a built `key32` / `key64` table whose keys span a dense range
    /// of at most 2^18 values becomes a `range*` fixed map indexed by `key - min_key`. The probe reads
    /// that map without hashing. Releases the shared table; the row refs it held are copied. Run after
    /// the build finished and before the join sizes its used flags.
    void tryConvertToFixedHashMap();
    /// Create the table, insert one block, then finish scratch and publish. A single fill thread
    /// runs the middle step per block. `reserve` is the cell count of the table created. `rows` is
    /// the row count so far, which sizes the arenas. `grow_at_max_fill_` lets walks double the table
    /// at max fill when no barrier plan will size it later.
    void beginSinglePartitionInsert(size_t reserve, size_t rows, bool grow_at_max_fill_);
    void insertSingleLaneBlock(FillBlock & fill);
    bool finishSinglePartitionInsert();
    /// The Join table engine's table: single-partition, small, grown by `emplace`; and its per-block
    /// insert, which leaves the table probe-ready. Its keys and `RowRefList` chains live in
    /// `join_table_arena`, shared by pointer with the per-query instances like the table itself.
    void createJoinTable();
    /// Returns whether a cell refers to the block: always for the list-valued shapes, and for the
    /// single-row ones when a row was the first of its key or replaced the row under
    /// `join_any_take_last_row`.
    bool insertJoinTableBlock(FillBlock & fill);
    /// Makes this clause a query's view of a Join table's: the table, its arena and its geometry are
    /// shared by pointer with `source`, the storage's clause.
    void shareTable(const HashJoinClause & source);
    /// Frees the post-build context and pool once the build is published.
    void releaseBuildScratch();
    /// Frees the table and arenas. The table goes first: cells point into the arenas and the row store.
    void releaseTable();

    /// The table reserve the plan derives from a distinct estimate: safety factor, row clamp, and the
    /// saturation clamp above `2^31` estimated words.
    size_t reserveFor(size_t rows, double distinct_estimate) const;
    UInt64 claimedTotal() const;
    /// Bytes the table and the duplicate storage will need for `rows` build rows holding `distinct`
    /// distinct keys. The post-build gate evaluates this with exact counts; the fill (the join's
    /// `predictedResidentBytes`) evaluates it with the running sketch estimate. `groups_est` is 1 for the
    /// ungrouped call and for the fill-phase gate; the grouped call receives the value `planPostBuild`
    /// computed from the ungrouped floor.
    size_t predictedTableAndArenaBytes(size_t rows, size_t distinct, bool grouped, size_t groups_est = 1) const;
    /// A pool of post-build waves. The join creates one per clause; the clause pool of a concurrent
    /// build and the join's drain into another join use one too.
    static std::unique_ptr<ThreadPool> makePostBuildPool(size_t workers);

    /// What the join's other clauses hold or will hold next to this clause's build - their tables and
    /// arenas, built or predicted. The memory gate, the grow veto and the partition floor's guard count
    /// it as resident. Set by the join before this clause is planned and before it is built.
    void setBytesReservedElsewhere(size_t bytes) { bytes_reserved_elsewhere = bytes; }

    bool hasTable() const { return table_maps != nullptr; }
    /// Which `HashJoin::MapsVariant` alternative the table mirrors.
    size_t mapsVariantIndex() const { return maps_variant_index; }
    const HashJoinTableMaps & tableMaps() const { return *table_maps; }
    /// The table's buffer bytes (drives the prefetch heuristics).
    size_t tableBytes() const { return ht_total_bytes; }
    size_t tableCells() const { return table_maps->getBufferSizeInCells(hash_join.data->type); }
    size_t tableRowCount() const { return table_maps->getTotalRowCount(hash_join.data->type); }
    /// The table's buffer plus the arenas holding its string keys and duplicate spans.
    size_t tableAndArenaBytes() const;
    size_t sizeDegree() const { return size_degree; }
    size_t partitionCount() const { return partitions; }
    bool amacEnabled() const { return amac_enabled; }
    BuildStats buildStats() const;

    /// Shrinks the reserve safety factor so the table is undersized. Growth then restores the fill, or
    /// the grow at the last free cell throws `LOGICAL_ERROR` when the budget cannot pay for the doubling.
    void setReserveSafetyFactorForTests(double factor) { reserve_safety = factor; }
    void setReserveOverrideForTests(size_t reserve) { reserve_override_for_tests = reserve; }
    void setGrowBudgetForTests(size_t bytes) { grow_budget = bytes; }
    /// The grow budget from the overflow drain on; a test uses it to lift the budget for the drain only.
    void setGrowBudgetForDrainForTests(size_t bytes) { grow_budget_for_drain_for_tests = bytes; }
    size_t predictedArenaBytesForTests(bool grouped) const;
    size_t predictedDuplicateScratchBytesForTests(size_t rows_in_range, bool first_group) const;

    /// Projection at a block-range boundary: the sketch estimate while the exact count is inside its
    /// safety band; once the count has passed the band, the larger of the count and its linear
    /// extrapolation.
    static UInt64
    boundaryProjection(UInt64 claimed_total, UInt64 rows_inserted, UInt64 insertable, double hll_estimate, double reserve_safety);
    /// Pins both phases onto the sequential loops, so tests can cross-check the ring against them.
    void setAmacEnabledForTests(bool value) { amac_enabled = value; }
    /// The L1 partition cap binds only past a few hundred partitions, a build too large for a unit test.
    /// A tiny L1 makes it bind on a small one.
    void setL1CacheSizeForTests(size_t bytes) { l1_cache_bytes_for_tests = bytes; }
    /// Forces the partition count (clamped to the table's degree), so plans of thousands of partitions
    /// can be executed on a build that fits a test.
    void setPartitionBitsForTests(size_t value) { forced_bits_for_tests = value; }

    /// Wrapping inserts call this before every row's walk; it grows the table when the walk would take
    /// the last free cell. Public only because the insert target in the translation unit calls it.
    template <typename Target>
    bool growBeforeLastFreeCell(Target & target);

private:
    void computeRoutesImpl(FillBlock & fill, DenseHyperLogLog * sketch) const;
    /// Shared across the post-build stages: histogram, allocate, scatter, owner waves, drain.
    struct PostBuildContext;
    /// Out-of-line so `unique_ptr<PostBuildContext>` can be destroyed from TUs that only see the
    /// forward declaration.
    struct PostBuildContextDeleter
    {
        void operator()(PostBuildContext * ctx) const;
    };

    /// Both return whether every inserted key was unique, which drives the RightAny promotion.
    bool postBuildPartitioned();
    bool postBuildSinglePartition(size_t rows);
    void preparePostBuildContext(ThreadPool & pool);
    void runGroupStages(size_t block_begin, size_t block_end);
    size_t chunkBytesForBlockRange(size_t b0, size_t b1) const;

    size_t predictedArenaBytes(size_t insertable_rows, bool grouped) const;
    size_t duplicateScratchBytesForRows(size_t rows_in_range, bool first_group) const;
    /// The scratch a block range needs at once: `workers` live partitions plus the drain's.
    size_t duplicateScratchBytesForRange(size_t rows_in_range, bool first_group) const;
    /// The buffer degree for `reserve` cells; throws past 2^32 cells.
    size_t sizeDegreeFor(size_t reserve) const;
    /// The safety factor a table reserve gets over its distinct estimate: none over an exact cached count.
    double reserveSafety() const { return estimate_is_exact ? 1.0 : reserve_safety; }
    /// Rows the partitioned inserts will see: the sum of the exact per-partition counts.
    UInt64 insertableRows() const;
    /// Bytes still held by the saved routes of every clause; a clause's scatter releases its own.
    size_t routeBytes() const;

    /// How the key columns are scattered. Fixed-width keys go by their raw bytes. Anything else
    /// (`String`, `LowCardinality`, ...) goes through `ColumnsScatter`, with an 8-byte hash word per
    /// column in the chunk.
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

    void measureGenericKeyBytes();
    void createHashJoinTable();
    /// The partition floor's memory guard: the scatter transient it introduces for `rows` rows has to fit
    /// the memory budget next to what is resident already (the post-build gate's ungrouped peak).
    bool partitionFloorFitsMemory(size_t floor_bits, size_t floor_degree, size_t rows) const;
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
    void drainOverflow(PostBuildContext & ctx);
    /// Why the table doubles: the walk is about to take the last free cell (this grow cannot be
    /// refused), or the projected fill exceeds the load factor. The load-factor grow is skipped at the
    /// 2^32-cell cap and under a tight budget.
    enum class GrowReason : UInt8
    {
        LastFreeCell,
        LoadFactor,
    };
    /// Read only between block ranges and at a grow: the join's byte count plus the overflow buffers and
    /// the pass scratch of the post-build context.
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

    /// Inserts one compact section - rows `[first_row, first_row + rows)` of `key_columns` - into partition
    /// `partition`'s range for `worker`. When `partition` is `single_partition`, inserts into the whole
    /// table from the stored blocks. That is the only path where `skip_bytes` applies and where a section
    /// starts past row 0. Row i's stored ref is `locators[i]`, the decoded `narrow_locators[i]`, or
    /// `RowRef(block_no, i)` when neither is set.
    static constexpr size_t single_partition = std::numeric_limits<size_t>::max();
    void insertPartitionSection(
        PostBuildContext & ctx,
        size_t worker,
        size_t partition,
        const ColumnRawPtrs & key_columns,
        size_t first_row,
        size_t rows,
        const UInt64 * locators,
        const UInt32 * narrow_locators_data,
        UInt32 block_no,
        const UInt8 * skip_bytes);

    /// Decided once, after the table is sized and before the inserts, on the same heuristics that
    /// enable the software prefetch.
    void decideAmacEngagement();

    /// The join's helper: map type, key sizes, kind and strictness, and the block store the inserted
    /// references point into.
    HashJoin & hash_join;
    const TableJoin & table_join;
    /// This clause's index in `table_join.getClauses()`, `hash_join.key_sizes` and `FillBlock::clauses`.
    const size_t clause_idx;
    const bool any_take_last_row;
    const size_t num_threads;
    /// Zero disables the gate; post-build is the ungrouped scatter.
    const size_t max_bytes_before_external_join;
    /// Same as the constructor budget unless a test lifts or tightens it to force or refuse a grow.
    size_t grow_budget = 0;
    /// See `setBytesReservedElsewhere`.
    size_t bytes_reserved_elsewhere = 0;
    std::optional<size_t> grow_budget_for_drain_for_tests;
    /// The join's concatenated fill blocks (one list for every clause), read by the post-build stages
    /// and released block by block as they are consumed.
    std::vector<FillBlock> & build_blocks;
    /// The join's byte count of the stored blocks and route transients, which the scatter releases into.
    std::atomic<size_t> & accumulated_bytes;
    std::optional<size_t> reserve_override_for_tests;

    /// Which `HashJoin::MapsVariant` alternative is active; the table mirrors it.
    const size_t maps_variant_index;

    size_t bits = 0;
    size_t partitions = 1;
    /// MSB-first slices of the route word, summing to `bits`.
    std::vector<size_t> pass_bits;
    size_t max_fanout_per_pass;
    bool cap_partitions_by_l1_descriptors;
    /// `parallel_hash_join_threshold`: from this many build rows on, the insert phase gets at least one
    /// partition per worker, as `parallel_hash` gets one table per slot.
    size_t parallel_hash_join_threshold;
    /// `enable_join_fixed_hash_table_conversion`.
    const bool fixed_hash_table_conversion_enabled;
    std::optional<size_t> l1_cache_bytes_for_tests;
    std::optional<size_t> forced_bits_for_tests;
    double hll_estimate = 0;
    /// Set when `hll_estimate` is a previous run's exact distinct count from the hash table statistics
    /// cache rather than the sketch's estimate; see `setDistinctEstimate`.
    bool estimate_is_exact = false;
    /// Reserve factor over the sketch estimate. Also the multiplicity band below which the arena
    /// prediction treats the build as unique (`predictedTableAndArenaBytes`). That second use needs
    /// the wide margin. The ~1.15% sketch error alone would not. An exact count from the cache gets no
    /// factor (`reserveSafety`).
    double reserve_safety = 1.2;
    /// The table's buffer degree, fixed at the barrier: `2^size_degree` cells, `2^bits` ranges.
    size_t size_degree = 0;
    /// The build's row count when the table was sized (`decidePartitionPlan`, or the single-partition
    /// table's creation); the memory predictions read it.
    size_t total_rows = 0;
    /// Set for the fill-time single-partition insert: the walks double the table at max fill, since no
    /// barrier plan sizes it afterwards.
    bool grow_at_max_fill = false;
    /// When every block and row number fits 16 bits, the scattered locator column packs into
    /// `(block_no << 16) | row_no`. It is decoded at insert. That halves the largest scatter transient.
    bool narrow_locators = false;

    /// The one table. `build_arenas` hold the string keys and the duplicate spans the cells point at,
    /// so they must outlive it: one arena per build worker plus one for the drain. A Join table's
    /// instance keeps its keys and `Batch` chains in `join_table_arena` instead; both are shared by
    /// pointer with the per-query instances.
    std::shared_ptr<HashJoinTableMaps> table_maps;
    std::deque<Arena> build_arenas;
    std::shared_ptr<Arena> join_table_arena;
    size_t ht_total_bytes = 0; /// the table's buffer bytes (drives the prefetch heuristics)

    /// The join's pool, set for the duration of the post-build waves (`planPostBuild`, `postBuild`).
    ThreadPool * post_build_pool = nullptr;
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

    /// `amac_enabled` is the switch; `amac_build_engaged` the decision taken before the owner inserts.
    bool amac_enabled = true;
    bool amac_build_engaged = false;

    BuildStats stats;

    LoggerPtr log;
};

}
