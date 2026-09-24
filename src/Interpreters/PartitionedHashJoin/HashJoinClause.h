#pragma once

#include <Columns/ColumnNullable.h>
#include <Interpreters/HashJoin/HashJoin.h>
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
  * blocks. The join owns the store, fill lanes, used flags and the probe. One instance per join
  * today. Several disjuncts are meant to share the store with one clause each.
  */
class HashJoinClause
{
public:
    /// One right-side block: stored payload, prepared keys, and saved routes.
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
        /// Null-key rows OR mask-filtered rows. Materialized only when the mask actually filters.
        /// Otherwise `skipData` returns the plain null map.
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

        /// Drops the prepared keys, masks and routes once the rows are inserted or scattered. The stored
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
        /// Per-partition claimed buffer cells at publication, excluding the zero cell.
        std::vector<UInt64> claimed_per_partition;
        /// Peak logical occupancy of any one pass scratch (`PassScratch::usedBytes`).
        size_t scratch_used_high_water = 0;
        /// Table growth. Zero until a grow runs.
        UInt64 table_resizes = 0;
        /// Stored blocks whose fixed-width payload went into a row store; counted by the join.
        UInt64 row_store_blocks = 0;
    };

    /// `hash_join_` is the schema helper; `build_blocks_` is the join's fill list; freed route bytes
    /// go back into `accumulated_bytes_`.
    HashJoinClause(
        HashJoin & hash_join_,
        const TableJoin & table_join,
        bool any_take_last_row_,
        size_t num_threads_,
        std::vector<FillBlock> & build_blocks_,
        std::atomic<size_t> & accumulated_bytes_,
        LoggerPtr log_);
    ~HashJoinClause();

    /// One map hash per insertable row. The top 16 bits of the placement word are the route.
    /// The top 32 bits of its mix are fed to `sketch`.
    void computeRoutes(FillBlock & fill, DenseHyperLogLog & sketch) const;
    void computeRoutes(FillBlock & fill) const;

    /// The barrier's decision for `rows` build rows, from the distinct estimate set with
    /// `setDistinctEstimate`: the table degree, the partition count and the scatter passes.
    void decidePartitionPlan(size_t rows);
    /// `exact` identifies a previous build's exact cached count, which gets no reserve safety factor.
    void setDistinctEstimate(double estimate, bool exact = false)
    {
        hll_estimate = estimate;
        estimate_is_exact = exact;
    }
    double hllEstimate() const { return hll_estimate; }
    /// The barrier's distinct estimate, floored at one so an empty build never sizes a zero-byte table.
    size_t distinctEstimate() const { return std::max<size_t>(static_cast<size_t>(std::llround(hll_estimate)), 1); }

    /// Builds the table from the fill blocks after the barrier. Returns whether every inserted key was
    /// unique, which drives the RightAny promotion.
    bool postBuild(size_t rows);
    /// Create the table, insert one block, then finish scratch and publish. A single fill thread
    /// runs the middle step per block. `reserve` is the cell count of the table created. `rows` is
    /// the row count so far, which sizes the arenas. `grow_at_max_fill_` lets walks double the table
    /// at max fill when no barrier plan will size it later.
    void beginSinglePartitionInsert(size_t reserve, size_t rows, bool grow_at_max_fill_);
    void insertSingleLaneBlock(FillBlock & fill);
    bool finishSinglePartitionInsert();
    /// Frees the post-build context and pool once the build is published.
    void releaseBuildScratch();
    /// Frees the table and arenas. The table goes first: cells point into the arenas and the row store.
    void releaseTable();

    /// The table reserve the plan derives from a distinct estimate: safety factor, row clamp, and the
    /// saturation clamp above `2^31` estimated words.
    size_t reserveFor(size_t rows, double distinct_estimate) const;
    UInt64 claimedTotal() const;

    bool hasTable() const { return table_maps != nullptr; }
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

    /// Shrinks the reserve safety factor so the table is undersized. Growth then restores the fill.
    void setReserveSafetyFactorForTests(double factor) { reserve_safety = factor; }
    void setReserveOverrideForTests(size_t reserve) { reserve_override_for_tests = reserve; }
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
    double reserveSafety() const { return estimate_is_exact ? 1.0 : reserve_safety; }
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
    void preparePostBuildContext();
    void runGroupStages(size_t block_begin, size_t block_end);

    /// Bytes the table and the duplicate storage will need for `rows` build rows holding `distinct`
    /// distinct keys, evaluated with the barrier's distinct estimate.
    size_t predictedTableAndArenaBytes(size_t rows, size_t distinct) const;
    size_t predictedArenaBytes(size_t insertable_rows) const;
    /// The buffer degree for `reserve` cells; throws past 2^32 cells.
    size_t sizeDegreeFor(size_t reserve) const;
    /// Rows the partitioned inserts will see: the sum of the exact per-partition counts.
    UInt64 insertableRows() const;

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
    static std::unique_ptr<ThreadPool> makePostBuildPool(size_t workers);

    void measureGenericKeyBytes();
    void createHashJoinTable();
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
    /// refused), or the projected fill exceeds the load factor. The load-factor grow is skipped
    /// only at the 2^32-cell cap.
    enum class GrowReason : UInt8
    {
        LastFreeCell,
        LoadFactor,
    };
    void grow(UInt64 occupied, UInt64 projected, GrowReason reason);
    template <typename Table>
    void growHashJoinTable(Table & table, UInt64 occupied, UInt64 projected, GrowReason reason);
    void maybeGrowForLoadFactor(UInt64 projected);
    /// Finishes one pass's scratch into spans. Returns the scratch's logical occupancy just before
    /// the finish (0 when the scratch was empty), so callers can fold a per-worker high water.
    size_t finishPassScratch(PassScratch & scratch, SpanWriter & writer);
    template <typename Table>
    void verifyPublishedTable(const Table & table) const;
    /// Accounts and pre-faults one partition's cell range (see `RangeCommittedBuffer`).
    void commitRange(size_t partition);
    /// Sets the table's distinct-key count from the owners' and the drain's claims.
    void publishTableSize(const PostBuildContext & ctx);

    /// Inserts one compact section of `rows` rows into partition `partition`'s range for `worker`.
    /// When `partition` is `single_partition`, inserts into the whole table from the stored blocks.
    /// That is the only path where `skip_bytes` applies. Row i's stored ref is `locators[i]`, the
    /// decoded `narrow_locators[i]`, or `RowRef(block_no, i)` when neither is set.
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

    /// Decided once, after the table is sized and before the inserts, on the same heuristics that
    /// enable the software prefetch.
    void decideAmacEngagement();

    /// The join's helper: map type, key sizes, kind and strictness, and the block store the inserted
    /// references point into.
    HashJoin & hash_join;
    const bool any_take_last_row;
    const size_t num_threads;
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
    std::optional<size_t> l1_cache_bytes_for_tests;
    std::optional<size_t> forced_bits_for_tests;
    double hll_estimate = 0;
    bool estimate_is_exact = false;
    /// Reserve factor over a sketch estimate. Also the multiplicity band below which the arena
    /// prediction treats the build as unique (`predictedTableAndArenaBytes`). That second use needs
    /// the wide margin. The ~1.15% sketch error alone would not.
    double reserve_safety = 1.2;
    /// The table's buffer degree, fixed at the barrier: `2^size_degree` cells, `2^bits` ranges.
    size_t size_degree = 0;
    /// Set for the fill-time single-partition insert: the walks double the table at max fill, since no
    /// barrier plan sizes it afterwards.
    bool grow_at_max_fill = false;
    /// When every block and row number fits 16 bits, the scattered locator column packs into
    /// `(block_no << 16) | row_no`. It is decoded at insert. That halves the largest scatter transient.
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
    /// Prepared key-column bytes across the whole build, measured once before the arenas are sized.
    /// Zero unless the keys are variable-length, which is when they are copied into the arena.
    size_t generic_key_bytes = 0;
    /// Set by `preparePostBuildContext` when the histogram already covers the whole build at the
    /// pass-1 width, so the scatter does not scan the routes twice.
    bool histogram_covers_full_build = false;

    /// `amac_enabled` is the switch; `amac_build_engaged` the decision taken before the owner inserts.
    bool amac_enabled = true;
    bool amac_build_engaged = false;

    BuildStats stats;

    LoggerPtr log;
};

}
