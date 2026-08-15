#pragma once

#include <Core/Block_fwd.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>

#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

class TableJoin;

/** RadixHashJoin — a radix-partitioned hash join exposed as `join_algorithm = 'radix_join'`.
  *
  * It targets the case where the build-side hash table working set exceeds last-level cache, for a
  * join whose key and payload columns are all fixed-width. The build side is radix-partitioned into
  * many small hash tables that each stay L2-resident, and the probe side is streamed against them in
  * budgeted waves so that each leaf table is loaded into cache once and hit by a long contiguous run
  * of probe rows. Where `parallel_hash` probes one shared map that has fallen out of LLC (a cold miss
  * per lookup), this probes a cache-hot leaf — that lookup locality is the win.
  *
  * v1 integrates the benchmark implementation (`src/Common/benchmarks/{radix_hash_join_bench,
  * hash_join_bench}.cpp`): the whole build side (keys and payload) is physically radix-scattered into
  * one exactly-sized chunk per leaf partition, and one real `HashJoin` is built per partition with an
  * exact reserve. The probe side is buffered and, once a probe-buffer budget is reached, radix-
  * scattered with the same kernels and probed against every touched leaf's `HashJoin`. Correctness
  * comes from delegating per-partition build/probe/emit to those `HashJoin` instances.
  *
  * The planner gate (`radixHashJoinApplicable` in PlannerJoins.cpp) admits only:
  *   - a single-disjunct inner ALL equi-join with no special storage,
  *   - join key columns that are all fixed-width, non-nullable, non-LowCardinality, whose packed
  *     width (the sum of the column widths) is a multiple of 4 in [4, 64], and
  *   - all columns of both sides fixed-width (so they scatter as raw bytes).
  * Anything else falls back to `parallel_hash` (or plain `hash` where even that shape does not hold).
  * The constructor re-checks these and throws a LOGICAL_ERROR if violated (rather than silently
  * degrading).
  *
  * Lifecycle:
  *   addBlockToJoin      accumulate the right block (move) into a per-lane block store; no scatter.
  *   onBuildPhaseFinish  the cheap build barrier only: concatenate the per-lane block stores. Runs
  *                       inside the last filling transform's prepare(), which must stay cheap (D-0003).
  *   runPostBuildPhase   the heavy post-build, parallelised over a dedicated `ThreadPool`: the radix
  *                       scatter of the whole build side to its leaf chunks, and one exactly-reserved
  *                       `HashJoin` built per non-empty partition.
  *   joinBlock           admit the probe block into the one shared wave against the byte budget; the
  *                       admission that crosses the budget seals the wave, and the same probe lanes
  *                       that admit also drain it cooperatively: each result's next() claims pending
  *                       wave work (sizing, stable scatter, refine passes, per-leaf probes) and
  *                       returns that lane's own output. No probe-side pool, queue, or dedicated
  *                       worker crews exist. Before the build barrier (the header/planning path) it
  *                       delegates to a schema-only `HashJoin`.
  *   getDelayedBlocks    after all probe input, seals the final partial wave and drains it through
  *                       the standard delayed-blocks mechanism (the `GraceHashJoin` path) — a thin
  *                       stream over the same wave machine, pulled concurrently by the executor's
  *                       delayed-worker transforms.
  */
class RadixHashJoin : public IJoin
{
public:
    RadixHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        size_t max_threads_,
        std::optional<UInt64> rhs_size_estimation_,
        UInt64 max_partitions_per_pass_,
        bool size_tables_by_distinct_estimate_,
        double probe_buffer_fraction_,
        UInt64 probe_buffer_min_bytes_,
        UInt64 probe_buffer_max_bytes_,
        const StatsCollectingParams & stats_collecting_params_);

    ~RadixHashJoin() override;

    std::string getName() const override { return "RadixHashJoin"; }
    const TableJoin & getTableJoin() const override;

    /// Build is parallel: the build path only accumulates blocks into per-lane stores (no shared map).
    bool supportParallelJoin() const override { return true; }

    bool addBlockToJoin(const Block & block, bool check_limits) override;
    bool addBlockToJoin(const Block & block, size_t num_rows, bool check_limits) override;
    bool addBlockToJoin(const Block & block, size_t num_rows, bool check_limits, size_t build_lane) override;

    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;
    JoinResultPtr joinBlock(Block block, size_t lane) override;

    /// The parallel build transforms each call setTotals concurrently on this shared object; serialize
    /// the assignment (the base does an unguarded `totals = block`). getTotals stays unlocked (read
    /// only after the build completes).
    void setTotals(const Block & block) override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// D-0003 split: `onBuildPhaseFinish` runs in the last filling transform's prepare() on this tree,
    /// so it does only the cheap build barrier; the heavy scatter runs in `runPostBuildPhase` (a work()
    /// quantum, timed under `JoinBuildPostProcessingMicroseconds`).
    void onBuildPhaseFinish() override;
    bool hasPostBuildPhase() const override { return true; }
    void runPostBuildPhase() override;

    /// The final partial probe window is flushed through the standard delayed-blocks mechanism, so the
    /// executor drives its probe across all delayed-worker transforms in parallel.
    bool hasDelayedBlocks() const override { return true; }
    IBlocksStreamPtr getDelayedBlocks() override;

    void setEnableLazyColumnsIndexing(bool value) override;

private:
    std::shared_ptr<TableJoin> table_join;
    SharedHeader right_sample_block;

    size_t max_threads;
    std::optional<UInt64> rhs_size_estimation;
    UInt64 max_partitions_per_pass;
    /// When true, leaf hash tables would be sized by a per-leaf HLL distinct-key estimate rather than by
    /// row count. No-op in v1: leaf tables are already sized exactly from the scatter histogram. Gated by
    /// setting `radix_join_size_tables_by_distinct_estimate`.
    bool size_tables_by_distinct_estimate;

    double probe_buffer_fraction;
    UInt64 probe_buffer_min_bytes;
    UInt64 probe_buffer_max_bytes;

    /// Cross-run hash-table statistics ("the stats"): keyed by the query plan. No-op in v1 (leaf tables
    /// are sized exactly from the histogram); kept for a future distinct-estimate sizing path.
    StatsCollectingParams stats_collecting_params;

    std::mutex totals_mutex;

    /// All radix-path state lives in the .cpp so this header stays free of the internals.
    struct State;
    std::unique_ptr<State> state;
};

}
