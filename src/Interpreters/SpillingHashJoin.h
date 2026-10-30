#pragma once

#include <atomic>
#include <mutex>

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/SpillableAdapter.h>
#include <Common/SharedMutex.h>


namespace DB
{

class HashJoin;
class GraceHashJoin;
class ConcurrentHashJoin;
class MergeJoin;

/// An IJoin wrapper that automatically switches to an external join to spill to disk when memory limits are exceeded.
/// The external join is GraceHashJoin, or MergeJoin (partial merge) when the query plan requires
/// the left stream's original row order (`keepLeftPipelineInOrder`): GraceHashJoin scatters left
/// rows into buckets by hash and returns them per bucket, while MergeJoin spills only the sorted
/// right side and streams every left block through exactly once, in order.
///
/// Operates in two modes depending on the constructor parameters:
///
/// Single-thread mode:
/// Blocks are fed directly into a HashJoin instance during the build phase.
/// If the data exceeds max_bytes_before_external_join, the blocks are extracted via releaseJoinedBlocks and drained into a new
/// GraceHashJoin (or MergeJoin when pinned to the left order).
/// If all blocks fit in memory, the HashJoin is promoted to chosen_join with zero rework.
///
/// Concurrent mode:
/// Blocks are fed into a ConcurrentHashJoin from multiple threads concurrently.
/// A SharedMutex protects the COLLECTING -> GRACE_HASH_JOIN / MERGE_JOIN transition: addBlockToJoin takes a shared lock, while
/// switchToExternalJoin takes an exclusive lock.
/// If the data exceeds max_bytes_before_external_join, the external join is created and ConcurrentHashJoin slots are converted via
/// addBlockToJoin calls possibly from multiple threads. Inserts into a MergeJoin target are serialized
/// by merge_join_insert_mutex, so its build stays single-threaded; GraceHashJoin inserts stay concurrent.
/// If all blocks fit in memory, the ConcurrentHashJoin is promoted to chosen_join with zero rework.
///
/// While not pinned, hasDelayedBlocks returns true so that the pipeline includes the
/// delayed-block transforms needed by GraceHashJoin. When HashJoin / ConcurrentHashJoin is used,
/// isDelayedBlocksProcessingEnabled is false and the probe streams close those transforms after the build.
/// The read-in-order-through-join optimisation in optimizeReadInOrder.cpp asks
/// canPreserveLeftBlockOrder first and, if the in-order plan is chosen, pins this join with
/// keepLeftPipelineInOrder: from then on the spill fallback is MergeJoin, no delayed-block
/// transforms are added, and the output keeps the left stream's order.
class SpillingHashJoin final : public IJoin
{
public:
    /// Single-thread mode: wraps a HashJoin.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        bool any_take_last_row_ = false);

    /// Concurrent mode: wraps a ConcurrentHashJoin.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        size_t concurrent_slots_,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        bool any_take_last_row_ = false);

    ~SpillingHashJoin() override;

    std::string getName() const override;
    std::string getAlgorithm() const override;
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override { return any_take_last_row; }

    bool addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context) override;
    void checkTypesOfKeys(const Block & block) const override;
    void initialize(const Block & sample_block) override;
    JoinResultPtr joinBlock(Block block) override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    StepAnalysisReport getAnalysisReport() const override;

    ISpillable * getSpillable() override { return &spillable; }
    /// While collecting: the whole right side, spilled by switching to the grace join.
    /// After the switch: the grace join and unconverted concurrent slots. Nothing once the build ended in memory.
    ProcessorMemoryStats getMemoryStats() const;
    size_t spill(size_t at_least_bytes);

    bool supportParallelJoin() const override { return concurrent_join != nullptr; }
    bool supportParallelNonJoinedBlocksProcessing() const override;
    bool isParallelNonJoinedProcessingEnabled() const override;

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block,
        const Block & result_sample_block,
        UInt64 max_block_size,
        size_t stream_idx,
        size_t num_streams) const override;

    IBlocksStreamPtr getDelayedBlocks() override;
    /// Pinned to the left order, the spill fallback is MergeJoin, which needs no delayed-block
    /// transforms (and their outputs would be interleaved with the main ones, breaking the order).
    bool hasDelayedBlocks() const override { return !keep_left_block_order; }
    bool isDelayedBlocksProcessingEnabled() const override;

    bool preservesLeftBlockOrder() const override { return keep_left_block_order; }
    bool canPreserveLeftBlockOrder() const override;
    void keepLeftPipelineInOrder() override;

    void onBuildPhaseFinish() override;
    void onProbePhaseFinish(std::optional<size_t> matched_right_rows) override;

    /// Forwarded to the join actually chosen in `onBuildPhaseFinish`, so that an in-memory
    /// `HashJoin` still gets its post-build optimizations (right-table reranging, conversion to a
    /// fixed hash map, publishing the shared runtime filter).
    /// After a spill `chosen_join` is a `GraceHashJoin`, which does not override these methods, so
    /// forwarding keeps the spilled path exactly as it is today: `GraceHashJoin` itself runs the
    /// post-build phase only when the right table ended up in a single bucket. Multi-bucket spills
    /// skip it, because a hash table holding one bucket cannot produce a runtime filter valid for
    /// the whole right table.
    bool hasPostBuildPhase() const override;
    void runPostBuildPhase() override;

    void setEnableLazyColumnsIndexing(bool value) override;

private:
    SpillableAdapter<SpillingHashJoin> spillable{*this};

    enum class State
    {
        COLLECTING, // Right-side blocks are being collected in HashJoin / ConcurrentHashJoin, no spilling yet.
        GRACE_HASH_JOIN, // Spilled to disk and switched to GraceHashJoin, but some concurrent slots may still be unconverted.
        MERGE_JOIN, // Spilled to disk and switched to MergeJoin (pinned to the left order), slots may still be unconverted.
        IN_MEMORY_JOIN // All blocks fit in memory, using HashJoin / ConcurrentHashJoin directly without switching.
    };

    /// Creates the external join (MergeJoin when pinned to the left order, GraceHashJoin
    /// otherwise) and drains the collected right blocks into it. `spill_immediately` is for the
    /// memory-pressure path: a new GraceHashJoin then repartitions as it takes the data over,
    /// instead of holding all of it in bucket 0 until the next spill request (MergeJoin always
    /// puts the drained blocks on disk right away).
    void switchToExternalJoin(JoinBuildContext context, bool spill_immediately = false);
    /// Every insert into the external join funnels through here: MergeJoin inserts are
    /// serialized by `merge_join_insert_mutex` (single-threaded build), GraceHashJoin inserts
    /// stay concurrent.
    bool addBlockToExternalJoin(const Block & block, size_t num_rows, JoinBuildContext context);
    void tryConvertSlots(JoinBuildContext context);

    LoggerPtr log;
    std::shared_ptr<TableJoin> table_join;
    SharedHeader left_sample_block;
    Block right_sample_block;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t initial_num_buckets;
    size_t max_num_buckets;
    bool any_take_last_row;
    size_t max_bytes_before_external_join;

    SharedMutex switch_mutex;
    std::atomic<size_t> next_slot_to_convert{0};
    /// Serializes the inserts into the pinned MergeJoin fallback: its build stays
    /// single-threaded even though multiple build threads hand their blocks over (see
    /// `addBlockToExternalJoin`).
    std::mutex merge_join_insert_mutex;
    mutable std::mutex totals_mutex;
    bool supports_parallel_non_joined_blocks_processing{false};

    /// The query plan requires the left stream's original row order: the spill fallback is pinned
    /// to MergeJoin instead of GraceHashJoin. Set by `keepLeftPipelineInOrder` during query plan
    /// optimization, before the query pipeline runs, so plain bool is enough.
    bool keep_left_block_order = false;

    std::atomic<State> state{State::COLLECTING};

    /// HashJoin that stores right-side blocks during COLLECTING phase (single-thread mode).
    std::shared_ptr<HashJoin> hash_join;

    /// ConcurrentHashJoin for multi-thread path (mutually exclusive with hash_join).
    std::shared_ptr<ConcurrentHashJoin> concurrent_join;

    /// GraceHashJoin created during overflow. Also assigned to chosen_join.
    std::shared_ptr<GraceHashJoin> grace_join;

    /// MergeJoin created during overflow when pinned to the left order. Also assigned to chosen_join.
    std::shared_ptr<MergeJoin> merge_join;

    /// The real join, created when switching out of COLLECTING state.
    JoinPtr chosen_join;
};

}
