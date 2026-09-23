#pragma once

#include <atomic>
#include <mutex>
#include <optional>

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/SharedMutex.h>


namespace DB
{

class GraceHashJoin;
class HashJoin;

/// An IJoin wrapper that automatically switches to GraceHashJoin to spill to disk when memory limits are exceeded.
///
/// The build phase feeds one in-memory HashJoin, which builds its table only at the
/// barrier, so the overflow check reads `predictedResidentBytes` instead of `getTotalByteCount`. On
/// overflow the fill lanes are handed to GraceHashJoin one block at a time (`tryConvertFillLanes`).
/// A build that stays under the threshold runs its barrier and asks `planPostBuild`: a `MustSpill`
/// verdict still switches, otherwise the HashJoin becomes chosen_join with no rework at all.
///
/// A SharedMutex protects the COLLECTING -> GRACE_HASH_JOIN transition.
/// `addBlockToJoin` takes a shared lock; `switchToGraceHashJoin` takes an exclusive lock.
/// That way no block can land in a join that is being drained.
///
/// `hasDelayedBlocks` always returns true so that the pipeline includes the delayed-block
/// transforms needed by `GraceHashJoin`. When the in-memory join is kept, `getDelayedBlocks` returns
/// nullptr and the delayed transforms finish instantly.
/// Because `hasDelayedBlocks` returns true, the read-in-order-through-join optimisation
/// in `optimizeReadInOrder.cpp` does not propagate through `SpillingHashJoin` (same as
/// `GraceHashJoin`). Spilling may reorder rows.
class SpillingHashJoin final : public IJoin
{
public:
    /// `build_rows_hint_` is the planner's right-side row estimate, see `HashJoin`.
    SpillingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t initial_num_buckets_,
        size_t max_num_buckets_,
        size_t num_threads_,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        bool any_take_last_row_ = false,
        std::optional<size_t> build_rows_hint_ = {});

    ~SpillingHashJoin() override;

    std::string getName() const override;
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override { return any_take_last_row; }

    bool addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void initialize(const Block & sample_block) override;
    JoinResultPtr joinBlock(Block block) override;
    JoinResultPtr joinBlock(Block block, size_t lane) override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;

    StepAnalysisReport getAnalysisReport() const override;

    bool supportParallelJoin() const override;
    bool emitsSizedOutputBlocks() const override;
    size_t getMaxBuildThreads() const override { return max_threads; }
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
    bool hasDelayedBlocks() const override { return true; }

    void onBuildPhaseFinish() override;
    void onProbePhaseFinish(std::optional<size_t> matched_right_rows) override;

    /// Forwarded to the join actually chosen in `onBuildPhaseFinish`.
    /// After a spill `chosen_join` is a `GraceHashJoin`. That class does not override these methods.
    /// Forwarding keeps the spilled path exactly as it is today.
    /// `GraceHashJoin` itself runs the post-build phase only when the right table ended up in a
    /// single bucket. Multi-bucket spills skip it: a hash table holding one bucket cannot produce
    /// a runtime filter valid for the whole right table.
    bool canSpillToDisk() const override { return true; }
    size_t getSpillableBytes() const override;
    void requestSpill() override;

    bool hasPostBuildPhase() const override;
    void runPostBuildPhase() override;

    void setEnableLazyColumnsIndexing(bool value) override;

private:
    enum class State
    {
        COLLECTING, // Right-side blocks are being collected in the HashJoin, no spilling yet.
        GRACE_HASH_JOIN, // Spilled to disk and switched to GraceHashJoin, but some fill lanes may still be unconverted.
        IN_MEMORY_JOIN // All blocks fit in memory, using the HashJoin directly without switching.
    };

    /// `spill_immediately` is for the memory-pressure path: the new GraceHashJoin repartitions as it
    /// takes the data over, instead of holding all of it in bucket 0 until the next spill request.
    void switchToGraceHashJoin(size_t worker_id, bool spill_immediately = false);
    /// Shared by the overflow switch and the `MustSpill` case after the build. The `MustSpill` case must
    /// not call `switchToGraceHashJoin`: that would drain build lanes `onBuildPhaseFinish` already consumed.
    /// A non-zero `initial_buckets_hint` raises the starting bucket count; `GraceHashJoin` rounds it up to
    /// a power of two and clamps it to the maximum.
    void createGraceJoin(size_t initial_buckets_hint = 0);
    /// Hands the in-memory join's fill lanes to `grace_join`.
    void tryConvertFillLanes(size_t worker_id);

    LoggerPtr log;
    std::shared_ptr<TableJoin> table_join;
    SharedHeader left_sample_block;
    Block right_sample_block;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t initial_num_buckets;
    size_t max_num_buckets;
    bool any_take_last_row;
    size_t max_bytes_before_external_join;
    const size_t max_threads;

    SharedMutex switch_mutex;
    std::atomic<size_t> next_fill_lane_to_convert{0};
    mutable std::mutex totals_mutex;
    bool supports_parallel_non_joined_blocks_processing{false};

    std::atomic<State> state{State::COLLECTING};

    /// The in-memory join that collects the right blocks.
    std::shared_ptr<HashJoin> partitioned_join;

    /// GraceHashJoin created during overflow. Also assigned to chosen_join.
    std::shared_ptr<GraceHashJoin> grace_join;

    /// The real join, created when switching out of COLLECTING state.
    JoinPtr chosen_join;
};

}
