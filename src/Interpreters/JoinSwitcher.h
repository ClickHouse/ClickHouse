#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>

#include <Core/Block.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/SharedMutex.h>


namespace DB
{

/// Used when setting 'join_algorithm' set to JoinAlgorithm::AUTO.
/// Starts JOIN with join-in-memory algorithm and switches to join-on-disk on the fly if there's no memory to place right table.
/// Current join-in-memory and join-on-disk are JoinAlgorithm::HASH and JoinAlgorithm::PARTIAL_MERGE joins respectively.
///
/// The hash phase uses the same `parallel_hash_join_threshold` layout as a bare `HashJoin`.
/// Concurrent fill takes a shared lock; draining onto `MergeJoin` takes an exclusive lock
/// because `MergeJoin::addBlockToJoin` is not concurrent. After a switch, probe is serialized
/// for the same reason: `supportParallelJoin` is fixed at plan time.
///
/// Unmatched RIGHT/FULL rows: `supportParallelNonJoinedBlocksProcessing` is captured from
/// the inner `HashJoin` so the pipeline wires `NonJoinedBlocksTransform`. After a drain
/// the 5-arg `getNonJoinedBlocks` still forwards; `MergeJoin` does not override it, so
/// `IJoin`'s default puts every unmatched row on stream 0.
///
/// Every access to `join` after construction takes `switch_mutex`. Totals live on this wrapper:
/// `FillingRightJoinSideTransform` calls `setTotals` on every filler at EOF, including while
/// another filler is still inserting and may replace `join`.
class JoinSwitcher : public IJoin
{
public:
    JoinSwitcher(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        bool any_take_last_row_,
        const HashJoinStatsCollectingParams & stats_collecting_params_ = {},
        size_t max_threads_ = 1,
        bool use_parallel_layout_ = false);

    std::string getName() const override { return "JoinSwitcher"; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override
    {
        std::shared_lock lock(switch_mutex);
        return join->anyTakeLastRow();
    }

    /// Add block of data from right hand of JOIN into current join object.
    /// If join-in-memory memory limit exceeded switches to join-on-disk and continue with it.
    /// @returns false, if join-on-disk disk limit exceeded
    bool addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override
    {
        std::shared_lock lock(switch_mutex);
        join->checkTypesOfKeys(block);
    }

    JoinResultPtr joinBlock(Block block) override;

    const Block & getTotals() const override
    {
        std::shared_lock lock(switch_mutex);
        return IJoin::getTotals();
    }

    void setTotals(const Block & block) override
    {
        std::lock_guard lock(switch_mutex);
        IJoin::setTotals(block);
    }

    size_t getTotalRowCount() const override
    {
        std::shared_lock lock(switch_mutex);
        return join->getTotalRowCount();
    }

    size_t getTotalByteCount() const override
    {
        std::shared_lock lock(switch_mutex);
        return join->getTotalByteCount();
    }

    bool alwaysReturnsEmptySet() const override
    {
        std::shared_lock lock(switch_mutex);
        return IJoin::getTotals().empty() && join->alwaysReturnsEmptySet();
    }

    StepAnalysisReport getAnalysisReport() const override
    {
        std::shared_lock lock(switch_mutex);
        return join->getAnalysisReport();
    }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override
    {
        std::shared_lock lock(switch_mutex);
        return join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
    }

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block,
        const Block & result_sample_block,
        UInt64 max_block_size,
        size_t stream_idx,
        size_t num_streams) const override
    {
        std::shared_lock lock(switch_mutex);
        return join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size, stream_idx, num_streams);
    }

    IBlocksStreamPtr getDelayedBlocks() override
    {
        std::shared_lock lock(switch_mutex);
        return join->getDelayedBlocks();
    }

    bool hasDelayedBlocks() const override
    {
        std::shared_lock lock(switch_mutex);
        return join->hasDelayedBlocks();
    }

    /// May switch to PartialMergeJoin at runtime, which re-sorts left blocks by the join key.
    /// The read-in-order decision is made at plan time (before any switch), so we must be
    /// conservative and never claim to preserve the left stream order. See issue #110662.
    bool preservesLeftBlockOrder() const override { return false; }

    bool supportParallelJoin() const override { return use_parallel_layout && max_threads > 1; }
    size_t getMaxBuildThreads() const override { return max_threads; }
    bool supportParallelNonJoinedBlocksProcessing() const override { return supports_parallel_non_joined_blocks_processing; }

    void onBuildPhaseFinish() override
    {
        std::shared_lock lock(switch_mutex);
        join->onBuildPhaseFinish();
    }

    void onProbePhaseFinish(size_t matched_right_rows) override
    {
        std::shared_lock lock(switch_mutex);
        join->onProbePhaseFinish(matched_right_rows);
    }

    bool hasPostBuildPhase() const override
    {
        std::shared_lock lock(switch_mutex);
        return join->hasPostBuildPhase();
    }

    void runPostBuildPhase() override
    {
        std::shared_lock lock(switch_mutex);
        join->runPostBuildPhase();
    }

    void setEnableLazyColumnsIndexing(bool value) override
    {
        std::shared_lock lock(switch_mutex);
        join->setEnableLazyColumnsIndexing(value);
    }

private:
    JoinPtr join;
    SizeLimits limits;
    std::atomic<bool> switched{false};
    mutable SharedMutex switch_mutex;
    std::shared_ptr<TableJoin> table_join;
    const Block right_sample_block;
    const size_t max_threads;
    const bool use_parallel_layout;
    bool supports_parallel_non_joined_blocks_processing = false;

    /// Change join-in-memory to join-on-disk moving right hand JOIN data from one to another.
    /// Throws an error if join-on-disk do not support JOIN kind or strictness.
    /// Caller holds an exclusive `switch_mutex`.
    bool switchJoin();
};

}
