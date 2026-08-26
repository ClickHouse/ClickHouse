#pragma once

#include <atomic>

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
/// because `MergeJoin::addBlockToJoin` is not concurrent.
class JoinSwitcher : public IJoin
{
public:
    JoinSwitcher(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block_,
        bool any_take_last_row_,
        const StatsCollectingParams & stats_collecting_params_ = {},
        size_t max_threads_ = 1,
        bool use_parallel_layout_ = false);

    std::string getName() const override { return "JoinSwitcher"; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool anyTakeLastRow() const override { return join->anyTakeLastRow(); }

    /// Add block of data from right hand of JOIN into current join object.
    /// If join-in-memory memory limit exceeded switches to join-on-disk and continue with it.
    /// @returns false, if join-on-disk disk limit exceeded
    bool addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override { join->checkTypesOfKeys(block); }

    JoinResultPtr joinBlock(Block block) override { return join->joinBlock(block); }

    const Block & getTotals() const override { return join->getTotals(); }

    void setTotals(const Block & block) override { join->setTotals(block); }

    size_t getTotalRowCount() const override { return join->getTotalRowCount(); }

    size_t getTotalByteCount() const override { return join->getTotalByteCount(); }

    bool alwaysReturnsEmptySet() const override { return join->alwaysReturnsEmptySet(); }

    StepAnalysisReport getAnalysisReport() const override { return join->getAnalysisReport(); }

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override
    {
        return join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
    }

    IBlocksStreamPtr getDelayedBlocks() override { return join->getDelayedBlocks(); }

    bool hasDelayedBlocks() const override { return join->hasDelayedBlocks(); }

    /// May switch to PartialMergeJoin at runtime, which re-sorts left blocks by the join key.
    /// The read-in-order decision is made at plan time (before any switch), so we must be
    /// conservative and never claim to preserve the left stream order. See issue #110662.
    bool preservesLeftBlockOrder() const override { return false; }

    bool supportParallelJoin() const override { return use_parallel_layout && max_threads > 1; }
    size_t getMaxBuildThreads() const override { return max_threads; }

    void onBuildPhaseFinish() override { join->onBuildPhaseFinish(); }

    bool hasPostBuildPhase() const override { return join->hasPostBuildPhase(); }

    void runPostBuildPhase() override { join->runPostBuildPhase(); }

    void setEnableLazyColumnsIndexing(bool value) override { join->setEnableLazyColumnsIndexing(value); }

private:
    JoinPtr join;
    SizeLimits limits;
    std::atomic<bool> switched{false};
    mutable SharedMutex switch_mutex;
    std::shared_ptr<TableJoin> table_join;
    const Block right_sample_block;
    const size_t max_threads;
    const bool use_parallel_layout;

    /// Change join-in-memory to join-on-disk moving right hand JOIN data from one to another.
    /// Throws an error if join-on-disk do not support JOIN kind or strictness.
    /// Caller holds an exclusive `switch_mutex`.
    bool switchJoin();
};

}
