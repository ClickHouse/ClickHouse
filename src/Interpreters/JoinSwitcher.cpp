#include <Common/typeid_cast.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/QueryExecutionCounters.h>

namespace DB
{

JoinSwitcher::JoinSwitcher(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    const bool any_take_last_row_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    bool use_partitioned_join_,
    size_t max_threads_,
    std::optional<size_t> build_rows_hint_)
    : limits(table_join_->sizeLimits())
    , switched(false)
    , use_partitioned_join(use_partitioned_join_)
    , table_join(table_join_)
    , right_sample_block(right_sample_block_->cloneEmpty())
{
    if (use_partitioned_join)
    {
        /// No memory budget: the limits of `table_join` decide the switch, and there is no join to spill into.
        join = std::make_shared<PartitionedHashJoin>(
            table_join,
            right_sample_block_,
            std::max<size_t>(1, max_threads_),
            any_take_last_row_,
            stats_collecting_params_,
            /*max_bytes_before_external_join_=*/0,
            build_rows_hint_);
    }
    else
    {
        join = HashJoin::create(
            table_join, right_sample_block_, HashJoinBuildLayout::oneBucket(1), any_take_last_row_, stats_collecting_params_);
        /// Until the build phase ends this join may have to hand its right blocks to `MergeJoin`.
        assert_cast<HashJoin *>(join.get())->keepRightBlocksForAnotherAlgorithm();
    }

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

bool JoinSwitcher::addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    std::lock_guard lock(switch_mutex);

    /// `MergeJoin` checks no limits of its own, so it does not matter whether `context` asks for them.
    if (switched)
        return join->addBlockToJoin(block, num_rows, context);

    /// HashJoin with external limits check

    join->addBlockToJoin(block, num_rows, context.callerChecksLimits());
    size_t rows = join->getTotalRowCount();
    size_t bytes = join->getTotalByteCount();

    if (!limits.softCheck(rows, bytes))
        return switchJoin(context);

    return true;
}

void JoinSwitcher::onBuildPhaseFinish()
{
    join->onBuildPhaseFinish();

    /// The switch to `MergeJoin` only happens while blocks are being added, and that is over: if it
    /// did not happen, nothing will take the right blocks now, so a join that stores only the keys
    /// can drop them.
    std::lock_guard lock(switch_mutex);
    if (!switched && !use_partitioned_join)
        assert_cast<HashJoin *>(join.get())->dropRightBlocksKeptForAnotherAlgorithm();
}

bool JoinSwitcher::switchJoin(JoinBuildContext context)
{
    BlocksList right_blocks = use_partitioned_join ? assert_cast<PartitionedHashJoin *>(join.get())->releaseJoinedBlocks(true)
                                                   : assert_cast<HashJoin *>(join.get())->releaseJoinedBlocks(true);

    /// Destroy old join & create new one.
    join = std::make_shared<MergeJoin>(table_join, std::make_shared<const Block>(right_sample_block));

    bool success = true;
    for (const Block & saved_block : right_blocks)
        success = success && join->addBlockToJoin(saved_block, saved_block.rows(), context);

    switched = true;

    QueryExecutionCounters::addUsedJoinAlgorithm(JoinAlgorithm::PARTIAL_MERGE);

    return success;
}

}
