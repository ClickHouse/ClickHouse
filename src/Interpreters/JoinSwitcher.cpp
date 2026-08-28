#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/MergeJoin.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{

/// `MergeJoin::joinBlock` is not concurrent. `supportParallelJoin` is decided at plan time,
/// so after a drain the pipeline may still probe from several `JoiningTransform`s.
class ExclusiveJoinResult : public IJoinResult
{
public:
    ExclusiveJoinResult(std::unique_lock<SharedMutex> lock_, JoinResultPtr inner_)
        : lock(std::move(lock_))
        , inner(std::move(inner_))
    {
    }

    JoinResultBlock next() override { return inner->next(); }

private:
    std::unique_lock<SharedMutex> lock;
    JoinResultPtr inner;
};

}

JoinSwitcher::JoinSwitcher(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    const bool any_take_last_row_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    size_t max_threads_,
    bool use_parallel_layout_)
    : limits(table_join_->sizeLimits())
    , table_join(table_join_)
    , right_sample_block(right_sample_block_->cloneEmpty())
    , max_threads(std::max<size_t>(1, max_threads_))
    , use_parallel_layout(use_parallel_layout_)
{
    join = std::make_shared<HashJoin>(
        table_join,
        right_sample_block_,
        any_take_last_row_,
        /*reserve_num_=*/0,
        /*instance_id_=*/"",
        stats_collecting_params_,
        max_threads,
        use_parallel_layout);
    supports_parallel_non_joined_blocks_processing = join->supportParallelNonJoinedBlocksProcessing();

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

bool JoinSwitcher::addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool)
{
    if (switched.load(std::memory_order_acquire))
    {
        std::unique_lock lock(switch_mutex);
        return join->addBlockToJoin(block, num_rows, worker_id, true);
    }

    bool over_limit = false;
    {
        std::shared_lock lock(switch_mutex);

        if (switched.load(std::memory_order_relaxed))
        {
            lock.unlock();
            std::unique_lock exclusive(switch_mutex);
            return join->addBlockToJoin(block, num_rows, worker_id, true);
        }

        join->addBlockToJoin(block, num_rows, worker_id, false);
        over_limit = !limits.softCheck(join->getTotalRowCount(), join->getTotalByteCount());
    }

    if (!over_limit)
        return true;

    std::unique_lock lock(switch_mutex);
    if (switched.load(std::memory_order_relaxed))
        return true;
    return switchJoin();
}

JoinResultPtr JoinSwitcher::joinBlock(Block block)
{
    if (!switched.load(std::memory_order_acquire))
    {
        std::shared_lock lock(switch_mutex);
        if (!switched.load(std::memory_order_relaxed))
            return join->joinBlock(std::move(block));
    }

    std::unique_lock lock(switch_mutex);
    return std::make_unique<ExclusiveJoinResult>(std::move(lock), join->joinBlock(std::move(block)));
}

bool JoinSwitcher::switchJoin()
{
    HashJoin * hash_join = assert_cast<HashJoin *>(join.get());
    LOG_DEBUG(
        getLogger("JoinSwitcher"),
        "Memory limit reached with HashJoin ({} bytes, {} rows), switching to PartialMergeJoin",
        hash_join->getTotalByteCount(),
        hash_join->getTotalRowCount());
    BlocksList right_blocks = hash_join->releaseJoinedBlocks(true);

    /// Destroy old join & create new one.
    join = std::make_shared<MergeJoin>(table_join, std::make_shared<const Block>(right_sample_block));

    bool success = true;
    for (const Block & saved_block : right_blocks)
        success = success && join->addBlockToJoin(saved_block, saved_block.rows(), /* worker_id = */ 0, true);

    switched.store(true, std::memory_order_release);
    return success;
}

}
