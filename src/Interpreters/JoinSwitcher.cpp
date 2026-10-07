#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FAULT_INJECTED;
}

namespace FailPoints
{
extern const char join_switcher_throw_after_hash_release[];
}

namespace
{

/// `MergeJoin::joinBlock` is not concurrent. `supportParallelJoin` is decided at plan time.
/// After a drain the pipeline may still probe from several `JoiningTransform`s.
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
    std::optional<size_t> build_rows_hint_)
    : limits(table_join_->sizeLimits())
    , table_join(table_join_)
    , right_sample_block(right_sample_block_->cloneEmpty())
    , max_threads(std::max<size_t>(1, max_threads_))
{
    /// No memory budget: the limits of `table_join` decide the switch, and there is no join to spill into.
    join = std::make_shared<HashJoin>(
        table_join,
        right_sample_block_,
        max_threads,
        any_take_last_row_,
        stats_collecting_params_,
        /*max_bytes_before_external_join_=*/0,
        build_rows_hint_);
    supports_parallel_join = join->supportParallelJoin();
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
        const size_t rows = assert_cast<const HashJoin &>(*join).rowCountForLimit(limits.max_rows);
        over_limit = !limits.softCheck(rows, join->getTotalByteCount());
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

void JoinSwitcher::onBuildPhaseFinish()
{
    std::shared_lock lock(switch_mutex);
    join->onBuildPhaseFinish();
}

bool JoinSwitcher::switchJoin()
{
    LOG_DEBUG(
        getLogger("JoinSwitcher"),
        "Memory limit reached with {} ({} bytes, {} rows), switching to PartialMergeJoin",
        join->getName(),
        join->getTotalByteCount(),
        join->getTotalRowCount());

    /// Construct first so a throw here leaves `join` as the live in-memory join with `switched == false`.
    auto merge_join = std::make_shared<MergeJoin>(table_join, std::make_shared<const Block>(right_sample_block));

    /// Keep the old table alive for the drain. Publish `MergeJoin` before releasing so a throw
    /// cannot send waiters back onto a drained HashJoin.
    auto old_join = std::move(join);
    switched.store(true, std::memory_order_release);
    join = merge_join;

    BlocksList right_blocks = assert_cast<HashJoin *>(old_join.get())->releaseJoinedBlocks(/*restructure=*/true);

    fiu_do_on(FailPoints::join_switcher_throw_after_hash_release, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after the in-memory join's data was released");
    });

    bool success = true;
    for (const Block & saved_block : right_blocks)
        success = success && merge_join->addBlockToJoin(saved_block, saved_block.rows(), /* worker_id = */ 0, true);

    return success;
}

}
