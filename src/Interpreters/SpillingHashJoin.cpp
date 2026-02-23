#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/SpillingHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/logger_useful.h>


namespace DB
{

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , limits(table_join->sizeLimits())
{
    hash_join = std::make_shared<HashJoin>(table_join, right_sample_block_);

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    size_t concurrent_slots_,
    const StatsCollectingParams & stats_collecting_params_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , limits(table_join->sizeLimits())
{
    concurrent_join = std::make_shared<ConcurrentHashJoin>(table_join, concurrent_slots_, right_sample_block_, stats_collecting_params_);

    if (!limits.hasLimits())
        limits.max_bytes = table_join->defaultMaxBytes();
}

SpillingHashJoin::~SpillingHashJoin() = default;

bool SpillingHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    /// Fast path: already switched to GraceHashJoin (no lock needed).
    if (state.load(std::memory_order_acquire) != SpillingState::COLLECTING)
        return inner_join->addBlockToJoin(block, check_limits);

    if (concurrent_join)
    {
        /// Shared lock: multiple threads add to ConcurrentHashJoin concurrently.
        std::shared_lock lock(switch_mutex);

        /// Re-check: another thread may have switched while we waited for the lock.
        if (state.load(std::memory_order_acquire) != SpillingState::COLLECTING)
            return inner_join->addBlockToJoin(block, check_limits);

        concurrent_join->addBlockToJoin(block, /*check_limits=*/false);
        lock.unlock();

        /// Check memory limit outside the shared lock.
        if (!limits.softCheck(concurrent_join->getTotalRowCount(), concurrent_join->getTotalByteCount()))
            switchToGraceHashJoin();

        return true;
    }

    /// Single-thread HashJoin path.
    hash_join->addBlockToJoin(block, /*check_limits=*/ false);

    if (!limits.softCheck(hash_join->getTotalRowCount(), hash_join->getTotalByteCount()))
    {
        LOG_DEBUG(log, "Memory limit exceeded ({} bytes, {} rows), switching to GraceHashJoin",
            hash_join->getTotalByteCount(), hash_join->getTotalRowCount());
        switchToGraceHashJoin();
    }

    return true;
}

void SpillingHashJoin::switchToGraceHashJoin()
{
    if (concurrent_join)
    {
        /// Exclusive lock: waits for all in-flight `addBlockToJoin` (shared lock holders)
        /// to complete. After this, no thread is inside `ConcurrentHashJoin::addBlockToJoin`.
        std::unique_lock lock(switch_mutex);

        /// Re-check: another thread may have already switched.
        if (state.load(std::memory_order_relaxed) != SpillingState::COLLECTING)
            return;

        LOG_DEBUG(
            log,
            "Memory limit exceeded ({} bytes, {} rows), "
            "switching ConcurrentHashJoin to GraceHashJoin",
            concurrent_join->getTotalByteCount(),
            concurrent_join->getTotalRowCount());

        /// Create GraceHashJoin.
        grace_join = std::make_shared<GraceHashJoin>(
            initial_num_buckets,
            max_num_buckets,
            table_join,
            left_sample_block,
            std::make_shared<const Block>(right_sample_block),
            tmp_data);
        grace_join->initialize(*left_sample_block);
        inner_join = grace_join;

        /// Set state BEFORE releasing the lock so new `addBlockToJoin` calls
        /// see GRACE_HASH_JOIN and go directly to `grace_join`.
        state.store(SpillingState::GRACE_HASH_JOIN, std::memory_order_release);
        lock.unlock();

        /// Now safe to extract slots (no concurrent `addBlockToJoin` on ConcurrentHashJoin).
        /// Extract one slot at a time for controlled memory release.
        const size_t total_slots = concurrent_join->getNumSlots();
        while (true)
        {
            size_t slot = next_slot_to_convert.fetch_add(1);
            if (slot >= total_slots)
                break;

            BlocksList blocks = concurrent_join->releaseSlotBlocks(slot);
            while (!blocks.empty())
            {
                grace_join->addBlockToJoin(blocks.front(), /*check_limits=*/false);
                blocks.pop_front();
            }
            slots_converted.fetch_add(1, std::memory_order_release);

            /// Check if memory is back under limit — stop converting, leave rest for joinBlock.
            size_t grace_bytes = grace_join->getTotalByteCount();
            size_t concurrent_bytes = concurrent_join->getTotalByteCount();
            if (limits.softCheck(0, grace_bytes + concurrent_bytes))
                break;
        }
        return;
    }

    /// Single-thread path: extract from HashJoin, feed to GraceHashJoin.
    BlocksList right_blocks = hash_join->releaseJoinedBlocks(/*restructure=*/ false);

    inner_join = std::make_shared<GraceHashJoin>(
        initial_num_buckets,
        max_num_buckets,
        table_join,
        left_sample_block,
        std::make_shared<const Block>(right_sample_block),
        tmp_data);

    inner_join->initialize(*left_sample_block);

    /// Drain extracted blocks into GraceHashJoin one by one,
    /// freeing each after insertion to limit peak memory.
    while (!right_blocks.empty())
    {
        inner_join->addBlockToJoin(right_blocks.front(), /*check_limits=*/ false);
        right_blocks.pop_front();
    }

    state.store(SpillingState::GRACE_HASH_JOIN, std::memory_order_release);
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    if (state.load(std::memory_order_acquire) == SpillingState::COLLECTING)
    {
        if (concurrent_join)
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting ConcurrentHashJoin",
                concurrent_join->getTotalByteCount(),
                concurrent_join->getTotalRowCount());
            inner_join = concurrent_join;
        }
        else
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting HashJoin",
                hash_join->getTotalByteCount(),
                hash_join->getTotalRowCount());
            inner_join = hash_join;
        }
        state.store(SpillingState::IN_MEMORY_JOIN, std::memory_order_release);
    }
    else if (
        state.load(std::memory_order_acquire) == SpillingState::GRACE_HASH_JOIN && concurrent_join
        && !conversion_complete.load(std::memory_order_acquire))
    {
        /// For GRACE_HASH_JOIN with unconverted concurrent slots:
        /// Do NOT call `inner_join->onBuildPhaseFinish` yet.
        /// Conversion and `onBuildPhaseFinish` are handled in `finishConcurrentConversion`.
        return;
    }

    inner_join->onBuildPhaseFinish();
}

void SpillingHashJoin::checkTypesOfKeys(const Block & block) const
{
    if (concurrent_join)
        concurrent_join->checkTypesOfKeys(block);
    else
        hash_join->checkTypesOfKeys(block);
}

void SpillingHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    if (!concurrent_join)
        hash_join->initialize(sample_block);
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block)
{
    /// During header computation (transformHeader), `joinBlock` is called with an empty block
    /// before any data is added. Delegate to the appropriate join in COLLECTING state.
    if (state.load(std::memory_order_acquire) == SpillingState::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->joinBlock(std::move(block));
        return hash_join->joinBlock(std::move(block));
    }

    /// If switched from concurrent to grace, ensure conversion is done before joining.
    if (state.load(std::memory_order_acquire) == SpillingState::GRACE_HASH_JOIN && concurrent_join
        && !conversion_complete.load(std::memory_order_acquire))
        finishConcurrentConversion();

    return inner_join->joinBlock(std::move(block));
}

void SpillingHashJoin::finishConcurrentConversion()
{
    chassert(concurrent_join);
    chassert(grace_join);

    const size_t total_slots = concurrent_join->getNumSlots();

    /// Each thread picks up slots, extracts blocks, pushes to shared queue.
    while (true)
    {
        size_t slot = next_slot_to_convert.fetch_add(1);
        if (slot >= total_slots)
            break;

        BlocksList blocks = concurrent_join->releaseSlotBlocks(slot);
        if (!blocks.empty())
        {
            std::lock_guard lock(block_queue_mutex);
            block_queue.splice(block_queue.end(), std::move(blocks));
        }

        /// Track that this slot has been extracted (blocks may still be in queue).
        slots_converted.fetch_add(1, std::memory_order_release);

        /// Also consume from queue (producer + consumer).
        consumeBlockQueue();
    }

    /// Phase 3: If all slots have been converted, finalize.
    /// Use `build_phase_called` to ensure exactly one thread calls `onBuildPhaseFinish`.
    if (slots_converted.load(std::memory_order_acquire) >= total_slots)
    {
        bool expected = false;
        if (build_phase_called.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
            consumeBlockQueue();
            grace_join->onBuildPhaseFinish();
            conversion_complete.store(true, std::memory_order_release);
        }
        else
        {
            /// Another thread is calling onBuildPhaseFinish. Wait for it.
            while (!conversion_complete.load(std::memory_order_acquire))
                consumeBlockQueue();
        }
    }
    else
    {
        /// Help drain queue while waiting for all slots to be converted.
        while (!conversion_complete.load(std::memory_order_acquire))
            consumeBlockQueue();
    }
}

void SpillingHashJoin::consumeBlockQueue()
{
    while (true)
    {
        Block block;
        {
            std::lock_guard lock(block_queue_mutex);
            if (block_queue.empty())
                return;
            block = std::move(block_queue.front());
            block_queue.pop_front();
        }
        grace_join->addBlockToJoin(block, /*check_limits=*/false);
    }
}

void SpillingHashJoin::setTotals(const Block & block)
{
    if (inner_join)
        inner_join->setTotals(block);
    else
    {
        std::lock_guard lock(totals_mutex);
        IJoin::setTotals(block);
    }
}

const Block & SpillingHashJoin::getTotals() const
{
    if (inner_join)
        return inner_join->getTotals();
    return IJoin::getTotals();
}

size_t SpillingHashJoin::getTotalRowCount() const
{
    if (state.load(std::memory_order_acquire) == SpillingState::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->getTotalRowCount();
        return hash_join->getTotalRowCount();
    }
    return inner_join->getTotalRowCount();
}

size_t SpillingHashJoin::getTotalByteCount() const
{
    if (state.load(std::memory_order_acquire) == SpillingState::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->getTotalByteCount();
        return hash_join->getTotalByteCount();
    }
    return inner_join->getTotalByteCount();
}

bool SpillingHashJoin::alwaysReturnsEmptySet() const
{
    if (state.load(std::memory_order_acquire) == SpillingState::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->alwaysReturnsEmptySet();
        return hash_join->alwaysReturnsEmptySet();
    }
    return inner_join->alwaysReturnsEmptySet();
}

IBlocksStreamPtr SpillingHashJoin::getNonJoinedBlocks(
    const Block & left_sample_block_,
    const Block & result_sample_block,
    UInt64 max_block_size) const
{
    chassert(inner_join);
    return inner_join->getNonJoinedBlocks(left_sample_block_, result_sample_block, max_block_size);
}

IBlocksStreamPtr SpillingHashJoin::getDelayedBlocks()
{
    chassert(inner_join);
    return inner_join->getDelayedBlocks();
}

}
