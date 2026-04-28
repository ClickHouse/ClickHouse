#include <Interpreters/SpillingHashJoin.h>

#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event JoinSpillingHashJoinSwitchedToGraceJoin;
}

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
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
{
    hash_join = std::make_shared<HashJoin>(table_join, right_sample_block_);
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
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
{
    concurrent_join = std::make_shared<ConcurrentHashJoin>(table_join, concurrent_slots_, right_sample_block_, stats_collecting_params_);
    supports_parallel_non_joined_blocks_processing = concurrent_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::~SpillingHashJoin() = default;

void SpillingHashJoin::tryConvertSlots()
{
    chassert(concurrent_join);
    chassert(grace_join);

    const auto total_slots = concurrent_join->getNumSlots();

    /// Fast path: all slots already converted.
    if (next_slot_to_convert.load(std::memory_order_acquire) >= total_slots)
        return;

    while (true)
    {
        size_t slot = next_slot_to_convert.fetch_add(1);
        if (slot >= total_slots)
            break;

        auto blocks = concurrent_join->releaseSlotBlocks(slot);
        while (!blocks.empty())
        {
            grace_join->addBlockToJoin(blocks.front(), /*check_limits=*/false);
            blocks.pop_front();
        }
    }
}

std::string SpillingHashJoin::getName() const
{
    static constexpr auto name_format = "SpillingHashJoin({})";

    if (concurrent_join)
        return fmt::format(name_format, concurrent_join->getName());

    return fmt::format(name_format, hash_join->getName());
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    /// Fast path: already switched to GraceHashJoin (no lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        /// Help convert one ConcurrentHashJoin slot while in GRACE_HASH_JOIN state.
        if (concurrent_join)
            tryConvertSlots();
        return chosen_join->addBlockToJoin(block, check_limits);
    }

    if (concurrent_join)
    {
        {
            /// Shared lock: multiple threads add to ConcurrentHashJoin concurrently.
            std::shared_lock lock(switch_mutex);

            /// Re-check: another thread may have switched while we waited for the lock.
            if (state.load(std::memory_order_acquire) != State::COLLECTING)
                return chosen_join->addBlockToJoin(block, check_limits);

            if (!concurrent_join->addBlockToJoin(block, check_limits))
                return false;
        }

        /// Check memory limit outside the shared lock.
        if (concurrent_join->getTotalByteCount() >= max_bytes_before_external_join)
            switchToGraceHashJoin();

        return true;
    }

    /// Single-thread HashJoin path.
    if (!hash_join->addBlockToJoin(block, check_limits))
        return false;

    if (hash_join->getTotalByteCount() >= max_bytes_before_external_join)
        switchToGraceHashJoin();

    return true;
}

void SpillingHashJoin::switchToGraceHashJoin()
{
    const auto print_limit_exceeded_log = [this](const JoinPtr & join, std::string_view join_name)
    {
        LOG_DEBUG(
            log,
            "Memory limit exceeded with {} ({} bytes, {} rows), switching to GraceHashJoin",
            join_name,
            join->getTotalByteCount(),
            join->getTotalRowCount());
    };
    if (concurrent_join)
    {
        {
            /// Exclusive lock: waits for all in-flight `addBlockToJoin` (shared lock holders)
            /// to complete. After this, no thread is inside `ConcurrentHashJoin::addBlockToJoin`.
            std::unique_lock lock(switch_mutex);

            /// Re-check: another thread may have already switched.
            if (state.load(std::memory_order_relaxed) != State::COLLECTING)
                return;

            ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);

            print_limit_exceeded_log(concurrent_join, "ConcurrentHashJoin");

            /// Create GraceHashJoin.
            grace_join = std::make_shared<GraceHashJoin>(
                initial_num_buckets,
                max_num_buckets,
                table_join,
                left_sample_block,
                std::make_shared<const Block>(right_sample_block),
                tmp_data);
            grace_join->initialize(*left_sample_block);
            chosen_join = grace_join;

            /// Set state BEFORE releasing the lock so new `addBlockToJoin` calls
            /// see GRACE_HASH_JOIN and go directly to `grace_join`.
            state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
        }
        /// Convert ConcurrentHashJoin slots into GraceHashJoin.
        /// Other build-phase threads will also help via `addBlockToJoin`.
        tryConvertSlots();
        return;
    }

    print_limit_exceeded_log(hash_join, "HashJoin");
    /// Single-thread path: extract from HashJoin, feed to GraceHashJoin.
    ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);
    BlocksList right_blocks = hash_join->releaseJoinedBlocks(/*restructure=*/false);

    chosen_join = std::make_shared<GraceHashJoin>(
        initial_num_buckets, max_num_buckets, table_join, left_sample_block, std::make_shared<const Block>(right_sample_block), tmp_data);

    chosen_join->initialize(*left_sample_block);

    /// Drain extracted blocks into GraceHashJoin one by one,
    /// freeing each after insertion to limit peak memory.
    while (!right_blocks.empty())
    {
        chosen_join->addBlockToJoin(right_blocks.front(), /*check_limits=*/false);
        right_blocks.pop_front();
    }

    state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        if (concurrent_join)
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting ConcurrentHashJoin",
                concurrent_join->getTotalByteCount(),
                concurrent_join->getTotalRowCount());
            chosen_join = concurrent_join;
        }
        else
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting HashJoin",
                hash_join->getTotalByteCount(),
                hash_join->getTotalRowCount());
            chosen_join = hash_join;
        }
        state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
    }

    chosen_join->onBuildPhaseFinish();
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
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->joinBlock(std::move(block));
        return hash_join->joinBlock(std::move(block));
    }

    return chosen_join->joinBlock(std::move(block));
}

void SpillingHashJoin::setTotals(const Block & block)
{
    std::lock_guard lock(totals_mutex);
    IJoin::setTotals(block);
}

const Block & SpillingHashJoin::getTotals() const
{
    std::lock_guard lock(totals_mutex);
    return IJoin::getTotals();
}

size_t SpillingHashJoin::getTotalRowCount() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->getTotalRowCount();
        return hash_join->getTotalRowCount();
    }
    return chosen_join->getTotalRowCount();
}

size_t SpillingHashJoin::getTotalByteCount() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->getTotalByteCount();
        return hash_join->getTotalByteCount();
    }
    return chosen_join->getTotalByteCount();
}

bool SpillingHashJoin::alwaysReturnsEmptySet() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->alwaysReturnsEmptySet();
        return hash_join->alwaysReturnsEmptySet();
    }
    return chosen_join->alwaysReturnsEmptySet();
}

bool SpillingHashJoin::supportParallelNonJoinedBlocksProcessing() const
{
    return supports_parallel_non_joined_blocks_processing;
}

bool SpillingHashJoin::isParallelNonJoinedProcessingEnabled() const
{
    return state == State::IN_MEMORY_JOIN && supports_parallel_non_joined_blocks_processing
        && chosen_join->supportParallelNonJoinedBlocksProcessing();
}

IBlocksStreamPtr
SpillingHashJoin::getNonJoinedBlocks(const Block & left_sample_block_, const Block & result_sample_block, UInt64 max_block_size) const
{
    chassert(chosen_join);
    return chosen_join->getNonJoinedBlocks(left_sample_block_, result_sample_block, max_block_size);
}

IBlocksStreamPtr SpillingHashJoin::getNonJoinedBlocks(
    const Block & left_sample_block_, const Block & result_sample_block, UInt64 max_block_size, size_t stream_idx, size_t num_streams) const
{
    chassert(chosen_join);
    return chosen_join->getNonJoinedBlocks(left_sample_block_, result_sample_block, max_block_size, stream_idx, num_streams);
}

IBlocksStreamPtr SpillingHashJoin::getDelayedBlocks()
{
    chassert(chosen_join);
    return chosen_join->getDelayedBlocks();
}

}
