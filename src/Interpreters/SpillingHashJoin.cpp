#include <Interpreters/SpillingHashJoin.h>

#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>
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
    size_t max_num_buckets_,
    const StatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , any_take_last_row(any_take_last_row_)
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
{
    hash_join = std::make_shared<HashJoin>(
        table_join, right_sample_block_, any_take_last_row, /*reserve_num_=*/0, /*instance_id_=*/"",
        /*use_two_level_maps_=*/false, stats_collecting_params_);
}

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    size_t concurrent_slots_,
    const StatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , any_take_last_row(any_take_last_row_)
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
{
    concurrent_join = std::make_shared<ConcurrentHashJoin>(
        table_join,
        concurrent_slots_,
        right_sample_block_,
        stats_collecting_params_,
        any_take_last_row,
        max_bytes_before_external_join);
    supports_parallel_non_joined_blocks_processing = concurrent_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::SpillingHashJoin(
    PartitionedCollectingTag,
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    size_t num_threads_,
    const StatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , any_take_last_row(any_take_last_row_)
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
{
    partitioned_join = std::make_shared<PartitionedHashJoin>(
        table_join, right_sample_block_, num_threads_, any_take_last_row, stats_collecting_params_, max_bytes_before_external_join);
    supports_parallel_non_joined_blocks_processing = partitioned_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::~SpillingHashJoin() = default;

IJoin & SpillingHashJoin::collectingJoin() const
{
    if (concurrent_join)
        return *concurrent_join;
    if (partitioned_join)
        return *partitioned_join;
    return *hash_join;
}

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

void SpillingHashJoin::tryConvertFillLanes()
{
    chassert(partitioned_join);
    chassert(grace_join);

    const auto total_lanes = partitioned_join->getNumFillLanes();

    if (next_fill_lane_to_convert.load(std::memory_order_acquire) >= total_lanes)
        return;

    while (true)
    {
        size_t lane = next_fill_lane_to_convert.fetch_add(1);
        if (lane >= total_lanes)
            break;

        /// One block at a time: a whole-lane list would keep every remaining block of the lane
        /// alive while grace is also allocating buckets for the block being inserted.
        while (true)
        {
            Block block = partitioned_join->releaseNextFillLaneBlock(lane);
            if (block.empty())
                break;
            grace_join->addBlockToJoin(block, /*check_limits=*/false);
        }
    }
}

std::string SpillingHashJoin::getName() const
{
    static constexpr auto name_format = "SpillingHashJoin({})";
    if (concurrent_join)
        return fmt::format(name_format, concurrent_join->getName());
    if (partitioned_join)
        return fmt::format(name_format, partitioned_join->getName());
    return fmt::format(name_format, hash_join->getName());
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    return addCollectedBlock(block, check_limits, /*forward_lane=*/false, 0);
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, size_t /*num_rows*/, bool check_limits, size_t build_lane)
{
    if (!partitioned_join)
        return addBlockToJoin(block, check_limits);
    return addCollectedBlock(block, check_limits, /*forward_lane=*/true, build_lane);
}

bool SpillingHashJoin::addCollectedBlock(const Block & block, bool check_limits, bool forward_lane, size_t build_lane)
{
    /// Fast path: already switched to GraceHashJoin (no lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        /// Help convert one ConcurrentHashJoin slot / PartitionedHashJoin fill lane while in
        /// GRACE_HASH_JOIN state.
        if (concurrent_join)
            tryConvertSlots();
        else if (partitioned_join)
            tryConvertFillLanes();
        return chosen_join->addBlockToJoin(block, check_limits);
    }

    /// The hash table buffer grows in power-of-two steps. Doubling from X to 2X allocates the new
    /// buffer while the old one is still alive, transiently using 3X memory. We must trigger the
    /// switch BEFORE the inner `addBlockToJoin` runs (and possibly doubles the buffer); a check
    /// that runs after the call would race with the doubling and observe the OOM only as an
    /// allocator exception. Threshold is half of `max_bytes_before_external_join` so that after
    /// the switch the live buffer (already at half) plus the conversion peak still fit under the
    /// configured cap.
    ///
    /// PartitionedHashJoin builds no table during the fill: its leaf buffers are exact-reserved
    /// and created once after the barrier, so the doubling case does not arise. The tables the
    /// factor was standing in for are counted explicitly by `predictedResidentBytes`. Keeping both
    /// the factor and those bytes would double-count them and make the partitioned path spill
    /// earlier than `parallel_hash`. The single-thread and concurrent modes keep `* 2`.
    const bool over_threshold = partitioned_join
        ? partitioned_join->predictedResidentBytes() >= max_bytes_before_external_join
        : collectingJoin().getTotalByteCount() * 2 >= max_bytes_before_external_join;
    if (over_threshold)
        switchToGraceHashJoin();

    /// Re-check: we may have just switched.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        if (concurrent_join)
            tryConvertSlots();
        else if (partitioned_join)
            tryConvertFillLanes();
        return chosen_join->addBlockToJoin(block, check_limits);
    }

    if (concurrent_join || partitioned_join)
    {
        /// Shared lock: multiple threads add to ConcurrentHashJoin / PartitionedHashJoin concurrently.
        std::shared_lock lock(switch_mutex);

        /// Re-check: another thread may have switched while we waited for the lock.
        if (state.load(std::memory_order_acquire) != State::COLLECTING)
            return chosen_join->addBlockToJoin(block, check_limits);

        if (partitioned_join)
        {
            if (forward_lane)
                return partitioned_join->addBlockToJoin(block, block.rows(), check_limits, build_lane);
            return partitioned_join->addBlockToJoin(block, check_limits);
        }

        return concurrent_join->addBlockToJoin(block, check_limits);
    }

    /// Single-thread HashJoin path.
    return hash_join->addBlockToJoin(block, check_limits);
}

void SpillingHashJoin::createGraceJoin()
{
    grace_join = std::make_shared<GraceHashJoin>(
        initial_num_buckets,
        max_num_buckets,
        table_join,
        left_sample_block,
        std::make_shared<const Block>(right_sample_block),
        tmp_data,
        any_take_last_row,
        max_bytes_before_external_join);
    grace_join->initialize(*left_sample_block);
    chosen_join = grace_join;
}

void SpillingHashJoin::switchToGraceHashJoin()
{
    const auto print_threshold_reached_log = [this](const JoinPtr & join, std::string_view join_name)
    {
        LOG_DEBUG(
            log,
            "Memory spill threshold reached with {} ({} bytes, {} rows), switching to GraceHashJoin",
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

            print_threshold_reached_log(concurrent_join, "ConcurrentHashJoin");

            createGraceJoin();

            /// Set state BEFORE releasing the lock so new `addBlockToJoin` calls
            /// see GRACE_HASH_JOIN and go directly to `grace_join`.
            state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
        }
        /// Convert ConcurrentHashJoin slots into GraceHashJoin.
        /// Other build-phase threads will also help via `addBlockToJoin`.
        tryConvertSlots();
        return;
    }

    if (partitioned_join)
    {
        {
            std::unique_lock lock(switch_mutex);

            if (state.load(std::memory_order_relaxed) != State::COLLECTING)
                return;

            ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);

            print_threshold_reached_log(partitioned_join, "PartitionedHashJoin");

            createGraceJoin();
            /// Routes, prepared keys and skip masks are not used on the grace path. Drop them
            /// before any drain so they are not still allocated at the conversion peak.
            partitioned_join->dropFillAuxiliary();

            state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
        }
        tryConvertFillLanes();
        return;
    }

    print_threshold_reached_log(hash_join, "HashJoin");
    /// Single-thread path: extract from HashJoin, feed to GraceHashJoin.
    ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);
    BlocksList right_blocks = hash_join->releaseJoinedBlocks(/*restructure=*/false);

    chosen_join = std::make_shared<GraceHashJoin>(
        initial_num_buckets,
        max_num_buckets,
        table_join,
        left_sample_block,
        std::make_shared<const Block>(right_sample_block),
        tmp_data,
        any_take_last_row,
        max_bytes_before_external_join);

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
        /// Safety net for the terminal block: the proactive pre-insert check in `addBlockToJoin`
        /// fires only on subsequent calls. If the very last block pushed past the threshold without
        /// a follow-up insert to trigger the switch, promote it to `GraceHashJoin` here so the
        /// configured cap is honored.
        ///
        /// Partitioned mode uses the same predicted-bytes check as `addCollectedBlock`. The
        /// single-thread and concurrent modes keep the unfactored `getTotalByteCount` check they
        /// have always used at this terminal point (the `* 2` lives only on the per-block path,
        /// where a subsequent insert could still double the buffer).
        const bool over_threshold = partitioned_join
            ? partitioned_join->predictedResidentBytes() >= max_bytes_before_external_join
            : collectingJoin().getTotalByteCount() >= max_bytes_before_external_join;
        if (over_threshold)
        {
            switchToGraceHashJoin();
        }
        else if (partitioned_join)
        {
            /// The barrier concatenates lanes and sizes the plan. The gate histograms and exact-reserves the
            /// duplicate-list arenas, so the in-memory path does not pay that allocation later as a surprise;
            /// `MustSpill` releases them in `beginStoredBlockDrain`.
            partitioned_join->onBuildPhaseFinish();
            const auto plan = partitioned_join->planPostBuild();
            if (plan == PartitionedHashJoin::PostBuildPlan::MustSpill)
            {
                ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);
                LOG_DEBUG(
                    log,
                    "Post-build gate: resident data does not fit ({} bytes, {} rows), switching to GraceHashJoin",
                    partitioned_join->getTotalByteCount(),
                    partitioned_join->getTotalRowCount());

                createGraceJoin();

                /// The barrier already consumed every fill lane, so a late helper finds nothing to
                /// convert - but it must still find a `grace_join` to convert into.
                next_fill_lane_to_convert.store(partitioned_join->getNumFillLanes(), std::memory_order_release);

                partitioned_join->dropFillAuxiliary();
                partitioned_join->beginStoredBlockDrain();
                while (true)
                {
                    Block block = partitioned_join->releaseNextStoredBlock();
                    if (block.empty())
                        break;
                    chosen_join->addBlockToJoin(block, /*check_limits=*/false);
                }
                state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
            }
            else
            {
                LOG_DEBUG(
                    log,
                    "All blocks fit in memory ({} bytes, {} rows), promoting PartitionedHashJoin",
                    partitioned_join->getTotalByteCount(),
                    partitioned_join->getTotalRowCount());
                chosen_join = partitioned_join;
                state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
            }
        }
        else if (concurrent_join)
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting ConcurrentHashJoin",
                concurrent_join->getTotalByteCount(),
                concurrent_join->getTotalRowCount());
            chosen_join = concurrent_join;
            state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
        }
        else
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting HashJoin",
                hash_join->getTotalByteCount(),
                hash_join->getTotalRowCount());
            chosen_join = hash_join;
            state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
        }
    }

    /// The partitioned collecting join already ran its barrier above. Calling it again would be
    /// undefined after a post-barrier drain, and redundant after an in-memory promotion.
    if (partitioned_join && state.load(std::memory_order_acquire) == State::IN_MEMORY_JOIN)
        return;

    chosen_join->onBuildPhaseFinish();
}

bool SpillingHashJoin::hasPostBuildPhase() const
{
    /// `FillingRightJoinSideTransform` asks this right after `onBuildPhaseFinish`, so `chosen_join`
    /// is already set. Stay defensive anyway: with no chosen join there is nothing to post-process.
    return chosen_join && chosen_join->hasPostBuildPhase();
}

void SpillingHashJoin::runPostBuildPhase()
{
    if (chosen_join)
        chosen_join->runPostBuildPhase();
}

void SpillingHashJoin::setEnableLazyColumnsIndexing(bool value)
{
    if (hash_join)
        hash_join->setEnableLazyColumnsIndexing(value);
    if (concurrent_join)
        concurrent_join->setEnableLazyColumnsIndexing(value);
    if (partitioned_join)
        partitioned_join->setEnableLazyColumnsIndexing(value);
}

void SpillingHashJoin::checkTypesOfKeys(const Block & block) const
{
    collectingJoin().checkTypesOfKeys(block);
}

void SpillingHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    if (hash_join)
        hash_join->initialize(sample_block);
    if (partitioned_join)
        partitioned_join->initialize(sample_block);
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block)
{
    /// During header computation (transformHeader), `joinBlock` is called with an empty block
    /// before any data is added. Delegate to the appropriate join in COLLECTING state.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return collectingJoin().joinBlock(std::move(block));

    return chosen_join->joinBlock(std::move(block));
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block, size_t lane)
{
    if (!partitioned_join)
        return joinBlock(std::move(block));

    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return partitioned_join->joinBlock(std::move(block), lane);

    return chosen_join->joinBlock(std::move(block), lane);
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
        return collectingJoin().getTotalRowCount();
    return chosen_join->getTotalRowCount();
}

size_t SpillingHashJoin::getTotalByteCount() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return collectingJoin().getTotalByteCount();
    return chosen_join->getTotalByteCount();
}

bool SpillingHashJoin::alwaysReturnsEmptySet() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return collectingJoin().alwaysReturnsEmptySet();
    return chosen_join->alwaysReturnsEmptySet();
}

StepAnalysisReport SpillingHashJoin::getAnalysisReport() const
{
    /// This method always runs after the built phase, so in principal we could have
    /// written it without this if statement. However, we keep it
    /// for canonicity with the other accessors and safety in case the call order ever changes.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return collectingJoin().getAnalysisReport();
    return chosen_join->getAnalysisReport();
}

bool SpillingHashJoin::supportParallelJoin() const
{
    return concurrent_join != nullptr || (partitioned_join && partitioned_join->supportParallelJoin());
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
