#include <Interpreters/SpillingHashJoin.h>

#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/QueryExecutionCounters.h>
#include <Interpreters/TableJoin.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>

namespace ProfileEvents
{
extern const Event JoinSpillingHashJoinSwitchedToGraceJoin;
extern const Event JoinSpillingHashJoinSwitchedToMergeJoin;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
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
        /*is_concurrent_hash_join_=*/false, stats_collecting_params_);
    /// Until the build phase ends this join may have to hand its right blocks to `GraceHashJoin`.
    hash_join->keepRightBlocksForAnotherAlgorithm();
}

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    size_t concurrent_slots_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
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
    /// As above: the slots' blocks go to `GraceHashJoin` if the switch happens.
    concurrent_join->keepRightBlocksForAnotherAlgorithm();
    supports_parallel_non_joined_blocks_processing = concurrent_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::~SpillingHashJoin() = default;

bool SpillingHashJoin::addBlockToExternalJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    if (keep_left_block_order)
    {
        /// MergeJoin cannot build right side concurrently, thus protect it with a mutex
        /// TODO(antaljanosbenjamin): Make SortedBlockWriter multi-threaded
        chassert(!chosen_join->supportParallelJoin());
        std::lock_guard lock(merge_join_insert_mutex);
        return chosen_join->addBlockToJoin(block, num_rows, context);
    }

    return chosen_join->addBlockToJoin(block, num_rows, context);
}

void SpillingHashJoin::tryConvertSlots(JoinBuildContext context)
{
    chassert(concurrent_join);
    chassert(grace_join || merge_join);

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
            addBlockToExternalJoin(blocks.front(), blocks.front().rows(), context.callerChecksLimits());
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

std::string SpillingHashJoin::getAlgorithm() const
{
    switch (state.load(std::memory_order_acquire))
    {
        case State::GRACE_HASH_JOIN:
            return toString(JoinAlgorithm::GRACE_HASH);
        case State::MERGE_JOIN:
            return toString(JoinAlgorithm::PARTIAL_MERGE);
        case State::COLLECTING:
        case State::IN_MEMORY_JOIN:
            return toString(concurrent_join ? JoinAlgorithm::PARALLEL_HASH : JoinAlgorithm::HASH);
    }
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    /// Fast path: already switched to the external join (no switch lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        /// Help convert ConcurrentHashJoin slots into the external join.
        if (concurrent_join)
            tryConvertSlots(context);
        return addBlockToExternalJoin(block, num_rows, context);
    }

    /// The hash table buffer grows in power-of-two steps. Doubling from X to 2X allocates the new
    /// buffer while the old one is still alive, transiently using 3X memory. We must trigger the
    /// switch BEFORE the inner `addBlockToJoin` runs (and possibly doubles the buffer); a check
    /// that runs after the call would race with the doubling and observe the OOM only as an
    /// allocator exception. Threshold is half of `max_bytes_before_external_join` so that after
    /// the switch the live buffer (already at half) plus the conversion peak still fit under the
    /// configured cap.
    if (concurrent_join)
    {
        if (concurrent_join->getTotalByteCount() * 2 >= max_bytes_before_external_join)
            switchToExternalJoin(context);
    }
    else
    {
        if (hash_join->getTotalByteCount() * 2 >= max_bytes_before_external_join)
            switchToExternalJoin(context);
    }

    /// Re-check: we may have just switched.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        if (concurrent_join)
            tryConvertSlots(context);
        return addBlockToExternalJoin(block, num_rows, context);
    }

    if (concurrent_join)
    {
        /// Shared lock: multiple threads add to ConcurrentHashJoin concurrently.
        std::shared_lock lock(switch_mutex);

        /// Re-check: another thread may have switched while we waited for the lock.
        if (state.load(std::memory_order_acquire) != State::COLLECTING)
            return addBlockToExternalJoin(block, num_rows, context);

        return concurrent_join->addBlockToJoin(block, num_rows, context);
    }

    /// Single-thread HashJoin path.
    return hash_join->addBlockToJoin(block, num_rows, context);
}

ProcessorMemoryStats SpillingHashJoin::getMemoryStats() const
{
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        /// Nothing to report for the MergeJoin fallback either: it writes the right side through
        /// its disk writer as it arrives, so a pinned join holds no spillable memory.
        if (!grace_join)
            return {};

        auto res = grace_join->getMemoryStats();
        if (concurrent_join)
        {
            /// Unconverted slots still own memory; their counters are cleared by `releaseSlotBlocks`.
            const auto unconverted_bytes = concurrent_join->getTotalByteCount();
            res.spillable_memory_bytes += unconverted_bytes;
            res.need_reserved_memory_bytes += unconverted_bytes;
        }
        return res;
    }

    ProcessorMemoryStats res;
    res.spillable_memory_bytes = concurrent_join ? concurrent_join->getTotalByteCount() : hash_join->getTotalByteCount();
    /// The released blocks stay alive while the grace join re-inserts them.
    res.need_reserved_memory_bytes = res.spillable_memory_bytes;
    return res;
}

size_t SpillingHashJoin::spill(size_t at_least_bytes)
{
    if (!at_least_bytes)
        return 0;

    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        size_t before = getTotalByteCount();
        switchToExternalJoin(JoinBuildContext::serial(), /*spill_immediately=*/true);
        size_t after = getTotalByteCount();
        /// Check the new grace bucket's spillable estimate before requesting a repartition.
        return before > after ? before - after : 0;
    }

    /// The MergeJoin fallback spills by construction, so a pinned join has nothing more to
    /// release here (`grace_join` stays null).
    return grace_join ? grace_join->spill(at_least_bytes) : 0;
}

void SpillingHashJoin::switchToExternalJoin(JoinBuildContext context, bool spill_immediately)
{
    /// The target is pinned at plan time: MergeJoin when the plan requires the left stream's row
    /// order (the pipeline was then built without the delayed-block transforms GraceHashJoin
    /// needs), GraceHashJoin otherwise. Everything but the target join creation is the same.
    const State target_state = keep_left_block_order ? State::MERGE_JOIN : State::GRACE_HASH_JOIN;
    const JoinAlgorithm target_algorithm = keep_left_block_order ? JoinAlgorithm::PARTIAL_MERGE : JoinAlgorithm::GRACE_HASH;
    const ProfileEvents::Event switch_event = keep_left_block_order
        ? ProfileEvents::JoinSpillingHashJoinSwitchedToMergeJoin
        : ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin;

    /// A scheduler-requested switch must partition the data, not just rebuild it in one bucket.
    /// Meaningful only for GraceHashJoin: the MergeJoin fallback always spills immediately by
    /// construction - `spillRightBlocksToDisk` switches to the disk writer before the first
    /// drained block.
    const size_t num_buckets = spill_immediately ? std::max<size_t>(initial_num_buckets, 2) : initial_num_buckets;

    const auto make_external_join = [&]() -> JoinPtr
    {
        if (keep_left_block_order)
        {
            merge_join = std::make_shared<MergeJoin>(
                table_join,
                std::make_shared<const Block>(right_sample_block),
                SizeLimits(/*max_rows_=*/0, max_bytes_before_external_join, OverflowMode::THROW));
            merge_join->spillRightBlocksToDisk();
            merge_join->keepLeftPipelineInOrder();
            return merge_join;
        }

        grace_join = std::make_shared<GraceHashJoin>(
            num_buckets,
            max_num_buckets,
            table_join,
            left_sample_block,
            std::make_shared<const Block>(right_sample_block),
            tmp_data,
            any_take_last_row,
            max_bytes_before_external_join);
        grace_join->initialize(*left_sample_block);
        return grace_join;
    };

    const auto log_switch = [&](const JoinPtr & join, std::string_view join_name)
    {
        LOG_DEBUG(
            log,
            "{}, switching to {}: {} holds {} bytes in {} rows",
            spill_immediately ? "Spill requested under memory pressure" : "Memory spill threshold reached",
            keep_left_block_order ? "MergeJoin (the plan requires the left block order)" : "GraceHashJoin",
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

            ProfileEvents::increment(switch_event);
            log_switch(concurrent_join, "ConcurrentHashJoin");

            chosen_join = make_external_join();

            /// `spill` has no build-stream identity. Replay under the exclusive lock before publishing
            /// the new join, so its `serial` context cannot overlap another insert into that join.
            if (spill_immediately)
                tryConvertSlots(context);

            /// Set state BEFORE releasing the lock so new `addBlockToJoin` calls
            /// see the new state and go directly to `chosen_join`.
            state.store(target_state, std::memory_order_release);
        }
        /// Convert ConcurrentHashJoin slots into the external join.
        /// Other build-phase threads will also help via `addBlockToJoin`.
        if (!spill_immediately)
            tryConvertSlots(context);

        QueryExecutionCounters::addUsedJoinAlgorithm(target_algorithm);
        return;
    }

    /// Re-check: another thread may have already switched.
    if (state.load(std::memory_order_relaxed) != State::COLLECTING)
        return;

    log_switch(hash_join, "HashJoin");
    ProfileEvents::increment(switch_event);

    /// Single-thread path: extract from HashJoin, feed to the external join.
    BlocksList right_blocks = hash_join->releaseJoinedBlocks(/*restructure=*/false);

    chosen_join = make_external_join();

    /// Drain extracted blocks into the external join one by one,
    /// freeing each after insertion to limit peak memory.
    while (!right_blocks.empty())
    {
        addBlockToExternalJoin(right_blocks.front(), right_blocks.front().rows(), context.callerChecksLimits());
        right_blocks.pop_front();
    }

    QueryExecutionCounters::addUsedJoinAlgorithm(target_algorithm);

    state.store(target_state, std::memory_order_release);
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        /// Safety net for the terminal block: the proactive pre-insert check in `addBlockToJoin`
        /// fires only on subsequent calls. If the very last block pushed total bytes past
        /// `max_bytes_before_external_join` without a follow-up insert to trigger the switch,
        /// promote it to `GraceHashJoin` here so the configured cap is honored.
        const size_t total_bytes = concurrent_join ? concurrent_join->getTotalByteCount() : hash_join->getTotalByteCount();
        if (total_bytes >= max_bytes_before_external_join)
        {
            switchToExternalJoin(JoinBuildContext::serial());
        }
        else if (concurrent_join)
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting ConcurrentHashJoin",
                total_bytes,
                concurrent_join->getTotalRowCount());
            chosen_join = concurrent_join;
            state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
        }
        else
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting HashJoin",
                total_bytes,
                hash_join->getTotalRowCount());
            chosen_join = hash_join;
            state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
        }
    }

    chosen_join->onBuildPhaseFinish();

    /// The switch to `GraceHashJoin` can only happen while collecting, and that is over: whatever
    /// right blocks were kept in case it did are now dead weight for the whole probe phase, so a
    /// join that stores only the keys can drop them.
    if (state.load(std::memory_order_acquire) == State::IN_MEMORY_JOIN)
    {
        if (concurrent_join)
            concurrent_join->dropRightBlocksKeptForAnotherAlgorithm();
        else
            hash_join->dropRightBlocksKeptForAnotherAlgorithm();
    }
}

bool SpillingHashJoin::canPreserveLeftBlockOrder() const
{
    /// The in-memory join processes the probe side when the build fits under the threshold, so it
    /// must preserve the order on its own.
    bool in_memory_join_preserves_order
        = concurrent_join ? concurrent_join->preservesLeftBlockOrder() : hash_join->preservesLeftBlockOrder();

    /// The spill fallback must be able to run as an order-preserving MergeJoin. `any_take_last_row`
    /// has no MergeJoin counterpart: it picks the first match in the right sort order instead.
    bool merge_join_fallback_is_possible = MergeJoin::isSupported(table_join)
        && (table_join->strictness() == JoinStrictness::Any || table_join->strictness() == JoinStrictness::All)
        && isInnerOrLeft(table_join->kind()) && !(any_take_last_row && table_join->strictness() == JoinStrictness::Any);

    return in_memory_join_preserves_order && merge_join_fallback_is_possible;
}

void SpillingHashJoin::keepLeftPipelineInOrder()
{
    /// The callers must check `canPreserveLeftBlockOrder` first (see IJoin::keepLeftPipelineInOrder).
    /// In particular, RIGHT and FULL joins can never keep the left order: the non-joined right
    /// rows are emitted out of band and have no position in the left order.
    if (!canPreserveLeftBlockOrder())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "SpillingHashJoin cannot preserve the left block order for this join");

    /// The pin must land before the build phase starts: the choice of the spill fallback and the
    /// delayed-block transforms in the pipeline depend on it.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SpillingHashJoin got the left-order pin after the build phase started");

    keep_left_block_order = true;
}

void SpillingHashJoin::onProbePhaseFinish(std::optional<size_t> matched_right_rows)
{
    chosen_join->onProbePhaseFinish(matched_right_rows);
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
    if (concurrent_join)
        concurrent_join->initialize(sample_block);
    else
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

StepAnalysisReport SpillingHashJoin::getAnalysisReport() const
{
    /// This method always runs after the built phase, so in principal we could have
    /// written it without this if statement. However, we keep it
    /// for canonicity with the other accessors and safety in case the call order ever changes.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        if (concurrent_join)
            return concurrent_join->getAnalysisReport();
        return hash_join->getAnalysisReport();
    }
    return chosen_join->getAnalysisReport();
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

bool SpillingHashJoin::isDelayedBlocksProcessingEnabled() const
{
    /// `hasDelayedBlocks` is false under the left-order pin: the pipeline is then built without
    /// the delayed-block transforms, and the MergeJoin fallback has none to process anyway (it
    /// inherits the nullptr `IJoin::getDelayedBlocks`). The conjunction is defensive - no
    /// `JoiningTransform` with a delayed input can observe a pinned join.
    return hasDelayedBlocks() && state.load(std::memory_order_acquire) != State::IN_MEMORY_JOIN;
}

}
