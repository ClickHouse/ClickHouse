#include <Interpreters/SpillingHashJoin.h>
#include <algorithm>

#include <utility>

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

HashJoin & SpillingHashJoin::collectingJoin()
{
    chassert(in_memory_hash_join);
    return *in_memory_hash_join;
}

const HashJoin & SpillingHashJoin::collectingJoin() const
{
    chassert(in_memory_hash_join);
    return *in_memory_hash_join;
}

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_,
    size_t max_threads_,
    bool use_parallel_layout_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , any_take_last_row(any_take_last_row_)
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
    , max_threads(std::max<size_t>(1, max_threads_))
{
    in_memory_hash_join = std::make_shared<HashJoin>(
        table_join,
        right_sample_block_,
        any_take_last_row,
        /*reserve_num_=*/0,
        /*instance_id_=*/"",
        stats_collecting_params_,
        max_threads,
        use_parallel_layout_);
    supports_parallel_non_joined_blocks_processing = in_memory_hash_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::~SpillingHashJoin() = default;

void SpillingHashJoin::tryConvertChunks(size_t worker_id)
{
    chassert(in_memory_hash_join);
    chassert(grace_join);

    const size_t total_chunks = in_memory_hash_join->getNumReleaseChunks();

    if (next_slot_to_convert.load(std::memory_order_acquire) >= total_chunks)
        return;

    while (true)
    {
        size_t chunk = next_slot_to_convert.fetch_add(1);
        if (chunk >= total_chunks)
            break;

        auto blocks = in_memory_hash_join->releaseJoinedBlocksChunk(chunk);
        while (!blocks.empty())
        {
            grace_join->addBlockToJoin(blocks.front(), blocks.front().rows(), worker_id, /*check_limits=*/false);
            blocks.pop_front();
        }
    }
}

std::string SpillingHashJoin::getName() const
{
    return fmt::format("SpillingHashJoin({})", in_memory_hash_join->getName());
}

bool SpillingHashJoin::supportParallelJoin() const
{
    return in_memory_hash_join->supportParallelJoin();
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits)
{
    /// Fast path: already switched to GraceHashJoin (no lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        /// Lend a hand with the conversion instead of waiting for it.
        if (in_memory_hash_join)
            tryConvertChunks(worker_id);
        return chosen_join->addBlockToJoin(block, num_rows, worker_id, check_limits);
    }

    /// The hash table buffer grows in power-of-two steps. Doubling from X to 2X allocates the new
    /// buffer while the old one is still alive, transiently using 3X memory. We must trigger the
    /// switch BEFORE the inner `addBlockToJoin` runs (and possibly doubles the buffer); a check
    /// that runs after the call would race with the doubling and observe the OOM only as an
    /// allocator exception. Threshold is half of `max_bytes_before_external_join` so that after
    /// the switch the live buffer (already at half) plus the conversion peak still fit under the
    /// configured cap.
    if (collectingJoin().getTotalByteCount() * 2 >= max_bytes_before_external_join)
        switchToGraceHashJoin(worker_id);

    /// Re-check: we may have just switched.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
        return chosen_join->addBlockToJoin(block, num_rows, worker_id, check_limits);

    /// Shared so build threads do not serialize, but still excludes them while it is drained.
    std::shared_lock lock(switch_mutex);

    if (state.load(std::memory_order_acquire) != State::COLLECTING)
        return chosen_join->addBlockToJoin(block, num_rows, worker_id, check_limits);

    return collectingJoin().addBlockToJoin(block, num_rows, worker_id, check_limits);
}

void SpillingHashJoin::switchToGraceHashJoin(size_t worker_id)
{
    {
        std::unique_lock lock(switch_mutex);

        if (state.load(std::memory_order_relaxed) != State::COLLECTING)
            return;

        LOG_DEBUG(
            log,
            "Memory spill threshold reached with {} ({} bytes, {} rows), switching to GraceHashJoin",
            in_memory_hash_join->getName(),
            in_memory_hash_join->getTotalByteCount(),
            in_memory_hash_join->getTotalRowCount());
        ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);

        grace_join = std::make_shared<GraceHashJoin>(
            initial_num_buckets,
            max_num_buckets,
            table_join,
            left_sample_block,
            std::make_shared<const Block>(right_sample_block),
            tmp_data,
            any_take_last_row,
            max_bytes_before_external_join,
            max_threads);

        grace_join->initialize(*left_sample_block);
        chosen_join = grace_join;

        state.store(State::GRACE_HASH_JOIN, std::memory_order_release);

        /// Under the lock: a build thread that got in before the state flipped is still inside
        /// the in-memory join. Freeing here also drops the maps before the conversion peak.
        in_memory_hash_join->releaseJoinMaps();
    }

    tryConvertChunks(worker_id);
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        /// Safety net for the terminal block: the proactive pre-insert check in `addBlockToJoin`
        /// fires only on subsequent calls. If the very last block pushed total bytes past
        /// `max_bytes_before_external_join` without a follow-up insert to trigger the switch,
        /// promote it to `GraceHashJoin` here so the configured cap is honored.
        const size_t total_bytes = collectingJoin().getTotalByteCount();
        if (total_bytes >= max_bytes_before_external_join)
        {
            switchToGraceHashJoin(/* worker_id = */ 0);
        }
        else
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} rows), promoting {}",
                total_bytes,
                collectingJoin().getTotalRowCount(),
                collectingJoin().getName());
            chosen_join = in_memory_hash_join;
            state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
        }
    }

    chosen_join->onBuildPhaseFinish();
}

void SpillingHashJoin::onProbePhaseFinish(size_t matched_right_rows)
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
    if (in_memory_hash_join)
        in_memory_hash_join->setEnableLazyColumnsIndexing(value);
}

void SpillingHashJoin::checkTypesOfKeys(const Block & block) const
{
    collectingJoin().checkTypesOfKeys(block);
}

void SpillingHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    collectingJoin().initialize(sample_block);
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block)
{
    /// During header computation (transformHeader), `joinBlock` is called with an empty block
    /// before any data is added. Delegate to the in-memory join in COLLECTING state.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return collectingJoin().joinBlock(std::move(block));

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
    {
        return collectingJoin().getAnalysisReport();
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

}
