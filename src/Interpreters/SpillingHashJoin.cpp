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

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    size_t num_threads_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_,
    std::optional<size_t> build_rows_hint_)
    : log(getLogger("SpillingHashJoin"))
    , table_join(std::move(table_join_))
    , left_sample_block(std::move(left_sample_block_))
    , right_sample_block(right_sample_block_->cloneEmpty())
    , tmp_data(std::move(tmp_data_))
    , initial_num_buckets(initial_num_buckets_)
    , max_num_buckets(max_num_buckets_)
    , any_take_last_row(any_take_last_row_)
    , max_bytes_before_external_join(table_join->maxBytesBeforeExternalJoin())
    , max_threads(std::max<size_t>(1, num_threads_))
{
    partitioned_join = std::make_shared<HashJoin>(
        table_join,
        right_sample_block_,
        max_threads,
        any_take_last_row,
        stats_collecting_params_,
        max_bytes_before_external_join,
        build_rows_hint_);
    supports_parallel_non_joined_blocks_processing = partitioned_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::~SpillingHashJoin() = default;

void SpillingHashJoin::tryConvertFillLanes(size_t worker_id)
{
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
        /// alive while grace allocates buckets for the block being inserted.
        while (true)
        {
            Block block = partitioned_join->releaseNextFillLaneBlock(lane);
            if (block.empty())
                break;
            grace_join->addBlockToJoin(block, block.rows(), worker_id, /*check_limits=*/false);
        }
    }
}

std::string SpillingHashJoin::getName() const
{
    return fmt::format("SpillingHashJoin({})", partitioned_join->getName());
}

bool SpillingHashJoin::supportParallelJoin() const
{
    return partitioned_join->supportParallelJoin();
}

bool SpillingHashJoin::emitsSizedOutputBlocks() const
{
    /// Only the in-memory join that survived the build can promise sized blocks. After a switch the grace
    /// join emits one bucket's share of each probe block, and that output needs the squashing.
    return state.load(std::memory_order_acquire) == State::IN_MEMORY_JOIN && chosen_join && chosen_join->emitsSizedOutputBlocks();
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, size_t num_rows, size_t worker_id, bool check_limits)
{
    /// Fast path: already switched to GraceHashJoin (no lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        /// Lend a hand with the conversion instead of waiting for it.
        tryConvertFillLanes(worker_id);
        return chosen_join->addBlockToJoin(block, num_rows, worker_id, check_limits);
    }

    /// Checked BEFORE the inner `addBlockToJoin` runs. `predictedResidentBytes` already counts the
    /// table the barrier is going to build, so the switch fires while the resident data plus the
    /// conversion peak still fit under the configured cap.
    if (partitioned_join->predictedResidentBytes() >= max_bytes_before_external_join)
        switchToGraceHashJoin(worker_id);

    /// Re-check: we may have just switched.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
        return chosen_join->addBlockToJoin(block, num_rows, worker_id, check_limits);

    /// Shared so build threads do not serialize, but still excludes them while it is drained.
    std::shared_lock lock(switch_mutex);

    if (state.load(std::memory_order_acquire) != State::COLLECTING)
        return chosen_join->addBlockToJoin(block, num_rows, worker_id, check_limits);

    return partitioned_join->addBlockToJoin(block, num_rows, worker_id, check_limits);
}

void SpillingHashJoin::createGraceJoin(size_t initial_buckets_hint)
{
    grace_join = std::make_shared<GraceHashJoin>(
        std::max(initial_buckets_hint, initial_num_buckets),
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
}

void SpillingHashJoin::switchToGraceHashJoin(size_t worker_id, bool spill_immediately)
{
    {
        std::unique_lock lock(switch_mutex);

        if (state.load(std::memory_order_relaxed) != State::COLLECTING)
            return;

        LOG_DEBUG(
            log,
            "{}, switching to GraceHashJoin: {} holds {} bytes in {} rows",
            spill_immediately ? "Spill requested under memory pressure" : "Memory spill threshold reached",
            partitioned_join->getName(),
            partitioned_join->getTotalByteCount(),
            partitioned_join->getTotalRowCount());
        ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);

        createGraceJoin();
        if (spill_immediately)
            grace_join->requestSpill();

        state.store(State::GRACE_HASH_JOIN, std::memory_order_release);

        /// Under the lock: a build thread that got in before the state flipped is still inside
        /// the in-memory join. Freeing here also drops the transients before the conversion peak.
        partitioned_join->dropFillAuxiliary();

        /// A single fill thread has no lanes: its rows sit in the stored blocks. This thread is the
        /// only one filling, so it hands them over here. A build that stored nothing keeps its data.
        if (partitioned_join->isSingleLaneBuild())
        {
            partitioned_join->beginStoredBlockDrain();
            if (partitioned_join->getTotalRowCount() > 0)
                partitioned_join->drainStoredBlocksInto(*grace_join);
        }
    }

    tryConvertFillLanes(worker_id);
}

size_t SpillingHashJoin::getSpillableBytes() const
{
    switch (state.load(std::memory_order_acquire))
    {
        case State::COLLECTING:
            /// Switching to GraceHashJoin puts what was collected on disk, except the one bucket it keeps
            /// in memory. An upper bound is fine here, the scheduler only ranks candidates by it.
            return partitioned_join->getTotalByteCount();
        case State::GRACE_HASH_JOIN:
            return chosen_join->getSpillableBytes();
        case State::IN_MEMORY_JOIN:
            /// Build phase finished in memory, nothing left to spill.
            return 0;
    }
}

void SpillingHashJoin::requestSpill()
{
    switch (state.load(std::memory_order_acquire))
    {
        case State::COLLECTING:
            /// `switchToGraceHashJoin` re-checks the state, so a concurrent switch is harmless.
            switchToGraceHashJoin(/*worker_id=*/0, /*spill_immediately=*/true);
            return;
        case State::GRACE_HASH_JOIN:
            chosen_join->requestSpill();
            return;
        case State::IN_MEMORY_JOIN:
            return;
    }
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        /// Safety net for the terminal block: the proactive pre-insert check in `addBlockToJoin`
        /// fires only on subsequent calls. If the very last block pushed the prediction past
        /// `max_bytes_before_external_join` without a follow-up insert to trigger the switch,
        /// promote it to `GraceHashJoin` here so the configured cap is honored.
        if (partitioned_join->predictedResidentBytes(/*at_barrier=*/true) >= max_bytes_before_external_join)
        {
            switchToGraceHashJoin(/* worker_id = */ 0);
        }
        else
        {
            /// The barrier concatenates the lanes, numbers the row-store blocks and merges the sketches;
            /// `planPostBuild` then judges the resident set against the budget.
            partitioned_join->onBuildPhaseFinish();
            const auto plan = partitioned_join->planPostBuild();
            if (plan == HashJoin::PostBuildPlan::MustSpill)
            {
                ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);

                /// `GraceHashJoin` spills a bucket at half the threshold (`hasMemoryOverflow`), so that is
                /// the per-bucket capacity. The bucket count comes from the barrier's exact totals; otherwise
                /// the grace join would double its buckets step by step and re-scatter the same rows once
                /// per doubling.
                const size_t in_memory_estimate = partitioned_join->graceInMemoryEstimateBytes();
                const size_t bucket_capacity = max_bytes_before_external_join / 2;
                const size_t buckets_hint = bucket_capacity ? (in_memory_estimate + bucket_capacity - 1) / bucket_capacity : 0;
                LOG_DEBUG(
                    log,
                    "Post-build gate: resident data does not fit ({} bytes, {} rows), switching to GraceHashJoin "
                    "(estimated {} bytes in memory, {} initial buckets)",
                    partitioned_join->getTotalByteCount(),
                    partitioned_join->getTotalRowCount(),
                    in_memory_estimate,
                    buckets_hint);

                createGraceJoin(buckets_hint);

                /// The barrier already consumed every fill lane, so a late helper finds nothing to
                /// convert. It must still find a `grace_join` to convert into.
                next_fill_lane_to_convert.store(partitioned_join->getNumFillLanes(), std::memory_order_release);

                partitioned_join->dropFillAuxiliary();
                partitioned_join->beginStoredBlockDrain();
                partitioned_join->drainStoredBlocksInto(*chosen_join);
                state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
            }
            else
            {
                LOG_DEBUG(
                    log,
                    "All blocks fit in memory ({} bytes, {} rows), promoting HashJoin",
                    partitioned_join->getTotalByteCount(),
                    partitioned_join->getTotalRowCount());
                chosen_join = partitioned_join;
                state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
            }
        }
    }

    /// The collecting join already ran its barrier above. Calling it again would be undefined after
    /// a post-barrier drain, and redundant after an in-memory promotion.
    if (state.load(std::memory_order_acquire) == State::IN_MEMORY_JOIN)
        return;

    chosen_join->onBuildPhaseFinish();
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
    partitioned_join->setEnableLazyColumnsIndexing(value);
}

void SpillingHashJoin::checkTypesOfKeys(const Block & block) const
{
    partitioned_join->checkTypesOfKeys(block);
}

void SpillingHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    partitioned_join->initialize(sample_block);
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block)
{
    /// During header computation (transformHeader), `joinBlock` is called with an empty block
    /// before any data is added. Delegate to the in-memory join in COLLECTING state.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return partitioned_join->joinBlock(std::move(block));

    return chosen_join->joinBlock(std::move(block));
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block, size_t lane)
{
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
        return partitioned_join->getTotalRowCount();
    return chosen_join->getTotalRowCount();
}

size_t SpillingHashJoin::getTotalByteCount() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return partitioned_join->getTotalByteCount();
    return chosen_join->getTotalByteCount();
}

bool SpillingHashJoin::alwaysReturnsEmptySet() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return partitioned_join->alwaysReturnsEmptySet();
    return chosen_join->alwaysReturnsEmptySet();
}

StepAnalysisReport SpillingHashJoin::getAnalysisReport() const
{
    /// This method always runs after the built phase, so in principal we could have
    /// written it without this if statement. However, we keep it
    /// for canonicity with the other accessors and safety in case the call order ever changes.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        return partitioned_join->getAnalysisReport();
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
