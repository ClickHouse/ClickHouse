#include <Interpreters/SpillingHashJoin.h>

#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/QueryExecutionCounters.h>
#include <Interpreters/TableJoin.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace ProfileEvents
{
extern const Event JoinSpillingHashJoinSwitchedToGraceJoin;
}

namespace DB
{

namespace ErrorCodes
{
extern const int FAULT_INJECTED;
}

namespace FailPoints
{
extern const char spilling_hash_join_throw_after_maps_release[];
}

SpillingHashJoin::SpillingHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader left_sample_block_,
    SharedHeader right_sample_block_,
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t initial_num_buckets_,
    size_t max_num_buckets_,
    HashJoinBuildLayout build_layout_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_,
    bool allow_deferred_build_)
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
    hash_join = HashJoin::create(
        table_join, right_sample_block_, build_layout_, any_take_last_row, stats_collecting_params_, allow_deferred_build_);
    /// Until the build phase ends this join may have to hand its right blocks to `GraceHashJoin`.
    hash_join->keepRightBlocksForAnotherAlgorithm();
    supports_parallel_non_joined_blocks_processing = hash_join->supportParallelNonJoinedBlocksProcessing();
}

SpillingHashJoin::~SpillingHashJoin() = default;

void SpillingHashJoin::tryConvertWorkers(JoinBuildContext context)
{
    chassert(grace_join);

    const size_t num_workers = hash_join->getMaxBuildThreads();

    if (next_worker_to_convert.load(std::memory_order_acquire) >= num_workers)
        return;

    while (true)
    {
        size_t worker = next_worker_to_convert.fetch_add(1);
        if (worker >= num_workers)
            break;

        auto blocks = hash_join->releaseWorkerBlocks(worker);
        while (!blocks.empty())
        {
            grace_join->addBlockToJoin(blocks.front(), blocks.front().rows(), context.callerChecksLimits());
            blocks.pop_front();
        }
    }
}

std::string SpillingHashJoin::getName() const
{
    static constexpr auto name_format = "SpillingHashJoin({})";

    return fmt::format(name_format, hash_join->getName());
}

bool SpillingHashJoin::supportParallelJoin() const
{
    return hash_join->supportParallelJoin();
}

size_t SpillingHashJoin::getMaxBuildThreads() const
{
    return hash_join->getMaxBuildThreads();
}

std::string SpillingHashJoin::getAlgorithm() const
{
    if (state.load(std::memory_order_acquire) == State::GRACE_HASH_JOIN)
        return toString(JoinAlgorithm::GRACE_HASH);

    return toString(JoinAlgorithm::HASH);
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    /// Fast path: already switched to GraceHashJoin (no lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        throwIfSwitchFailed();
        tryConvertWorkers(context);
        return chosen_join->addBlockToJoin(block, num_rows, context);
    }

    /// The hash table buffer grows in power-of-two steps. Doubling from X to 2X allocates the new
    /// buffer while the old one is still alive, transiently using 3X memory. We must trigger the
    /// switch BEFORE the inner `addBlockToJoin` runs (and possibly doubles the buffer); a check
    /// that runs after the call would race with the doubling and observe the OOM only as an
    /// allocator exception. Threshold is half of `max_bytes_before_external_join` so that after
    /// the switch the live buffer (already at half) plus the conversion peak still fit under the
    /// configured cap.
    if (hash_join->getTotalByteCount() * 2 >= max_bytes_before_external_join)
    {
        switchToGraceHashJoin(context);
    }
    /// A deferred build holds only the stored blocks; its maps are allocated when the blocks are inserted. The check above
    /// keeps room to convert the stored blocks, and this one keeps the maps of the inserts under the threshold.
    else if (hash_join->defersBuild()
             && hash_join->getProjectedBuildBytes(/*all_blocks_stored=*/false) >= max_bytes_before_external_join)
    {
        LOG_DEBUG(log, "The projected size of the deferred build reached the spill threshold while the right blocks arrive");
        switchToGraceHashJoin(context);
    }

    /// Re-check: we may have just switched.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        throwIfSwitchFailed();
        tryConvertWorkers(context);
        return chosen_join->addBlockToJoin(block, num_rows, context);
    }

    /// Shared so build threads do not serialize, but still excludes them while it is drained.
    std::shared_lock lock(switch_mutex);

    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        throwIfSwitchFailed();
        return chosen_join->addBlockToJoin(block, num_rows, context);
    }

    return hash_join->addBlockToJoin(block, num_rows, context);
}

ProcessorMemoryStats SpillingHashJoin::getMemoryStats() const
{
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        if (!grace_join)
            return {};

        auto res = grace_join->getMemoryStats();
        if (getMaxBuildThreads() > 1)
        {
            /// Unconverted workers still own memory; their counters are cleared by `releaseWorkerBlocks`.
            const auto unconverted_bytes = hash_join->getTotalByteCount();
            res.spillable_memory_bytes += unconverted_bytes;
            res.need_reserved_memory_bytes += unconverted_bytes;
        }
        return res;
    }

    ProcessorMemoryStats res;
    res.spillable_memory_bytes = hash_join->getTotalByteCount();
    /// The released blocks stay alive while the grace join re-inserts them.
    res.need_reserved_memory_bytes = res.spillable_memory_bytes;
    /// A deferred build allocates its maps only when it inserts the stored blocks. So it needs the bytes of the maps,
    /// and spilling it pays off only when they do not fit.
    if (hash_join->defersBuild())
    {
        const auto projected_bytes = static_cast<Int64>(hash_join->getProjectedBuildBytes(/*all_blocks_stored=*/false));
        res.need_reserved_memory_bytes = std::max<Int64>(projected_bytes - res.spillable_memory_bytes, 0);
    }
    return res;
}

size_t SpillingHashJoin::spill(size_t at_least_bytes)
{
    if (!at_least_bytes)
        return 0;

    throwIfSwitchFailed();

    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        size_t before = getTotalByteCount();
        switchToGraceHashJoin(JoinBuildContext::serial(), /*spill_immediately=*/true);
        size_t after = getTotalByteCount();
        /// Check the new grace bucket's spillable estimate before requesting a repartition.
        return before > after ? before - after : 0;
    }

    return grace_join ? grace_join->spill(at_least_bytes) : 0;
}

void SpillingHashJoin::switchToGraceHashJoin(JoinBuildContext context, bool spill_immediately)
{
    /// A scheduler-requested switch must partition the data, not just rebuild it in one bucket.
    const size_t num_buckets = spill_immediately ? std::max<size_t>(initial_num_buckets, 2) : initial_num_buckets;
    {
        std::unique_lock lock(switch_mutex);

        if (state.load(std::memory_order_relaxed) != State::COLLECTING)
            return;

        LOG_DEBUG(
            log,
            "Memory spill threshold reached, switching to GraceHashJoin: {} holds {} bytes in {} rows",
            hash_join->getName(),
            hash_join->getTotalByteCount(),
            hash_join->getTotalRowCount());
        ProfileEvents::increment(ProfileEvents::JoinSpillingHashJoinSwitchedToGraceJoin);

        grace_join = std::make_shared<GraceHashJoin>(
            num_buckets,
            max_num_buckets,
            table_join,
            left_sample_block,
            std::make_shared<const Block>(right_sample_block),
            tmp_data,
            any_take_last_row,
            max_bytes_before_external_join,
            getMaxBuildThreads());

        grace_join->initialize(*left_sample_block);
        chosen_join = grace_join;

        /// `spill` has no build-stream identity. Replay under the exclusive lock before publishing
        /// the new join, so its `serial` context cannot overlap another insert into that join.
        if (spill_immediately)
        {
            try
            {
                hash_join->releaseJoinMaps();
                fiu_do_on(FailPoints::spilling_hash_join_throw_after_maps_release,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after the in-memory maps were released");
                });
                tryConvertWorkers(context);
            }
            catch (...)
            {
                /// The maps are freed and some blocks may be gone too, so neither join can take another block.
                /// A retry would build the grace join from what is left. Threads waiting on the lock must see
                /// the failure, not `COLLECTING`.
                switch_error_code = getCurrentExceptionCode();
                switch_error_message = getCurrentExceptionMessage(/*with_stacktrace=*/ false);
                state.store(State::FAILED, std::memory_order_release);
                throw;
            }
        }

        state.store(State::GRACE_HASH_JOIN, std::memory_order_release);
        QueryExecutionCounters::addUsedJoinAlgorithm(JoinAlgorithm::GRACE_HASH);
    }

    if (spill_immediately)
        return;

    /// Every insert into the in-memory join holds `switch_mutex` shared, so none runs any more, and threads
    /// queued on the lock need not wait for the free. Freeing before the conversion lowers its peak.
    hash_join->releaseJoinMaps();
    tryConvertWorkers(context);
}

void SpillingHashJoin::throwIfSwitchFailed() const
{
    /// Each thread gets its own exception, because the pipeline appends to the message of the one it catches.
    if (state.load(std::memory_order_acquire) == State::FAILED)
        throw Exception(switch_error_code, "Switching the join to GraceHashJoin failed: {}", switch_error_message);
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    throwIfSwitchFailed();

    if (state.load(std::memory_order_acquire) == State::COLLECTING && hash_join->defersBuild())
    {
        /// `FillingRightJoinSideTransform` calls this from `prepare`. So both the conversion to `GraceHashJoin` and the
        /// inserts of the deferred build run in `runPostBuildPhase`. Until then the memory spill scheduler can still
        /// switch the join.
        finish_deferred_build = true;
        return;
    }

    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        /// Safety net for the terminal block: the proactive pre-insert check in `addBlockToJoin`
        /// fires only on subsequent calls. If the very last block pushed total bytes past
        /// `max_bytes_before_external_join` without a follow-up insert to trigger the switch,
        /// promote it to `GraceHashJoin` here so the configured cap is honored.
        const size_t total_bytes = hash_join->getTotalByteCount();
        if (total_bytes >= max_bytes_before_external_join)
        {
            switchToGraceHashJoin(JoinBuildContext::serial());
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
        hash_join->dropRightBlocksKeptForAnotherAlgorithm();
}

void SpillingHashJoin::onProbePhaseFinish(std::optional<size_t> matched_right_rows)
{
    chosen_join->onProbePhaseFinish(matched_right_rows);
}

bool SpillingHashJoin::hasPostBuildPhase() const
{
    /// `FillingRightJoinSideTransform` asks this right after `onBuildPhaseFinish`, which sets `chosen_join`
    /// unless a deferred build finishes in `runPostBuildPhase`. Stay defensive anyway: with no chosen join
    /// there is nothing to post-process.
    return finish_deferred_build || (chosen_join && chosen_join->hasPostBuildPhase());
}

void SpillingHashJoin::runPostBuildPhase()
{
    if (finish_deferred_build)
    {
        finish_deferred_build = false;
        /// The memory spill scheduler may have switched the join since `onBuildPhaseFinish`, and that switch may have failed.
        throwIfSwitchFailed();
        if (state.load(std::memory_order_acquire) == State::COLLECTING)
        {
            const size_t projected_bytes = hash_join->getProjectedBuildBytes(/*all_blocks_stored=*/true);
            if (projected_bytes >= max_bytes_before_external_join)
            {
                LOG_DEBUG(
                    log,
                    "The projected size of the deferred build, {} bytes, reached the spill threshold at the end of the build",
                    projected_bytes);
                switchToGraceHashJoin(JoinBuildContext::serial());
            }
            else
            {
                LOG_DEBUG(log, "The deferred build fits in memory ({} bytes projected), promoting HashJoin", projected_bytes);
                chosen_join = hash_join;
                state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
            }
        }

        chosen_join->onBuildPhaseFinish();
        if (chosen_join->hasPostBuildPhase())
            chosen_join->runPostBuildPhase();
        return;
    }

    if (chosen_join)
        chosen_join->runPostBuildPhase();
}

void SpillingHashJoin::setEnableLazyColumnsIndexing(bool value)
{
    hash_join->setEnableLazyColumnsIndexing(value);
}

void SpillingHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_join->checkTypesOfKeys(block);
}

void SpillingHashJoin::initialize(const Block & sample_block)
{
    left_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());
    hash_join->initialize(sample_block);
}

JoinResultPtr SpillingHashJoin::joinBlock(Block block)
{
    /// During header computation (transformHeader), `joinBlock` is called with an empty block
    /// before any data is added. Delegate to the appropriate join in COLLECTING state.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return hash_join->joinBlock(std::move(block));

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
        return hash_join->getTotalRowCount();
    return chosen_join->getTotalRowCount();
}

size_t SpillingHashJoin::getTotalByteCount() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return hash_join->getTotalByteCount();
    return chosen_join->getTotalByteCount();
}

bool SpillingHashJoin::alwaysReturnsEmptySet() const
{
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
        return hash_join->alwaysReturnsEmptySet();
    return chosen_join->alwaysReturnsEmptySet();
}

StepAnalysisReport SpillingHashJoin::getAnalysisReport() const
{
    /// This method always runs after the built phase, so in principal we could have
    /// written it without this if statement. However, we keep it
    /// for canonicity with the other accessors and safety in case the call order ever changes.
    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
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
    return state.load(std::memory_order_acquire) != State::IN_MEMORY_JOIN;
}

}
