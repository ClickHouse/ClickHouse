#include <Interpreters/SpillingHashJoin.h>

#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinPartitionedBuild.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/QueryExecutionCounters.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerUtils.h>
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
extern const int FAULT_INJECTED;
extern const int LOGICAL_ERROR;
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
    hash_join = HashJoin::create(table_join, right_sample_block_, build_layout_, any_take_last_row, stats_collecting_params_);
    /// Until the build phase ends this join may have to hand its right blocks to `GraceHashJoin`.
    hash_join->keepRightBlocksForAnotherAlgorithm();
    /// A memory spill scheduler that switches the join on memory pressure needs to see the table grow before the limit.
    /// The wait of several build streams ends at build finish, where `getMemoryStats` reports the build to come.
    if (!hash_join->supportParallelJoin())
        if (const auto hard_limit = getCurrentQueryHardLimit())
            hash_join->setWaitMemoryBound(*hard_limit / 2);
    supports_parallel_non_joined_blocks_processing = hash_join->supportParallelNonJoinedBlocksProcessing();
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

void SpillingHashJoin::tryConvertBuildStreams(JoinBuildContext context)
{
    chassert(grace_join || merge_join);

    const size_t num_build_streams = hash_join->getMaxBuildThreads();

    if (next_build_stream_to_convert.load(std::memory_order_acquire) >= num_build_streams)
        return;

    while (true)
    {
        size_t stream = next_build_stream_to_convert.fetch_add(1);
        if (stream >= num_build_streams)
            break;

        auto blocks = hash_join->releaseBuildStreamBlocks(stream);
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
    switch (state.load(std::memory_order_acquire))
    {
        case State::GRACE_HASH_JOIN:
            return toString(JoinAlgorithm::GRACE_HASH);
        case State::MERGE_JOIN:
            return toString(JoinAlgorithm::PARTIAL_MERGE);
        case State::COLLECTING:
        case State::IN_MEMORY_JOIN:
        case State::FAILED:
            return toString(JoinAlgorithm::HASH);
    }
}

bool SpillingHashJoin::addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    /// Fast path: already switched to the external join (no switch lock needed).
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        throwIfSwitchFailed();
        /// Help convert the HashJoin build streams into the external join.
        tryConvertBuildStreams(context);
        return addBlockToExternalJoin(block, num_rows, context);
    }

    /// Before the inner `addBlockToJoin`: a check after it would race with a doubling of the table.
    if (overBudgetBeforeBlock())
        switchToExternalJoin(context);

    /// Re-check: we may have just switched.
    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        throwIfSwitchFailed();
        tryConvertBuildStreams(context);
        return addBlockToExternalJoin(block, num_rows, context);
    }

    /// Shared so build threads do not serialize, but still excludes them while it is drained.
    std::shared_lock lock(switch_mutex);

    if (state.load(std::memory_order_acquire) != State::COLLECTING)
    {
        throwIfSwitchFailed();
        return addBlockToExternalJoin(block, num_rows, context);
    }

    return hash_join->addBlockToJoin(block, num_rows, context);
}

bool SpillingHashJoin::overBudgetBeforeBlock() const
{
    /// A table that exists grows in power-of-two steps. Doubling from X to 2X allocates the new
    /// buffer while the old one is still alive, transiently using 3X memory. The threshold is half of
    /// `max_bytes_before_external_join`, so that after the switch the live buffer (already at half)
    /// plus the conversion peak still fit under the configured cap.
    if (!hash_join->isWaiting() || hash_join->builtEarly())
        return hash_join->getTotalByteCount() * 2 >= max_bytes_before_external_join;

    /// While the join waits, its tables do not exist yet, and the projection counts them. It leaves out the copy that
    /// scatters the keys to the partitions: a build in rounds keeps that copy small, and spilling such a build would be
    /// far slower than building it in memory. The tables of several build streams are reserved once from the key
    /// estimate and do not double. The table of one build stream grows as above.
    const size_t projected_bytes = hash_join->getProjectedByteCount();
    if (hash_join->supportParallelJoin())
        return projected_bytes >= max_bytes_before_external_join;
    return projected_bytes * 2 >= max_bytes_before_external_join;
}

bool SpillingHashJoin::isLastTurnBeforeBuild() const
{
    /// `onBuildPhaseFinish` runs in `prepare` of the last filling transform and the build in its `work`. The
    /// executor gives the memory scheduler one turn in between, and never during the build.
    return state.load(std::memory_order_acquire) == State::IN_MEMORY_JOIN && !hash_join->isBuildFinished();
}

ProcessorMemoryStats SpillingHashJoin::getMemoryStats() const
{
    if (state.load(std::memory_order_acquire) != State::COLLECTING && !isLastTurnBeforeBuild())
    {
        /// Nothing to report for the MergeJoin fallback either: it writes the right side through
        /// its disk writer as it arrives, so a pinned join holds no spillable memory.
        if (!grace_join)
            return {};

        auto res = grace_join->getMemoryStats();
        if (getMaxBuildThreads() > 1)
        {
            /// Unconverted build streams still own memory; their counters are cleared by `releaseBuildStreamBlocks`.
            const auto unconverted_bytes = hash_join->getTotalByteCount();
            res.spillable_memory_bytes += unconverted_bytes;
            res.need_reserved_memory_bytes += unconverted_bytes;
        }
        return res;
    }

    /// While collecting, and in the last turn before the build. A switch frees what the join holds. A join that waits
    /// holds only its blocks, and its tables and the scatter copy of its rounds come with the build, where the scheduler
    /// gets no turn. So its need is the growth still to come plus, as for a built join, its whole size once built, because
    /// the released blocks stay alive while the grace join re-inserts them. Once built, the need is its bytes.
    const size_t allocated_bytes = hash_join->getTotalByteCount();
    const size_t projected_bytes = std::max(hash_join->getProjectedByteCount() + hash_join->getProjectedCopyBytes(), allocated_bytes);
    ProcessorMemoryStats res;
    res.spillable_memory_bytes = allocated_bytes;
    res.need_reserved_memory_bytes = projected_bytes + (projected_bytes - allocated_bytes);
    return res;
}

size_t SpillingHashJoin::spill(size_t at_least_bytes)
{
    if (!at_least_bytes)
        return 0;

    throwIfSwitchFailed();

    const bool last_turn = isLastTurnBeforeBuild();
    if (last_turn || state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        size_t before = getTotalByteCount();
        switchToExternalJoin(JoinBuildContext::serial(), /*spill_immediately=*/true);
        /// The in-memory join already had its `onBuildPhaseFinish`; the external join that replaces it needs its own.
        if (last_turn && state.load(std::memory_order_acquire) != State::IN_MEMORY_JOIN)
            chosen_join->onBuildPhaseFinish();
        size_t after = getTotalByteCount();
        /// Check the new grace bucket's spillable estimate before requesting a repartition.
        return before > after ? before - after : 0;
    }

    /// The MergeJoin fallback spills by construction, so a pinned join has nothing more to
    /// release here (`grace_join` stays null).
    return grace_join ? grace_join->spill(at_least_bytes) : 0;
}

void SpillingHashJoin::switchToExternalJoin(
    JoinBuildContext context, bool spill_immediately, size_t num_buckets_hint, size_t conversion_workers)
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
    size_t num_buckets = std::max(initial_num_buckets, num_buckets_hint);
    if (spill_immediately)
        num_buckets = std::max<size_t>(num_buckets, 2);

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
            max_bytes_before_external_join,
            getMaxBuildThreads());
        grace_join->initialize(*left_sample_block);
        return grace_join;
    };

    {
        std::unique_lock lock(switch_mutex);

        if (state.load(std::memory_order_relaxed) != State::COLLECTING && !isLastTurnBeforeBuild())
            return;

        LOG_DEBUG(
            log,
            "{}, switching to {}: {} holds {} bytes in {} rows",
            spill_immediately ? "Spill requested under memory pressure" : "Memory spill threshold reached",
            keep_left_block_order ? "MergeJoin (the plan requires the left block order)" : "GraceHashJoin",
            hash_join->getName(),
            hash_join->getTotalByteCount(),
            hash_join->getTotalRowCount());
        ProfileEvents::increment(switch_event);

        chosen_join = make_external_join();

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
                tryConvertBuildStreams(context);
            }
            catch (...)
            {
                /// The maps are freed and some blocks may be gone too, so neither join can take another block.
                /// A retry would build the external join from what is left. Threads waiting on the lock must see
                /// the failure, not `COLLECTING`.
                switch_error_code = getCurrentExceptionCode();
                switch_error_message = getCurrentExceptionMessage(/*with_stacktrace=*/ false);
                state.store(State::FAILED, std::memory_order_release);
                throw;
            }
        }

        state.store(target_state, std::memory_order_release);
        QueryExecutionCounters::addUsedJoinAlgorithm(target_algorithm);
    }

    if (spill_immediately)
        return;

    /// Every insert into the in-memory join holds `switch_mutex` shared, so none runs any more, and threads
    /// queued on the lock need not wait for the free. Freeing before the conversion lowers its peak.
    hash_join->releaseJoinMaps();
    if (conversion_workers <= 1)
    {
        tryConvertBuildStreams(context);
        return;
    }
    /// Each worker inserts with a stream id of its own, as the filling threads do.
    std::atomic<bool> stop = false;
    JoinPartitionedBuild::runWorkers(
        conversion_workers, stop, [&](size_t worker) { tryConvertBuildStreams(JoinBuildContext::forStream(worker, conversion_workers)); });
}

void SpillingHashJoin::throwIfSwitchFailed() const
{
    /// Each thread gets its own exception, because the pipeline appends to the message of the one it catches.
    if (state.load(std::memory_order_acquire) == State::FAILED)
        throw Exception(
            switch_error_code,
            "Switching the join to {} failed: {}",
            keep_left_block_order ? "MergeJoin" : "GraceHashJoin",
            switch_error_message);
}

void SpillingHashJoin::onBuildPhaseFinish()
{
    throwIfSwitchFailed();

    if (state.load(std::memory_order_acquire) == State::COLLECTING)
    {
        /// The pre-insert check in `addBlockToJoin` fires only on a following block, so the last block may have
        /// pushed the join past `max_bytes_before_external_join`. A join that waits is judged by the build it would
        /// run: at once, in rounds, or not at all.
        const bool must_spill = hash_join->getTotalByteCount() >= max_bytes_before_external_join
            || hash_join->evaluateBuildFit(max_bytes_before_external_join) == HashJoin::BuildFit::MustSpill;
        if (must_spill)
        {
            /// `GraceHashJoin` splits a bucket at half the threshold (`hasMemoryOverflow`), so that is the capacity of
            /// one bucket. Starting with enough buckets saves re-scattering the same rows once per doubling.
            const size_t projected_bytes = hash_join->getProjectedByteCount();
            const size_t bucket_capacity = max_bytes_before_external_join / 2;
            const size_t num_buckets_hint = bucket_capacity ? (projected_bytes + bucket_capacity - 1) / bucket_capacity : 0;
            LOG_DEBUG(
                log,
                "The build does not fit in memory ({} bytes now, {} bytes projected, {} rows), {} initial buckets",
                hash_join->getTotalByteCount(),
                projected_bytes,
                hash_join->getTotalRowCount(),
                num_buckets_hint);
            /// Every filling thread but this one has finished, so the build streams convert on threads of their own. The
            /// `MergeJoin` fallback takes one block at a time.
            switchToExternalJoin(
                JoinBuildContext::serial(),
                /*spill_immediately=*/false,
                num_buckets_hint,
                keep_left_block_order ? 1 : getMaxBuildThreads());
        }
        else
        {
            LOG_DEBUG(
                log,
                "All blocks fit in memory ({} bytes, {} bytes projected, {} rows), promoting HashJoin",
                hash_join->getTotalByteCount(),
                hash_join->getProjectedByteCount(),
                hash_join->getTotalRowCount());
            chosen_join = hash_join;
            state.store(State::IN_MEMORY_JOIN, std::memory_order_release);
        }
    }

    chosen_join->onBuildPhaseFinish();

    /// Once the build has run, no switch can happen, so whatever right blocks were kept in case it did are dead
    /// weight for the whole probe phase, and a join that stores only the keys can drop them. `HashJoin` runs the drop
    /// at the end of its build, which still reads the blocks of a key-only table.
    if (state.load(std::memory_order_acquire) == State::IN_MEMORY_JOIN)
        hash_join->dropRightBlocksKeptForAnotherAlgorithm();
}

bool SpillingHashJoin::canPreserveLeftBlockOrder() const
{
    /// Hash-key equality must survive the switch: the hash join matches the key's byte
    /// representation while MergeJoin matches with `compareAt`, and for some types the two
    /// disagree - floating point (`-0.0` == `+0.0`, NaNs with different payloads compare equal
    /// but hash differently), `JSON` and `Dynamic` (the hash depends on the physical layout) -
    /// also when nested. Crossing the spill threshold must not change the result, so such keys
    /// cannot take the order-preserving fallback (see `keyTypeBreaksHashSharding`).
    auto keys_preserve_hash_equality = [this]
    {
        auto keys_ok = [](const Names & key_names, const Block & header)
        {
            for (const auto & name : key_names)
            {
                const auto * key = header.findByName(name);
                /// Be conservative when a key column cannot be found.
                if (!key || QueryPlanOptimizations::keyTypeBreaksHashSharding(*key->type))
                    return false;
            }
            return true;
        };

        const auto & on_clause = table_join->getOnlyClause();
        return keys_ok(on_clause.key_names_left, *left_sample_block) && keys_ok(on_clause.key_names_right, right_sample_block);
    };

    /// The spill fallback must be able to run as an order-preserving MergeJoin. `any_take_last_row`
    /// has no MergeJoin counterpart: it picks the first match in the right sort order instead.
    return MergeJoin::isSupported(table_join)
        && (table_join->strictness() == JoinStrictness::Any || table_join->strictness() == JoinStrictness::All)
        && isInnerOrLeft(table_join->kind()) && !(any_take_last_row && table_join->strictness() == JoinStrictness::Any)
        && keys_preserve_hash_equality();
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

    hash_join->keepLeftPipelineInOrder();
    keep_left_block_order = true;
}

void SpillingHashJoin::allowLeftRowsReordering()
{
    if (!keep_left_block_order)
        hash_join->allowLeftRowsReordering();
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
    /// `hasDelayedBlocks` is false under the left-order pin: the pipeline is then built without
    /// the delayed-block transforms, and the MergeJoin fallback has none to process anyway (it
    /// inherits the nullptr `IJoin::getDelayedBlocks`). The conjunction is defensive - no
    /// `JoiningTransform` with a delayed input can observe a pinned join.
    return hasDelayedBlocks() && state.load(std::memory_order_acquire) != State::IN_MEMORY_JOIN;
}

}
