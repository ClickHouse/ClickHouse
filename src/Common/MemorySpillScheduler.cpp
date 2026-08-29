#include <algorithm>
#include <mutex>
#include <Common/MemoryTrackerUtils.h>
#include <Common/MemorySpillScheduler.h>
#include <Processors/IProcessor.h>


namespace DB
{
bool MemorySpillScheduler::checkAndSpill(IProcessor * processor)
{
    /// One controller decision gates every prepared task. Non-spillable work cannot contribute to
    /// recovery and may allocate, so it stays parked for the complete graph-pressure episode too.
    /// PipelineExecutor's outer scan boundary linearizes an episode which opens concurrently with
    /// a task admitted just before this observation.
    if (!processor->isSpillable())
        return pressure_round.load(std::memory_order_acquire) != 0;

    const auto stats = processor->getMemoryStats();
    UInt64 forced_epoch = 0;
    bool pressure_active = false;
    bool force_spill = false;
    bool claimed = false;

    {
        std::lock_guard lock(mutex);
        auto it = processor_states.find(processor);
        chassert(it != processor_states.end());
        if (it == processor_states.end())
            return pressure_round.load(std::memory_order_relaxed) != 0;

        /// Read the epoch only after collecting statistics and taking the scheduler mutex. If an
        /// epoch opens while getMemoryStats() runs, this work must still observe and offer itself
        /// to that epoch instead of letting the following graph scan prove false exhaustion.
        forced_epoch = forced_spill_request_epoch.load(std::memory_order_relaxed);
        pressure_active = pressure_round.load(std::memory_order_relaxed) != 0;
        force_spill = pressure_active
            && forced_spill_completed_epoch.load(std::memory_order_relaxed) < forced_epoch;

        auto & state = it->second;
        state.stats = stats;

        /// Forced recovery belongs to the query. The first runnable processor that can actually
        /// spill claims the epoch. The processor table is populated before pressure begins, so
        /// this path performs no allocation.
        if (force_spill
            && state.retired_forced_epoch < forced_epoch
            && stats.spillable_memory_bytes > 0
            && forced_spill_claimed_epoch.load(std::memory_order_relaxed) < forced_epoch)
        {
            forced_spill_claimant = processor;
            forced_spill_claimed_epoch.store(forced_epoch, std::memory_order_release);
            claimed = true;
        }
        if (force_spill)
            state.retired_forced_epoch = forced_epoch;
    }

    if (force_spill)
    {
        if (!claimed)
            return true;

        bool spill_succeeded = false;
        Int64 reclaimed_bytes = 0;
        try
        {
            const Int64 memory_before = getCurrentQueryMemoryUsage();
            spill_succeeded = processor->spillOnSize(stats.spillable_memory_bytes);
            const Int64 memory_after = getCurrentQueryMemoryUsage();
            reclaimed_bytes = std::max<Int64>(memory_before - memory_after, 0);
        }
        catch (...)
        {
            /// A failed recovery task must not strand the graph in Pending. Publish the bounded
            /// no-progress result before preserving the processor exception for normal query
            /// cancellation/error handling.
            std::lock_guard lock(mutex);
            if (forced_spill_request_epoch.load(std::memory_order_acquire) == forced_epoch
                && forced_spill_claimant == processor)
            {
                forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
                forced_spill_reclaimed_bytes.store(0, std::memory_order_relaxed);
                forced_spill_result_epoch.store(forced_epoch, std::memory_order_release);
                forced_spill_completed_epoch.store(forced_epoch, std::memory_order_release);
            }
            throw;
        }

        std::lock_guard lock(mutex);
        if (forced_spill_request_epoch.load(std::memory_order_acquire) == forced_epoch
            && forced_spill_claimant == processor
            && pressure_round.load(std::memory_order_acquire) != 0)
        {
            forced_spill_reclaimed_bytes.store(reclaimed_bytes, std::memory_order_relaxed);
            forced_spill_outcome.store(
                spill_succeeded || reclaimed_bytes > 0
                    ? ForcedSpillOutcome::Progress
                    : ForcedSpillOutcome::NoProgress,
                std::memory_order_relaxed);
            forced_spill_result_epoch.store(forced_epoch, std::memory_order_release);
            forced_spill_completed_epoch.store(forced_epoch, std::memory_order_release);
        }
        return true;
    }

    /// Completion is only the query-to-scheduler hand-off. Keep every prepared task in the
    /// recovery lane until the allocation scheduler approves, kills, or otherwise resolves all
    /// requesters in this dependency graph and closes pressure_round.
    if (pressure_active)
        return true;

    if (!enable || !getHardLimit())
        return false;

    if (processor && processor == selectSpilledProcessor(processor, stats, false))
        processor->spillOnSize(stats.spillable_memory_bytes);
    return false;
}

void MemorySpillScheduler::registerProcessor(IProcessor * processor)
{
    if (!processor->isSpillable())
        return;
    std::lock_guard lock(mutex);
    processor_states.try_emplace(processor);
    updateTopProcessor();
}

void MemorySpillScheduler::setProcessorRunnable(IProcessor * processor, bool runnable)
{
    if (!processor->isSpillable())
        return;

    std::lock_guard lock(mutex);
    auto it = processor_states.find(processor);
    if (it == processor_states.end())
        return;

    const bool became_runnable = runnable && !it->second.runnable;
    it->second.runnable = runnable;

    /// A stable all-zero census is allowed to finish immediately; we never reserve capacity for
    /// hypothetical future work. If a concrete spillable node becomes executable before suction
    /// priority is injected, that event opens exactly one new recovery epoch for the same graph.
    /// The previous result stays monotonic for lagging observers, while the parked allocation's
    /// next scheduler pass receives the new Pending epoch.
    const UInt64 requested = forced_spill_request_epoch.load(std::memory_order_relaxed);
    if (became_runnable
        && pressure_round.load(std::memory_order_relaxed) == 1
        && forced_spill_completed_epoch.load(std::memory_order_relaxed) >= requested
        && forced_spill_outcome.load(std::memory_order_relaxed) == ForcedSpillOutcome::NoProgress)
    {
        forced_spill_claimant = nullptr;
        forced_spill_request_epoch.store(requested + 1, std::memory_order_release);
    }
}

void MemorySpillScheduler::beginForcedSpillScan()
{
    std::lock_guard lock(mutex);
    ++active_forced_spill_scans;
}

void MemorySpillScheduler::finishForcedSpillScan()
{
    std::lock_guard lock(mutex);
    chassert(active_forced_spill_scans > 0);
    if (active_forced_spill_scans == 0)
        return;
    --active_forced_spill_scans;
    if (active_forced_spill_scans != 0 || pressure_round.load(std::memory_order_relaxed) == 0)
        return;

    const UInt64 requested = forced_spill_request_epoch.load(std::memory_order_relaxed);
    const bool pending = pressure_round.load(std::memory_order_relaxed) != 0
        && forced_spill_completed_epoch.load(std::memory_order_relaxed) < requested
        && forced_spill_claimed_epoch.load(std::memory_order_relaxed) < requested;
    if (!pending)
        return;

    const bool has_recovery_candidate = std::any_of(
        processor_states.begin(), processor_states.end(),
        [requested](const auto & entry)
        {
            return entry.second.runnable && entry.second.retired_forced_epoch < requested;
        });
    if (!has_recovery_candidate)
    {
        forced_spill_claimant = nullptr;
        forced_spill_claimed_epoch.store(requested, std::memory_order_relaxed);
        forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
        forced_spill_reclaimed_bytes.store(0, std::memory_order_relaxed);
        forced_spill_result_epoch.store(requested, std::memory_order_release);
        forced_spill_completed_epoch.store(requested, std::memory_order_release);
    }
}

MemorySpillScheduler::ForcedSpillRequest MemorySpillScheduler::requestForcedSpill(bool register_requester)
{
    std::lock_guard lock(mutex);

    if (register_requester)
        ++pressure_requesters;

    const UInt64 current_round = pressure_round.load(std::memory_order_acquire);
    const UInt64 requested_epoch = forced_spill_request_epoch.load(std::memory_order_acquire);
    const UInt64 completed_epoch = forced_spill_completed_epoch.load(std::memory_order_acquire);

    if (current_round == 0)
    {
        const UInt64 new_epoch = requested_epoch + 1;
        pressure_round.store(1, std::memory_order_release);
        forced_spill_claimant = nullptr;
        forced_spill_request_epoch.store(new_epoch, std::memory_order_release);

        /// An empty runnable set at a stable graph boundary is an explicit no-candidate result,
        /// not a timeout or a count of worker wakeups.
        const bool has_recovery_candidate = std::any_of(
            processor_states.begin(), processor_states.end(),
            [new_epoch](const auto & entry)
            {
                return entry.second.runnable && entry.second.retired_forced_epoch < new_epoch;
            });
        if (!has_recovery_candidate && active_forced_spill_scans == 0)
        {
            forced_spill_claimed_epoch.store(new_epoch, std::memory_order_relaxed);
            forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
            forced_spill_reclaimed_bytes.store(0, std::memory_order_relaxed);
            forced_spill_result_epoch.store(new_epoch, std::memory_order_release);
            forced_spill_completed_epoch.store(new_epoch, std::memory_order_release);
        }
        return {.epoch = new_epoch, .inject_priority = false};
    }

    /// Do not turn repeated observations into priority while the actual reclaim attempt is still
    /// pending. Only its explicit completion can advance the pressure episode.
    if (completed_epoch < requested_epoch)
        return {.epoch = requested_epoch, .inject_priority = false};

    pressure_round.store(2, std::memory_order_release);
    return {.epoch = requested_epoch, .inject_priority = true};
}

MemorySpillScheduler::ForcedSpillResult MemorySpillScheduler::getForcedSpillResult(UInt64 epoch) const
{
    if (epoch == 0 || forced_spill_completed_epoch.load(std::memory_order_acquire) < epoch)
        return {};
    if (forced_spill_result_epoch.load(std::memory_order_acquire) != epoch)
    {
        /// A newer completed epoch may overwrite the single result slot before a lagging worker
        /// observes this one. Completion is monotonic: report a completed sentinel rather than
        /// turning an already-finished episode back into Pending. Current demand reconciliation
        /// observes the actual memory movement independently.
        return {.outcome = ForcedSpillOutcome::NoProgress, .reclaimed_bytes = 0};
    }
    return {
        .outcome = forced_spill_outcome.load(std::memory_order_relaxed),
        .reclaimed_bytes = forced_spill_reclaimed_bytes.load(std::memory_order_relaxed),
    };
}

void MemorySpillScheduler::finishMemoryPressure(bool unregister_requester)
{
    std::lock_guard lock(mutex);
    if (unregister_requester)
    {
        chassert(pressure_requesters > 0);
        if (pressure_requesters > 0)
            --pressure_requesters;
        if (pressure_requesters > 0)
            return;
    }
    else
        pressure_requesters = 0;

    pressure_round.store(0, std::memory_order_release);
    const UInt64 requested = forced_spill_request_epoch.load(std::memory_order_acquire);
    if (forced_spill_completed_epoch.load(std::memory_order_relaxed) < requested)
    {
        forced_spill_claimed_epoch.store(requested, std::memory_order_relaxed);
        forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
        forced_spill_reclaimed_bytes.store(0, std::memory_order_relaxed);
        forced_spill_result_epoch.store(requested, std::memory_order_release);
        forced_spill_completed_epoch.store(requested, std::memory_order_release);
    }
    forced_spill_claimant = nullptr;
}

Int64 MemorySpillScheduler::getHardLimit()
{
    if (hard_limit < 0) [[unlikely]]
    {
        auto most_hard_limit = getCurrentQueryHardLimit();
        if (most_hard_limit)
            hard_limit = *most_hard_limit;
        else
            hard_limit = 0;
    }
    return hard_limit;
}

void MemorySpillScheduler::remove(IProcessor * processor)
{
    // Forced pressure is active even when adaptive spilling is disabled, so every tracked
    // spillable processor must be removed unconditionally.
    if (!processor->isSpillable())
        return;
    std::lock_guard lock(mutex);
    processor_states.erase(processor);
    const UInt64 requested = forced_spill_request_epoch.load(std::memory_order_relaxed);
    if (forced_spill_claimant == processor)
    {
        forced_spill_claimant = nullptr;
        if (forced_spill_completed_epoch.load(std::memory_order_relaxed) < requested)
        {
            forced_spill_claimed_epoch.store(requested - 1, std::memory_order_release);
            /// The claimant disappeared before publishing a result. Re-offer the same epoch to
            /// every still-runnable processor; the enclosing executor scan will either find a new
            /// claimant or close the stable empty census as NoProgress.
            for (auto & [_, state] : processor_states)
            {
                if (state.runnable && state.retired_forced_epoch == requested)
                    state.retired_forced_epoch = requested - 1;
            }
        }
    }
    /// Do not close on an individual transition: this graph update may make a downstream
    /// processor runnable. finishForcedSpillScan() observes the stable post-update set.
    updateTopProcessor();
}

void MemorySpillScheduler::updateTopProcessor()
{
    top_processor = nullptr;
    Int64 max_spillable_memory_bytes = 0;
    max_reserved_memory_bytes = 0;
    for (const auto & [proc, state] : processor_states)
    {
        const auto & stats = state.stats;
        max_reserved_memory_bytes = std::max(stats.need_reserved_memory_bytes, max_reserved_memory_bytes);
        if (!top_processor || stats.spillable_memory_bytes > max_spillable_memory_bytes)
        {
            top_processor = proc;
            max_spillable_memory_bytes = stats.spillable_memory_bytes;
        }
    }
}

IProcessor * MemorySpillScheduler::selectSpilledProcessor(
    IProcessor * current_processor, const ProcessorMemoryStats & mem_stats, bool force_spill)
{
    const auto current_mem_used = force_spill ? 0 : getCurrentQueryMemoryUsage();
    const auto limit = force_spill ? 0 : getHardLimit();
    std::lock_guard lock(mutex);
    auto it = processor_states.find(current_processor);
    chassert(it != processor_states.end());
    if (it == processor_states.end())
        return nullptr;
    it->second.stats = mem_stats;

    // quick check
    max_reserved_memory_bytes = std::max(mem_stats.need_reserved_memory_bytes, max_reserved_memory_bytes);
    if (!force_spill && current_mem_used + max_reserved_memory_bytes < limit)
        return nullptr;

    updateTopProcessor();

    if (!force_spill && current_mem_used + max_reserved_memory_bytes < limit)
        return nullptr;
    return top_processor;
}
}
