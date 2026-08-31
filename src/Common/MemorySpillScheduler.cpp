#include <algorithm>
#include <mutex>
#include <Common/MemoryTrackerUtils.h>
#include <Common/MemorySpillScheduler.h>
#include <Processors/IProcessor.h>


namespace DB
{
void MemorySpillScheduler::checkAndSpill(IProcessor * processor)
{
    chassert(processor->isSpillable());

    const UInt64 forced_epoch = forced_spill_request_epoch.load(std::memory_order_acquire);
    const bool force_spill = forced_spill_completed_epoch.load(std::memory_order_acquire) < forced_epoch;
    const auto stats = processor->getMemoryStats();

    if (force_spill)
    {
        bool should_spill = false;
        {
            std::lock_guard lock(mutex);
            if (!forced_spill_active
                || forced_spill_request_epoch.load(std::memory_order_acquire) != forced_epoch)
                return;

            auto & state = processor_states[processor];
            state.stats = stats;
            if (state.claimed_forced_epoch >= forced_epoch)
                return;

            state.claimed_forced_epoch = forced_epoch;
            should_spill = stats.spillable_memory_bytes > 0;
            if (!should_spill)
                state.completed_forced_epoch = forced_epoch;

            if (!should_spill)
            {
                const bool all_completed = !processor_states.empty() && std::all_of(
                    processor_states.begin(), processor_states.end(), [forced_epoch](const auto & item)
                    {
                        return item.second.completed_forced_epoch >= forced_epoch;
                    });
                if (all_completed)
                {
                    if (forced_spill_outcome.load(std::memory_order_relaxed) == ForcedSpillOutcome::Pending)
                        forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
                    forced_spill_completed_epoch.store(forced_epoch, std::memory_order_release);
                }
            }
        }

        if (!should_spill)
            return;

        const Int64 memory_before = getCurrentQueryMemoryUsage();
        const bool spill_succeeded = processor->spillOnSize(stats.spillable_memory_bytes);
        const Int64 memory_after = getCurrentQueryMemoryUsage();
        const Int64 reclaimed_bytes = std::max<Int64>(memory_before - memory_after, 0);

        std::lock_guard lock(mutex);
        if (!forced_spill_active
            || forced_spill_request_epoch.load(std::memory_order_acquire) != forced_epoch)
            return;

        auto state = processor_states.find(processor);
        if (state == processor_states.end())
            return;

        state->second.completed_forced_epoch = forced_epoch;
        if (spill_succeeded || reclaimed_bytes > 0)
        {
            forced_spill_outcome.store(ForcedSpillOutcome::Progress, std::memory_order_relaxed);
            forced_spill_reclaimed_bytes.fetch_add(reclaimed_bytes, std::memory_order_relaxed);
        }

        const bool all_completed = !processor_states.empty() && std::all_of(
            processor_states.begin(), processor_states.end(), [forced_epoch](const auto & item)
            {
                return item.second.completed_forced_epoch >= forced_epoch;
            });
        if (all_completed)
        {
            if (forced_spill_outcome.load(std::memory_order_relaxed) == ForcedSpillOutcome::Pending)
                forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
            forced_spill_completed_epoch.store(forced_epoch, std::memory_order_release);
        }
        return;
    }

    if (!enable || !getHardLimit())
        return;

    if (processor == selectSpilledProcessor(processor, stats, false))
        processor->spillOnSize(stats.spillable_memory_bytes);
}

void MemorySpillScheduler::registerProcessor(IProcessor * processor)
{
    if (!processor->isSpillable())
        return;
    std::lock_guard lock(mutex);
    processor_states.try_emplace(processor);
    updateTopProcessor();
}

MemorySpillScheduler::ForcedSpillRequest MemorySpillScheduler::requestForcedSpill()
{
    std::lock_guard lock(mutex);

    const UInt64 requested_epoch = forced_spill_request_epoch.load(std::memory_order_acquire);
    if (forced_spill_active)
        return {.epoch = requested_epoch};

    const UInt64 new_epoch = requested_epoch + 1;
    forced_spill_active = true;
    forced_spill_outcome.store(ForcedSpillOutcome::Pending, std::memory_order_relaxed);
    forced_spill_reclaimed_bytes.store(0, std::memory_order_relaxed);
    forced_spill_request_epoch.store(new_epoch, std::memory_order_release);

    /// An empty registered set is an explicit no-candidate result.
    if (processor_states.empty())
    {
        forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
        forced_spill_completed_epoch.store(new_epoch, std::memory_order_release);
    }
    return {.epoch = new_epoch};
}

MemorySpillScheduler::ForcedSpillResult MemorySpillScheduler::getForcedSpillResult(UInt64 epoch) const
{
    if (epoch == 0 || forced_spill_completed_epoch.load(std::memory_order_acquire) < epoch)
        return {};
    return {
        .outcome = forced_spill_outcome.load(std::memory_order_relaxed),
        .reclaimed_bytes = forced_spill_reclaimed_bytes.load(std::memory_order_relaxed),
    };
}

void MemorySpillScheduler::finishMemoryPressure()
{
    std::lock_guard lock(mutex);
    forced_spill_active = false;
    const UInt64 requested = forced_spill_request_epoch.load(std::memory_order_acquire);
    forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
    forced_spill_reclaimed_bytes.store(0, std::memory_order_relaxed);
    forced_spill_completed_epoch.store(requested, std::memory_order_release);
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
    const bool forced_spill_is_pending = forced_spill_active
        && forced_spill_completed_epoch.load(std::memory_order_relaxed) < requested;
    const bool all_remaining_completed = std::all_of(
        processor_states.begin(), processor_states.end(), [requested](const auto & item)
        {
            return item.second.completed_forced_epoch >= requested;
        });
    if (forced_spill_is_pending && all_remaining_completed)
    {
        if (forced_spill_outcome.load(std::memory_order_relaxed) == ForcedSpillOutcome::Pending)
            forced_spill_outcome.store(ForcedSpillOutcome::NoProgress, std::memory_order_relaxed);
        forced_spill_completed_epoch.store(requested, std::memory_order_release);
    }
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
    processor_states[current_processor].stats = mem_stats;

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
