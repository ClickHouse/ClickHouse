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
    if (!force_spill && (!enable || !getHardLimit()))
        return;

    auto stats = processor->getMemoryStats();
    auto * selected_processor = selectSpilledProcessor(processor, stats, force_spill);

    if (processor == selected_processor)
    {
        if (force_spill)
            forced_spill_completed_epoch.store(forced_epoch, std::memory_order_release);
        processor->spillOnSize(stats.spillable_memory_bytes);
    }
}

bool MemorySpillScheduler::requestForcedSpill()
{
    forced_spill_request_epoch.fetch_add(1, std::memory_order_release);
    /// One reclaim attempt is the recovery lane. Reaching the same pressure point again is the
    /// event-driven suction signal: stop admitting competing growth and resolve this query.
    return pressure_round.fetch_add(1, std::memory_order_acq_rel) + 1 >= 2;
}

void MemorySpillScheduler::finishMemoryPressure()
{
    pressure_round.store(0, std::memory_order_release);
    const UInt64 requested = forced_spill_request_epoch.load(std::memory_order_acquire);
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
    processor_stats.erase(processor);
    updateTopProcessor();
}

void MemorySpillScheduler::updateTopProcessor()
{
    top_processor = nullptr;
    Int64 max_spillable_memory_bytes = 0;
    max_reserved_memory_bytes = 0;
    for (const auto & [proc, stats] : processor_stats)
    {
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
    processor_stats[current_processor] = mem_stats;

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
