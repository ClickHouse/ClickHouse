#include <mutex>
#include <Common/MemoryTrackerUtils.h>
#include <Common/MemorySpillScheduler.h>
#include <Processors/ISpillable.h>


namespace DB
{
size_t MemorySpillScheduler::checkAndSpill(ISpillable * processor)
{
    if (!enable || !getHardLimit())
        return 0;

    auto stats = processor->getMemoryStats();
    auto * selected_processor = selectSpilledProcessor(processor, stats);

    if (processor == selected_processor)
        return processor->spill(/*at_least_bytes=*/ 1);

    return 0;
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

void MemorySpillScheduler::remove(ISpillable * processor)
{
    if (!enable)
        return;
    std::lock_guard lock(mutex);
    processor_stats.erase(processor);
    updateTopProcessor();
}

void MemorySpillScheduler::updateTopProcessor()
{
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

ISpillable * MemorySpillScheduler::selectSpilledProcessor(ISpillable * current_processor, const ProcessorMemoryStats & mem_stats)
{
    auto current_mem_used = getCurrentQueryMemoryUsage();
    auto limit = getHardLimit();
    std::lock_guard lock(mutex);
    processor_stats[current_processor] = mem_stats;

    // quick check
    max_reserved_memory_bytes = std::max(mem_stats.need_reserved_memory_bytes, max_reserved_memory_bytes);
    if (current_mem_used + max_reserved_memory_bytes < limit)
        return nullptr;

    updateTopProcessor();

    if (current_mem_used + max_reserved_memory_bytes < limit)
        return nullptr;
    return top_processor;
}
}
