#include <mutex>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/MemorySpillScheduler.h>
#include <Processors/IProcessor.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
void MemorySpillScheduler::checkAndSpill(IProcessor * processor)
{
    chassert(processor->isSpillable());
    if (!enable || !getHardLimit())
        return;

    auto stats = processor->getMemoryStats();
    auto * selected_processor = selectSpilledProcessor(processor, stats);

    if (processor == selected_processor)
    {
        processor->spillOnSize(stats.spillable_memory_bytes);
    }
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
    // Only the spillable processors are tracked.
    if (!enable || !processor->isSpillable())
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

IProcessor * MemorySpillScheduler::selectSpilledProcessor(IProcessor * current_processor, const ProcessorMemoryStats & mem_stats)
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
