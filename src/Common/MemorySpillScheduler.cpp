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

    if (!enable || !processor->spillable() || !getHardLimit())
    {
        return;
    }
    auto stats = processor->getMemoryStats();
    std::lock_guard lock(mutex);
    processor_stats[processor] = stats;
    updateTopProcessor();
    if (getCurrentQueryMemoryUsage() + max_reserved_memory_bytes < getHardLimit())
        return;
    LOG_DEBUG(getLogger("MemorySpillScheduler"),
        "need to spill. current memory usage: {}, hard limit: {}, max_reserved_memory_bytes: {}",
        getCurrentQueryMemoryUsage(), getHardLimit(), max_reserved_memory_bytes);
    if (processor == top_processor)
        processor->spillOnSize(stats.spillable_memory_bytes);
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
    if (!enable || !processor->spillable())
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
        if (stats.need_reserved_memory_bytes > max_reserved_memory_bytes)
        {
            max_reserved_memory_bytes = stats.need_reserved_memory_bytes;
        }
        if (!top_processor || stats.spillable_memory_bytes > max_spillable_memory_bytes)
        {
            top_processor = proc;
            max_spillable_memory_bytes = stats.spillable_memory_bytes;
        }
    }
}
}
