#include <mutex>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/MemorySpillScheduler.h>
#include <Processors/IProcessor.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
// If current memory is over limit and the processor is the largest one that has the largest
// reclaimable memory, let the processor spill.
Int64 MemorySpillScheduler::needSpill(IProcessor * processor)
{

    if (!processor->spillable() || !getHardLimit())
    {
        return 0;
    }
    auto stats = processor->getSpillMemoryStats();
    std::lock_guard lock(mutex);
    processor_stats[processor] = stats;
    refreshTopProcessor();
    if (getCurrentQueryMemoryUsage() + max_spill_aux_bytes < getHardLimit())
        return 0;
    LOG_DEBUG(getLogger("MemorySpillScheduler"),
        "need to spill. current memory usage: {}, hard limit: {}, max_spill_aux_bytes: {}",
        getCurrentQueryMemoryUsage(), getHardLimit(), max_spill_aux_bytes);
    refreshTopProcessor();
    return processor == top_processor ? stats.spillable_bytes : 0;
}

Int64 MemorySpillScheduler::getHardLimit()
{
    if (hard_limit < 0) [[unlikely]]
    {
        auto most_hard_limit = getMostHardLimit();
        if (most_hard_limit)
            hard_limit = *most_hard_limit;
        else
            hard_limit = 0;
    }
    return hard_limit;
}

void MemorySpillScheduler::processorFinished(IProcessor * processor)
{
    // Only the spillable processors are tracked.
    if (!processor->spillable())
        return;
    std::lock_guard lock(mutex);
    processor_stats.erase(processor);
    if (processor == top_processor)
        refreshTopProcessor();
}

void MemorySpillScheduler::refreshTopProcessor()
{
    Int64 max_spillable_bytes = 0;
    max_spill_aux_bytes = 0;
    for (const auto & [proc, stats] : processor_stats)
    {
        if (stats.spill_aux_bytes > max_spill_aux_bytes)
        {
            max_spill_aux_bytes = stats.spill_aux_bytes;
        }
        if (!top_processor || stats.spillable_bytes > max_spillable_bytes)
        {
            top_processor = proc;
            max_spillable_bytes = stats.spillable_bytes;
        }
    }
}
}
