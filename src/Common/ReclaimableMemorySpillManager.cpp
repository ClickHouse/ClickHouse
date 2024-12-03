#include <mutex>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ReclaimableMemorySpillManager.h>
#include <Processors/IProcessor.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
// If current memory is over limit and the processor is the largest one that has the largest
// reclaimable memory, let the processor spill.
Int64 RelaimableMemorySpillManager::needSpill(IProcessor * processor)
{

    if (!processor->spillable() || !getHardLimit())
    {
        return 0;
    }
    auto reclaimable_mem = processor->getReclaimableMemoryUsage();
    std::unique_lock lock(mutex);
    processor_reclaimable_memory[processor] = reclaimable_mem;
    refreshLargestProcessor();
    if (getCurrentQueryMemoryUsage() + largest_spill_aux_bytes < getHardLimit())
        return 0;
    refreshLargestProcessor();
    return processor == largest_processor ? reclaimable_mem.reclaimable_bytes : 0;
}

Int64 RelaimableMemorySpillManager::getHardLimit()
{
    if (hard_limit < 0)
    {
        auto most_hard_limit = getMostHardLimit();
        if (most_hard_limit)
            hard_limit = *most_hard_limit;
        else
            hard_limit = 0;
    }
    return hard_limit;
}

void RelaimableMemorySpillManager::processorFinished(IProcessor * processor)
{
    // Only the spillable processors are tracked.
    if (!processor->spillable())
        return;
    std::unique_lock lock(mutex);
    processor_reclaimable_memory.erase(processor);
    if (processor == largest_processor)
        refreshLargestProcessor();
}

void RelaimableMemorySpillManager::refreshLargestProcessor()
{
    largest_spill_aux_bytes = 0;
    for (const auto & [proc, memory] : processor_reclaimable_memory)
    {
        if (!largest_processor || memory.spill_aux_bytes > largest_spill_aux_bytes)
        {
            largest_processor = proc;
            largest_spill_aux_bytes = memory.reclaimable_bytes;
        }
    }
}
}
