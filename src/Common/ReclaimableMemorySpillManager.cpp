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
    auto it = processor_reclaimable_memory.find(processor);
    if (it == processor_reclaimable_memory.end())
    {
        processor_reclaimable_memory[processor] = reclaimable_mem;
        it = processor_reclaimable_memory.find(processor);
        total_spill_aux_bytes += it->second.spill_aux_bytes;
    }
    else
    {
        total_spill_aux_bytes += (reclaimable_mem.spill_aux_bytes - it->second.spill_aux_bytes);
        if (total_spill_aux_bytes < 0)
        {
            total_spill_aux_bytes = 0;
        }
        it->second = reclaimable_mem;
    }

    if (getCurrentQueryMemoryUsage() + total_spill_aux_bytes < getHardLimit())
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
    Int64 largest_reclaimable_bytes = 0;
    for (const auto & [proc, memory] : processor_reclaimable_memory)
    {
        if (!largest_processor || memory.reclaimable_bytes > largest_reclaimable_bytes)
        {
            largest_processor = proc;
            largest_reclaimable_bytes = memory.reclaimable_bytes;
        }
    }
}
}
