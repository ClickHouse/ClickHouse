#pragma once
#include <unordered_map>
#include <base/types.h>
#include <shared_mutex>

namespace DB 
{
class IProcessor;
struct ProcessorReclaimableMemory
{
    // memory that can be spilled into external storage.
    // ReclaimableMemorySpillManager uses this to decide which processor to spill, the largest one
    // is selected in general.
    Int64 reclaimable_bytes = 0;
    // to spill the data, how much additional memory is needed. The available memory should be at
    // least this much. If current memory usage of this thread group + sum(spill_aux_bytes) > memory_limit,
    // we need to spill some data.
    Int64 spill_aux_bytes = 0;
};

// ReclaimableMemorySpillManager is bound to one thread group. It's a query-scoped manager to trigger processor spill.
class RelaimableMemorySpillManager
{
public:
    RelaimableMemorySpillManager() = default;
    ~RelaimableMemorySpillManager() = default;

    Int64 needSpill(IProcessor * processor);
    void processorFinished(IProcessor * processor);

private:
    std::shared_mutex mutex;
    std::unordered_map<IProcessor *, ProcessorReclaimableMemory> processor_reclaimable_memory;
    IProcessor * largest_processor = nullptr;
    Int64 total_spill_aux_bytes = 0;
    std::atomic<Int64> hard_limit = -1;

    void refreshLargestProcessor();

    Int64 getHardLimit();
};
}
