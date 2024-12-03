#pragma once
#include <unordered_map>
#include <base/types.h>
#include <mutex>

namespace DB 
{
class IProcessor;
struct SpillMemoryStats
{
    // memory that can be spilled into external storage.
    // scheduler uses this to decide which processor to spill.
    Int64 spillable_bytes = 0;
    // to spill the data, how much additional memory is needed. The available memory should be at
    // least this much. If current memory usage of this thread group + max_spill_aux_bytes > memory_limit,
    // we need to spill some data.
    Int64 spill_aux_bytes = 0;
};

// ReclaimableMemorySpillManager is bound to one thread group. It's a query-scoped manager to trigger processor spill.
class MemorySpillScheduler
{
public:
    MemorySpillScheduler() = default;
    ~MemorySpillScheduler() = default;

    Int64 needSpill(IProcessor * processor);
    void processorFinished(IProcessor * processor);

private:
    std::mutex mutex;
    std::unordered_map<IProcessor *, SpillMemoryStats> processor_stats;
    IProcessor * top_processor = nullptr;
    Int64 max_spill_aux_bytes = 0;
    std::atomic<Int64> hard_limit = -1;

    void refreshTopProcessor();

    Int64 getHardLimit();
};
}
