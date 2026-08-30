#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <Common/ProcessorMemoryStats.h>

namespace DB
{
class IProcessor;

// MemorySpillScheduler is bound to one thread group. It's a query-scoped manager to trigger processor spill.
class MemorySpillScheduler
{
public:
    explicit MemorySpillScheduler(bool enable_ = false) : enable(enable_) {}
    ~MemorySpillScheduler() = default;

    void checkAndSpill(IProcessor * processor);
    void remove(IProcessor * processor);

private:
    bool enable = true;
    std::mutex mutex;
    // Only trace the spillable processors, this map is not expected to be too large.
    std::unordered_map<IProcessor *, ProcessorMemoryStats> processor_stats;
    IProcessor * top_processor = nullptr;
    Int64 max_reserved_memory_bytes = 0;
    std::atomic<Int64> hard_limit = -1;

    // When there is no need to spill, return nullptr. otherwise return top_processor;
    IProcessor * selectSpilledProcessor(IProcessor * current_processor, const ProcessorMemoryStats & mem_stats);

    void updateTopProcessor();

    Int64 getHardLimit();
};

using MemorySpillSchedulerPtr = std::shared_ptr<MemorySpillScheduler>;

}
