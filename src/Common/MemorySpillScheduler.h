#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>

namespace DB
{
class IProcessor;
struct ProcessorMemoryStats
{
    // The total spillable memory in the processor
    Int64 spillable_memory_bytes = 0;
    // To avoid this processor cause OOM, at least `reserved_memory_bytes` should be reserved.
    // including auxiliary memory to finish the spilling process.
    Int64 need_reserved_memory_bytes = 0;
};

// MemorySpillScheduler is bound to one thread group. It's a query-scoped manager to trigger processor spill.
class MemorySpillScheduler
{
public:
    using Ptr = std::shared_ptr<MemorySpillScheduler>;

    explicit MemorySpillScheduler(bool enable_ = false) : enable(enable_) {}
    ~MemorySpillScheduler() = default;

    void checkAndSpill(IProcessor * processor);
    void remove(IProcessor * processor);

    /// Inject query-level memory pressure from the reservation scheduler. The first pressure
    /// round requests a forced spill. A second round without an intervening approval asks the
    /// allocation scheduler to protect this query's growth and resolve it through normal eviction.
    bool requestForcedSpill();
    void finishMemoryPressure();

private:
    bool enable = true;
    std::mutex mutex;
    // Only trace the spillable processors, this map is not expected to be too large.
    std::unordered_map<IProcessor *, ProcessorMemoryStats> processor_stats;
    IProcessor * top_processor = nullptr;
    Int64 max_reserved_memory_bytes = 0;
    std::atomic<Int64> hard_limit = -1;

    /// Monotonic epochs avoid losing a spill request when a different processor observes it first.
    std::atomic<UInt64> forced_spill_request_epoch = 0;
    std::atomic<UInt64> forced_spill_completed_epoch = 0;
    std::atomic<UInt64> pressure_round = 0;

    // When there is no need to spill, return nullptr. otherwise return top_processor;
    IProcessor * selectSpilledProcessor(
        IProcessor * current_processor, const ProcessorMemoryStats & mem_stats, bool force_spill);

    void updateTopProcessor();

    Int64 getHardLimit();
};
}
