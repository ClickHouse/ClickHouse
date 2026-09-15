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
    enum class ForcedSpillOutcome : UInt8
    {
        Pending,
        Progress,
        NoProgress,
    };

    struct ForcedSpillRequest
    {
        UInt64 epoch = 0;
    };

    struct ForcedSpillResult
    {
        ForcedSpillOutcome outcome = ForcedSpillOutcome::Pending;
        Int64 reclaimed_bytes = 0;
    };

    explicit MemorySpillScheduler(bool enable_ = false) : enable(enable_) {}
    ~MemorySpillScheduler() = default;

    void checkAndSpill(IProcessor * processor);
    /// Called by the same worker after successful processor work. A spill callback may only arm
    /// deferred work, so neither the callback nor work completes the attempt while a spill is pending.
    void finishSpill(IProcessor * processor);
    void registerProcessor(IProcessor * processor);
    void registerProcessor(const std::shared_ptr<IProcessor> & processor);
    void remove(IProcessor * processor);

    /// Start one exhaustive forced-spill pass when a query enters the eviction queue.
    /// Repeated calls while that pass is active return the same epoch.
    ForcedSpillRequest requestForcedSpill();
    ForcedSpillResult getForcedSpillResult(UInt64 epoch) const;
    /// Query-thread recovery work, independent of processor readiness. No scheduler or graph lock
    /// is held while a processor spills. Ordinary pipeline work is not needed to finish this pass.
    void executeForcedSpill(UInt64 epoch);
    void finishMemoryPressure();

private:
    struct ProcessorState
    {
        ProcessorMemoryStats stats;
        UInt64 claimed_forced_epoch = 0;
        UInt64 completed_forced_epoch = 0;
        Int64 memory_before_spill = 0;
        bool spill_requested = false;
        bool dedicated_spill_in_progress = false;
        bool lifetime_tracked = false;
        std::weak_ptr<IProcessor> lifetime;
    };

    bool enable = true;
    std::mutex mutex;
    std::mutex forced_spill_execution_mutex;
    // Only trace the spillable processors, this map is not expected to be too large.
    std::unordered_map<IProcessor *, ProcessorState> processor_states;
    IProcessor * top_processor = nullptr;
    Int64 max_reserved_memory_bytes = 0;
    std::atomic<Int64> hard_limit = -1;

    /// Monotonic epochs make completion observable without coupling the reservation to a processor.
    std::atomic<UInt64> forced_spill_request_epoch = 0;
    std::atomic<UInt64> forced_spill_completed_epoch = 0;
    std::atomic<ForcedSpillOutcome> forced_spill_outcome = ForcedSpillOutcome::Pending;
    std::atomic<Int64> forced_spill_reclaimed_bytes = 0;
    bool forced_spill_active = false;
    size_t forced_spill_remaining = 0; /// Protected by `mutex`.

    // When there is no need to spill, return nullptr. otherwise return top_processor;
    IProcessor * selectSpilledProcessor(
        IProcessor * current_processor, const ProcessorMemoryStats & mem_stats, bool force_spill);

    void updateTopProcessor();
    void registerProcessorImpl(IProcessor * processor, std::weak_ptr<IProcessor> lifetime, bool lifetime_tracked);
    void completeForcedSpillProcessor(UInt64 epoch, ProcessorState & state);

    Int64 getHardLimit();
};

using MemorySpillSchedulerPtr = std::shared_ptr<MemorySpillScheduler>;

}
