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

// MemorySpillScheduler is bound to one dependency graph. It is shared by nested executors through
// their canonical MemoryReservation and coordinates query-scoped processor spill.
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
        bool inject_priority = false;
    };

    struct ForcedSpillResult
    {
        ForcedSpillOutcome outcome = ForcedSpillOutcome::Pending;
        Int64 reclaimed_bytes = 0;
    };

    explicit MemorySpillScheduler(bool enable_ = false) : enable(enable_) {}
    ~MemorySpillScheduler() = default;

    /// Returns true when this task was consumed by the forced-recovery lane. The executor must
    /// then skip ordinary processor work, which may allocate, and preserve the already-prepared
    /// task until the pressure episode is resolved.
    bool checkAndSpill(IProcessor * processor);
    void registerProcessor(IProcessor * processor);
    /// Executor lifecycle, separate from graph-lifetime registration. Only processors which can
    /// actually enter work participate in a forced-spill epoch.
    void setProcessorRunnable(IProcessor * processor, bool runnable);
    void beginForcedSpillScan();
    /// Called after one complete graph update. An empty unclaimed runnable set at this boundary is
    /// explicit proof that the epoch has no candidate; it is not inferred from time or sync calls.
    void finishForcedSpillScan();
    void remove(IProcessor * processor);

    /// Inject query-level memory pressure from the reservation scheduler. The first pressure
    /// round requests a forced spill. Only explicit completion of that attempt allows the second
    /// round to inject suction priority and resolve the conflict through normal victim selection.
    ForcedSpillRequest requestForcedSpill(bool register_requester = false);
    ForcedSpillResult getForcedSpillResult(UInt64 epoch) const;
    void finishMemoryPressure(bool unregister_requester = false);

private:
    struct ProcessorState
    {
        ProcessorMemoryStats stats;
        UInt64 retired_forced_epoch = 0;
        bool runnable = false;
    };

    bool enable = true;
    std::mutex mutex;
    // Only trace the spillable processors, this map is not expected to be too large.
    std::unordered_map<IProcessor *, ProcessorState> processor_states;
    /// Executor task boundaries are counted even outside pressure so a concurrently opening epoch
    /// cannot mistake an in-flight graph transition for a stable empty candidate set.
    size_t active_forced_spill_scans = 0; // protected by mutex
    IProcessor * top_processor = nullptr;
    IProcessor * forced_spill_claimant = nullptr;
    Int64 max_reserved_memory_bytes = 0;
    std::atomic<Int64> hard_limit = -1;

    /// Monotonic epochs avoid losing a spill request when a different processor observes it first.
    std::atomic<UInt64> forced_spill_request_epoch = 0;
    std::atomic<UInt64> forced_spill_claimed_epoch = 0;
    std::atomic<UInt64> forced_spill_completed_epoch = 0;
    /// Epoch tagged result. Opening a later Pending epoch must not overwrite the completion still
    /// being consumed by a lagging reservation in the same dependency graph.
    std::atomic<UInt64> forced_spill_result_epoch = 0;
    std::atomic<ForcedSpillOutcome> forced_spill_outcome = ForcedSpillOutcome::Pending;
    std::atomic<Int64> forced_spill_reclaimed_bytes = 0;
    std::atomic<UInt64> pressure_round = 0;
    /// Multiple reservations can be nodes of one query dependency graph. They share the forced
    /// spill epoch, and one node resolving must not close it while another is still parked.
    size_t pressure_requesters = 0;

    // When there is no need to spill, return nullptr. otherwise return top_processor;
    IProcessor * selectSpilledProcessor(
        IProcessor * current_processor, const ProcessorMemoryStats & mem_stats, bool force_spill);

    void updateTopProcessor();

    Int64 getHardLimit();
};

using MemorySpillSchedulerPtr = std::shared_ptr<MemorySpillScheduler>;

}
