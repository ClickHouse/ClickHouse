#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <Common/ProcessorMemoryStats.h>

namespace DB
{
class IProcessor;

enum class MemoryRecoverySpillOutcome : UInt8
{
    Pending,
    Progress,
    NoProgress,
};

/// One query-level memory-pressure recovery attempt. The reservation owns the episode; the spill
/// scheduler only keeps a weak reference to the currently active one. Results and exceptions stay
/// attached to this object, so a later recovery attempt can never overwrite an earlier result.
struct MemoryRecoveryEpisode
{
    UInt64 id = 0;
    std::atomic<MemoryRecoverySpillOutcome> outcome = MemoryRecoverySpillOutcome::Pending;
    std::atomic<Int64> reclaimed_bytes = 0;
    std::atomic_bool completed = false;
    std::atomic_bool closed = false;
    std::atomic_bool execution_running = false;
    std::atomic_bool async_running = false;

    mutable std::mutex mutex;
    std::condition_variable cv;
    std::exception_ptr exception;
};

using MemoryRecoveryEpisodePtr = std::shared_ptr<MemoryRecoveryEpisode>;

// MemorySpillScheduler is bound to one thread group. It's a query-scoped manager to trigger processor spill.
class MemorySpillScheduler : public std::enable_shared_from_this<MemorySpillScheduler>
{
public:
    using ForcedSpillOutcome = MemoryRecoverySpillOutcome;

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

    /// Start one exhaustive forced-spill pass when a query enters recovery. Repeated calls for the
    /// same active recovery return the same episode.
    MemoryRecoveryEpisodePtr requestForcedSpill();
    ForcedSpillResult getForcedSpillResult(const MemoryRecoveryEpisodePtr & episode) const;
    /// Query-thread recovery work, independent of processor readiness. No scheduler or graph lock
    /// is held while a processor spills. Ordinary pipeline work is not needed to finish this pass.
    void executeForcedSpill(const MemoryRecoveryEpisodePtr & episode);
    /// Execute the same pass on a query-attached global-pool thread and wait only until `deadline`.
    /// Work which has not started can be abandoned when recovery closes; an in-flight callback is
    /// allowed to finish and keeps any exception on its originating episode.
    void executeForcedSpillUntil(
        const MemoryRecoveryEpisodePtr & episode,
        std::chrono::steady_clock::time_point deadline);
    void finishMemoryPressure(const MemoryRecoveryEpisodePtr & episode);
    void rethrowIfFailed(const MemoryRecoveryEpisodePtr & episode) const;

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

    UInt64 next_recovery_id = 0; /// Protected by `mutex`.
    std::weak_ptr<MemoryRecoveryEpisode> active_recovery; /// Protected by `mutex`.
    size_t forced_spill_remaining = 0; /// Protected by `mutex`.

    // When there is no need to spill, return nullptr. otherwise return top_processor;
    IProcessor * selectSpilledProcessor(
        IProcessor * current_processor, const ProcessorMemoryStats & mem_stats, bool force_spill);

    void updateTopProcessor();
    void registerProcessorImpl(IProcessor * processor, std::weak_ptr<IProcessor> lifetime, bool lifetime_tracked);
    void completeForcedSpillProcessor(const MemoryRecoveryEpisodePtr & episode, ProcessorState & state);

    Int64 getHardLimit();
};

using MemorySpillSchedulerPtr = std::shared_ptr<MemorySpillScheduler>;

}
