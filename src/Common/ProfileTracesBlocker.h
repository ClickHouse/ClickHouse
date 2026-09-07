#pragma once

#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ThreadStatus.h>

#include <atomic>

namespace DB
{

/// Queueing and serializing samples must not add the delivery path to the trace stream.
/// The signal-safe flag suppresses streaming without disabling `CPU` and `Real` samples in `system.trace_log`.
/// Accounting and sampling are independent: blocking the memory tracker alone does not stop
/// the per-allocation sampling cache in `ThreadStatus`.
/// Use only in scopes that cannot yield: legacy network coroutines share their caller's thread state.
class ProfileTracesBlocker
{
public:
    ProfileTracesBlocker()
        : was_blocked(blocked.exchange(true, std::memory_order_relaxed))
        , thread(CurrentThread::isInitialized() ? &CurrentThread::get() : nullptr)
    {
        if (thread)
        {
            sample_config = thread->getMemorySampleConfig();
            auto disabled = sample_config;
            disabled.probability = 0;
            thread->setMemorySampleConfig(disabled);
        }
    }

    ~ProfileTracesBlocker()
    {
        if (thread)
            thread->setMemorySampleConfig(sample_config);
        blocked.store(was_blocked, std::memory_order_relaxed);
    }

    static bool isBlocked()
    {
        return blocked.load(std::memory_order_relaxed);
    }

    ProfileTracesBlocker(const ProfileTracesBlocker &) = delete;
    ProfileTracesBlocker & operator=(const ProfileTracesBlocker &) = delete;

private:
    static inline thread_local constinit std::atomic<bool> blocked{false};
    static_assert(std::atomic<bool>::is_always_lock_free);

    bool was_blocked;
    MemoryTrackerBlockerInThread accounting_blocker{VariableContext::Global};
    ThreadStatus * thread;
    MemoryTracker::SampleConfig sample_config{};
};

}
