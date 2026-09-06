#pragma once

#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

/// Queueing and serializing memory samples must not sample their own allocations.
/// Accounting and sampling are independent: blocking the memory tracker alone does not stop
/// the per-allocation sampling cache in `ThreadStatus`.
/// Use only in scopes that cannot yield: legacy network coroutines share their caller's thread state.
class ProfileTracesBlocker
{
public:
    ProfileTracesBlocker()
        : thread(CurrentThread::isInitialized() ? &CurrentThread::get() : nullptr)
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
    }

    ProfileTracesBlocker(const ProfileTracesBlocker &) = delete;
    ProfileTracesBlocker & operator=(const ProfileTracesBlocker &) = delete;

private:
    MemoryTrackerBlockerInThread accounting_blocker{VariableContext::Global};
    ThreadStatus * thread;
    MemoryTracker::SampleConfig sample_config{};
};

}
