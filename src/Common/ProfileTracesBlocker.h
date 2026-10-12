#pragma once

#include <Common/CurrentThread.h>
#include <Common/FiberLocal.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

/// Suppress delivery to the live stream without changing memory accounting or `system.trace_log`.
/// The flag belongs to the execution context, including stackful coroutines and migrating silk fibers.
/// Unlike the stronger synchronous guard below, this scope may span a suspended network read.
class ProfileTracesStreamBlocker
{
public:
    ProfileTracesStreamBlocker()
        : was_blocked(FiberLocalStorage::exchangeAtomic<FiberLocalSlot::PROFILE_TRACES_BLOCKED>(true))
    {
    }

    ~ProfileTracesStreamBlocker()
    {
        FiberLocalStorage::exchangeAtomic<FiberLocalSlot::PROFILE_TRACES_BLOCKED>(was_blocked);
    }

    static bool isBlocked()
    {
        return FiberLocalStorage::loadAtomic<FiberLocalSlot::PROFILE_TRACES_BLOCKED>();
    }

    ProfileTracesStreamBlocker(const ProfileTracesStreamBlocker &) = delete;
    ProfileTracesStreamBlocker & operator=(const ProfileTracesStreamBlocker &) = delete;

private:
    bool was_blocked;
};

/// Queueing and serializing samples must not add the delivery path to the trace stream.
/// The signal-safe flag suppresses streaming without disabling `CPU` and `Real` samples in `system.trace_log`.
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

    static bool isBlocked()
    {
        return ProfileTracesStreamBlocker::isBlocked();
    }

    ProfileTracesBlocker(const ProfileTracesBlocker &) = delete;
    ProfileTracesBlocker & operator=(const ProfileTracesBlocker &) = delete;

private:
    ProfileTracesStreamBlocker stream_blocker;
    MemoryTrackerBlockerInThread accounting_blocker{VariableContext::Global};
    ThreadStatus * thread;
    MemoryTracker::SampleConfig sample_config{};
};

}
