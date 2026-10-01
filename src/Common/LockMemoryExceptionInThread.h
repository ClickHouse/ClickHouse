#pragma once

#include <cstdint>
#include <Common/VariableContext.h>

/// To be able to avoid MEMORY_LIMIT_EXCEEDED Exception in destructors:
/// - either configured memory limit reached
/// - or fault injected
///
/// So this will simply ignore the configured memory limit (and avoid fault injection).
///
/// NOTE: exception will be silently ignored, no message in log
/// (since logging from MemoryTracker::alloc() is tricky)
///
/// NOTE: MEMORY_LIMIT_EXCEEDED Exception implicitly blocked if
/// stack unwinding is currently in progress in this thread (to avoid
/// std::terminate()), so you don't need to use it in this case explicitly.
struct LockMemoryExceptionInThread
{
private:
    static thread_local uint64_t counter;
    static thread_local VariableContext level;
    static thread_local bool block_fault_injections;

    VariableContext previous_level;
    bool previous_block_fault_injections;
public:
    /// level_ - block in level and above
    /// block_fault_injections_ - block in fault injection too
    explicit LockMemoryExceptionInThread(VariableContext level_ = VariableContext::User, bool block_fault_injections_ = true);
    ~LockMemoryExceptionInThread();

    LockMemoryExceptionInThread(const LockMemoryExceptionInThread &) = delete;
    LockMemoryExceptionInThread & operator=(const LockMemoryExceptionInThread &) = delete;

    static void addUniqueLock(VariableContext level_ = VariableContext::User, bool block_fault_injections_ = true);
    static void removeUniqueLock();

    static bool isBlocked(VariableContext current_level, bool fault_injection)
    {
        return counter > 0 && current_level >= level && (!fault_injection || block_fault_injections);
    }
};
