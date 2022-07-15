#pragma once
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>

/// It can be used in critical places to exit on unexpected exceptions.
/// SIGABRT is usually better that broken in-memory state with unpredictable consequences.
/// It also temporarily disables exception from memory tracker in current thread.
/// Strict version does not take into account nested exception (i.e. it aborts even when we're in catch block).

#define NOEXCEPT_SCOPE_IMPL(...) do {                          \
    LockMemoryExceptionInThread                                \
        noexcept_lock_memory_tracker(VariableContext::Global); \
    try                                                        \
    {                                                          \
        __VA_ARGS__;                                           \
    }                                                          \
    catch (...)                                                \
    {                                                          \
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);       \
        std::terminate();                                      \
    }                                                          \
} while (0) /* to allow leading semi-colon */

#define NOEXCEPT_SCOPE_STRICT(...)                    \
    if (std::uncaught_exceptions()) std::terminate(); \
    NOEXCEPT_SCOPE_IMPL(__VA_ARGS__)

#define NOEXCEPT_SCOPE(...) NOEXCEPT_SCOPE_IMPL(__VA_ARGS__)
