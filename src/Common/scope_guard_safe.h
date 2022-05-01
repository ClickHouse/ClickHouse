#pragma once

#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/LockMemoryExceptionInThread.h>

/// Same as SCOPE_EXIT() but block the MEMORY_LIMIT_EXCEEDED errors.
///
/// Typical example of SCOPE_EXIT_MEMORY() usage is when code under it may do
/// some tiny allocations, that may fail under high memory pressure or/and low
/// max_memory_usage (and related limits).
///
/// NOTE: it should be used with caution.
#define SCOPE_EXIT_MEMORY(...) SCOPE_EXIT(                    \
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global); \
    __VA_ARGS__;                                              \
)

/// Same as SCOPE_EXIT() but try/catch/tryLogCurrentException any exceptions.
///
/// SCOPE_EXIT_SAFE() should be used in case the exception during the code
/// under SCOPE_EXIT() is not "that fatal" and error message in log is enough.
///
/// Good example is calling CurrentThread::detachQueryIfNotDetached().
///
/// Anti-pattern is calling WriteBuffer::finalize() under SCOPE_EXIT_SAFE()
/// (since finalize() can do final write and it is better to fail abnormally
/// instead of ignoring write error).
///
/// NOTE: it should be used with double caution.
#define SCOPE_EXIT_SAFE(...) SCOPE_EXIT(             \
    try                                              \
    {                                                \
        __VA_ARGS__;                                 \
    }                                                \
    catch (...)                                      \
    {                                                \
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);    \
    }                                                \
)

/// Same as SCOPE_EXIT() but:
/// - block the MEMORY_LIMIT_EXCEEDED errors,
/// - try/catch/tryLogCurrentException any exceptions.
///
/// SCOPE_EXIT_MEMORY_SAFE() can be used when the error can be ignored, and in
/// addition to SCOPE_EXIT_SAFE() it will also lock MEMORY_LIMIT_EXCEEDED to
/// avoid such exceptions.
///
/// It does exists as a separate helper, since you do not need to lock
/// MEMORY_LIMIT_EXCEEDED always (there are cases when code under SCOPE_EXIT does
/// not do any allocations, while LockExceptionInThread increment atomic
/// variable).
///
/// NOTE: it should be used with triple caution.
#define SCOPE_EXIT_MEMORY_SAFE(...) SCOPE_EXIT(                   \
    try                                                           \
    {                                                             \
        LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global); \
        __VA_ARGS__;                                              \
    }                                                             \
    catch (...)                                                   \
    {                                                             \
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);          \
    }                                                             \
)
