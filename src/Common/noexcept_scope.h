#pragma once
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>


#define NOEXCEPT_SCOPE_IMPL_CONCAT(n, expected) \
    LockMemoryExceptionInThread lock_memory_tracker##n(VariableContext::Global);   \
    SCOPE_EXIT(                                                                    \
        {                                                                          \
            const auto uncaught = std::uncaught_exceptions();                      \
            assert((expected) == uncaught || (expected) + 1 == uncaught);          \
            if ((expected) < uncaught)                                             \
            {                                                                      \
                tryLogCurrentException("NOEXCEPT_SCOPE");                          \
                abort();                                                           \
            }                                                                      \
        }                                                                          \
    )

#define NOEXCEPT_SCOPE_IMPL(n, expected) NOEXCEPT_SCOPE_IMPL_CONCAT(n, expected)

#define NOEXCEPT_SCOPE_CONCAT(n)                                                   \
    const auto num_curr_exceptions##n = std::uncaught_exceptions();                \
    NOEXCEPT_SCOPE_IMPL(n, num_curr_exceptions##n)

#define NOEXCEPT_SCOPE_FWD(n) NOEXCEPT_SCOPE_CONCAT(n)


/// It can be used in critical places to exit on unexpected exceptions.
/// SIGABRT is usually better that broken in-memory state with unpredictable consequences.
/// It also temporarily disables exception from memory tracker in current thread.
/// Strict version does not take into account nested exception (i.e. it aborts even when we're in catch block).

#define NOEXCEPT_SCOPE_STRICT NOEXCEPT_SCOPE_IMPL(__LINE__, 0)
#define NOEXCEPT_SCOPE NOEXCEPT_SCOPE_FWD(__LINE__)
