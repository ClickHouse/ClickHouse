#pragma once

#include <base/defines.h> /// MEMORY_SANITIZER, via base/sanitizer_defs.h

/// Both macros below are consumed by `#if`, so they cannot be enums.
/// NOLINTBEGIN(modernize-macro-to-enum)

/// Whether fibers may be used in this build.
///
/// On AArch64, MemorySanitizer reaches its shadow TLS through the thread pointer (TPIDR_EL0),
/// which the compiler may read once and keep in a callee-saved register across a call. A fiber
/// stack switch inside that call can resume the frame on another OS thread, leaving the cached
/// pointer naming the old thread, and the next shadow access faults.
/// Always defined (0 or 1) so it can be used in #if without tripping -Wundef.
#if defined(__aarch64__) && defined(MEMORY_SANITIZER)
#    define CH_FIBERS_SUPPORTED 0
#else
#    define CH_FIBERS_SUPPORTED 1
#endif

/// Whether the asynchronous (fiber-backed) remote read/send path is compiled in. This is the
/// single definition of that condition: `RemoteQueryExecutor`'s guards are written in terms of it,
/// so no copy of the condition can drift from it.
///
/// Note that `RemoteSource` and the epoll/eventfd/timerfd helpers keep their own
/// `OS_LINUX || OS_DARWIN` guards - those select platform primitives that exist regardless of
/// fibers, and are a different condition from this one.
#if (defined(OS_LINUX) || defined(OS_DARWIN)) && CH_FIBERS_SUPPORTED
#    define CH_REMOTE_ASYNC_IO 1
#else
#    define CH_REMOTE_ASYNC_IO 0
#endif

/// NOLINTEND(modernize-macro-to-enum)
