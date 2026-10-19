#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <csignal>

namespace DB
{

/// Signal used by system.stack_trace to ask a thread to record its own stack from a signal handler.
/// Shared here so the query profiler can block it in its handler mask (both share the same
/// thread-local stack-unwinding recovery in StackTrace and must not nest).
/// A macro rather than a `const int`: `SIGRTMIN` expands to a libc call, so a variable initialized from it
/// gets a static initializer in every translation unit that includes this header.
#if defined(OS_LINUX)
#    define STACK_TRACE_SERVICE_SIGNAL (SIGRTMIN)
#elif defined(OS_DARWIN)
/// macOS has no real-time signals; SIGUSR1/SIGUSR2 are the query profiler's, so use the free
/// virtual-timer signal here.
#    define STACK_TRACE_SERVICE_SIGNAL (SIGVTALRM)
#endif

}

#endif
