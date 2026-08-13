#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <csignal>

namespace DB
{

/// Signal used by system.stack_trace to ask a thread to record its own stack from a signal handler.
/// Shared here so the query profiler can block it in its handler mask (both share the same
/// thread-local stack-unwinding recovery in StackTrace and must not nest).
#if defined(OS_LINUX)
const int STACK_TRACE_SERVICE_SIGNAL = SIGRTMIN;
#elif defined(OS_DARWIN)
/// macOS has no real-time signals; SIGUSR1/SIGUSR2 are the query profiler's, so use the free
/// virtual-timer signal here.
const int STACK_TRACE_SERVICE_SIGNAL = SIGVTALRM;
#endif

}

#endif
