#pragma once

#include <optional>
#include <base/types.h>
#include <base/sanitizer_defs.h> /// THREAD_SANITIZER, used by QUERY_PROFILER_SUPPORTED below
#include <signal.h>
#include <time.h>

#include <Common/Logger.h>


namespace Poco
{
    class Logger;
}

/// Whether the sampling query profiler can run in this build.
///
/// It is disabled under TSan on macOS: the profiler pauses threads with signals, and a signal
/// delivered to a thread waiting on a `pthread_rwlock` makes Darwin's implementation lose the
/// wakeup and deadlock the process (Apple FB24027930). Other Darwin builds link the replacement
/// in `base/darwin-compatibility`, but TSan builds cannot, because TSan interposes those same
/// functions to track lock order.
///
/// It is disabled under MSan because it destroys every MSan report. Printing a report takes the
/// sanitizer runtime seconds (it symbolizes each frame through an `llvm-symbolizer` subprocess),
/// and it holds `ScopedErrorReportLock` for the whole time. A profiler signal delivered to that
/// thread meanwhile runs instrumented code in the handler, which trips a second MSan check; the
/// runtime treats a same-thread re-entry into the report lock as unrecoverable, writes
/// `MemorySanitizer: nested bug in the same thread, aborting.` and calls `internal__exit`. The
/// report is then truncated after its `WARNING: MemorySanitizer: use-of-uninitialized-value`
/// header line, with no stack trace, no `SUMMARY` and no origin - so a real bug found under MSan
/// carries no information at all about where it is. Other sanitizers are unaffected: their checks
/// fire only on genuinely invalid accesses, which the handler does not perform.
///
/// Emscripten declares `SIGEV_THREAD_ID` but its `sigevent` has no `_sigev_un`, and a
/// WebAssembly sandbox has no signals to deliver a timer expiry with in the first place.
#if (defined(SIGEV_THREAD_ID) || defined(OS_DARWIN)) && !(defined(THREAD_SANITIZER) && defined(OS_DARWIN)) \
    && !defined(MEMORY_SANITIZER) && defined(OS_HAS_SIGNAL_HANDLERS)
#    define QUERY_PROFILER_SUPPORTED 1
#endif

namespace DB
{

/**
  * Query profiler implementation for selected thread.
  *
  * This class installs timer and signal handler on creation to:
  *  1. periodically pause given thread
  *  2. collect thread's current stack trace
  *  3. write collected stack trace to trace_pipe for TraceCollector
  *
  * Destructor tries to unset timer and restore previous signal handler.
  * Note that signal handler implementation is defined by template parameter. See QueryProfilerReal and QueryProfilerCPU.
  */

#if defined(SIGEV_THREAD_ID) && defined(OS_HAS_SIGNAL_HANDLERS)
class Timer
{
public:
    Timer();
    Timer(const Timer &) = delete;
    Timer & operator = (const Timer &) = delete;
    ~Timer();

    void createIfNecessary(UInt64 thread_id, int clock_type, int pause_signal);
    void set(UInt64 period);
    void stop();
    void cleanup();

private:
    LoggerPtr log;
    std::optional<timer_t> timer_id;
};
#endif // defined(SIGEV_THREAD_ID) && defined(OS_HAS_SIGNAL_HANDLERS)

template <typename ProfilerImpl>
class QueryProfilerBase
{
    friend ProfilerImpl;

public:
    ~QueryProfilerBase();

    void setPeriod(UInt64 period_);

private:
    QueryProfilerBase(UInt64 thread_id, int clock_type, UInt64 period, int pause_signal_);
    void cleanup();

    LoggerPtr log;

#if defined(SIGEV_THREAD_ID) && defined(OS_HAS_SIGNAL_HANDLERS)
    inline static thread_local Timer timer = Timer();
#endif

    /// Pause signal to interrupt threads to get traces
    int pause_signal;
};

/// Query profiler with timer based on real clock
class QueryProfilerReal : public QueryProfilerBase<QueryProfilerReal>
{
public:
    QueryProfilerReal(UInt64 thread_id, UInt64 period); /// NOLINT

    static void signalHandler(int sig, siginfo_t * info, void * context);

    static constexpr int PAUSE_SIGNAL = SIGUSR1;
};

/// Query profiler with timer based on CPU clock
class QueryProfilerCPU : public QueryProfilerBase<QueryProfilerCPU>
{
public:
    QueryProfilerCPU(UInt64 thread_id, UInt64 period); /// NOLINT

    static void signalHandler(int sig, siginfo_t * info, void * context);

    static constexpr int PAUSE_SIGNAL = SIGUSR2;
};

}
