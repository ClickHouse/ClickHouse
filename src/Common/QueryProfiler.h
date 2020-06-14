#pragma once

#include <Core/Types.h>
#include <signal.h>
#include <time.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif


namespace Poco
{
    class Logger;
}

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
  * Desctructor tries to unset timer and restore previous signal handler.
  * Note that signal handler implementation is defined by template parameter. See QueryProfilerReal and QueryProfilerCpu.
  */
template <typename ProfilerImpl>
class QueryProfilerBase
{
public:
    QueryProfilerBase(const UInt64 thread_id, const int clock_type, UInt32 period, const int pause_signal_);
    ~QueryProfilerBase();

private:
    void tryCleanup();

    Poco::Logger * log;

#if USE_UNWIND
    /// Timer id from timer_create(2)
    timer_t timer_id = nullptr;
#endif

    /// Pause signal to interrupt threads to get traces
    int pause_signal;

    /// Previous signal handler to restore after query profiler exits
    struct sigaction * previous_handler = nullptr;
};

/// Query profiler with timer based on real clock
class QueryProfilerReal : public QueryProfilerBase<QueryProfilerReal>
{
public:
    QueryProfilerReal(const UInt64 thread_id, const UInt32 period);

    static void signalHandler(int sig, siginfo_t * info, void * context);
};

/// Query profiler with timer based on CPU clock
class QueryProfilerCpu : public QueryProfilerBase<QueryProfilerCpu>
{
public:
    QueryProfilerCpu(const UInt64 thread_id, const UInt32 period);

    static void signalHandler(int sig, siginfo_t * info, void * context);
};

}
