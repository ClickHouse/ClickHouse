#pragma once

#include <optional>
#include <base/types.h>
#include <signal.h>
#include <time.h>

#include "config.h"

#include <Common/Logger.h>


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
  * Destructor tries to unset timer and restore previous signal handler.
  * Note that signal handler implementation is defined by template parameter. See QueryProfilerReal and QueryProfilerCPU.
  */

#ifndef __APPLE__
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
#endif

template <typename ProfilerImpl>
class QueryProfilerBase
{
public:
    QueryProfilerBase(UInt64 thread_id, int clock_type, UInt64 period, int pause_signal_);
    ~QueryProfilerBase();

    void setPeriod(UInt64 period_);

private:
    void cleanup();

    LoggerPtr log;

#ifndef __APPLE__
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
};

/// Query profiler with timer based on CPU clock
class QueryProfilerCPU : public QueryProfilerBase<QueryProfilerCPU>
{
public:
    QueryProfilerCPU(UInt64 thread_id, UInt64 period); /// NOLINT

    static void signalHandler(int sig, siginfo_t * info, void * context);
};

}
