#pragma once

#include <common/Pipe.h>
#include <common/StackTrace.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include <signal.h>
#include <time.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

extern LazyPipe trace_pipe;

enum class TimerType : UInt8
{
    Real,
    Cpu,
};

namespace
{
    /// Normally query_id is a UUID (string with a fixed length) but user can provide custom query_id.
    /// Thus upper bound on query_id length should be introduced to avoid buffer overflow in signal handler.
    constexpr size_t QUERY_ID_MAX_LEN = 1024;

    void writeTraceInfo(TimerType timer_type, int /* sig */, siginfo_t * /* info */, void * context)
    {
        constexpr size_t buf_size = sizeof(char) + // TraceCollector stop flag
                                    8 * sizeof(char) + // maximum VarUInt length for string size
                                    QUERY_ID_MAX_LEN * sizeof(char) + // maximum query_id length
                                    sizeof(StackTrace) + // collected stack trace
                                    sizeof(TimerType); // timer type
        char buffer[buf_size];
        DB::WriteBufferFromFileDescriptor out(trace_pipe.fds_rw[1], buf_size, buffer);

        const std::string & query_id = CurrentThread::getQueryId();
        const StringRef cut_query_id(query_id.c_str(), std::min(query_id.size(), QUERY_ID_MAX_LEN));

        const auto signal_context = *reinterpret_cast<ucontext_t *>(context);
        const StackTrace stack_trace(signal_context);

        DB::writeChar(false, out);
        DB::writeStringBinary(cut_query_id, out);
        DB::writePODBinary(stack_trace, out);
        DB::writePODBinary(timer_type, out);
        out.next();
    }

    const UInt32 TIMER_PRECISION = 1e9;
}


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
    QueryProfilerBase(const Int32 thread_id, const int clock_type, const UInt32 period, const int pause_signal = SIGALRM)
        : log(&Logger::get("QueryProfiler"))
        , pause_signal(pause_signal)
    {
        struct sigaction sa{};
        sa.sa_sigaction = ProfilerImpl::signalHandler;
        sa.sa_flags = SA_SIGINFO | SA_RESTART;

        if (sigemptyset(&sa.sa_mask))
            throw Poco::Exception("Failed to clean signal mask for query profiler");

        if (sigaddset(&sa.sa_mask, pause_signal))
            throw Poco::Exception("Failed to add signal to mask for query profiler");

        if (sigaction(pause_signal, &sa, previous_handler))
            throw Poco::Exception("Failed to setup signal handler for query profiler");

        struct sigevent sev;
        sev.sigev_notify = SIGEV_THREAD_ID;
        sev.sigev_signo = pause_signal;
        sev._sigev_un._tid = thread_id;
        if (timer_create(clock_type, &sev, &timer_id))
            throw Poco::Exception("Failed to create thread timer");

        struct timespec interval{.tv_sec = period / TIMER_PRECISION, .tv_nsec = period % TIMER_PRECISION};
        struct itimerspec timer_spec = {.it_interval = interval, .it_value = interval};
        if (timer_settime(timer_id, 0, &timer_spec, nullptr))
            throw Poco::Exception("Failed to set thread timer");
    }

    ~QueryProfilerBase()
    {
        if (timer_delete(timer_id))
            LOG_ERROR(log, "Failed to delete query profiler timer");

        if (sigaction(pause_signal, previous_handler, nullptr))
            LOG_ERROR(log, "Failed to restore signal handler after query profiler");
    }

private:
    Poco::Logger * log;

    /// Timer id from timer_create(2)
    timer_t timer_id = nullptr;

    /// Pause signal to interrupt threads to get traces
    int pause_signal;

    /// Previous signal handler to restore after query profiler exits
    struct sigaction * previous_handler = nullptr;
};

/// Query profiler with timer based on real clock
class QueryProfilerReal : public QueryProfilerBase<QueryProfilerReal>
{
public:
    QueryProfilerReal(const Int32 thread_id, const UInt32 period)
        : QueryProfilerBase(thread_id, CLOCK_REALTIME, period, SIGUSR1)
    {}

    static void signalHandler(int sig, siginfo_t * info, void * context)
    {
        writeTraceInfo(TimerType::Real, sig, info, context);
    }
};

/// Query profiler with timer based on CPU clock
class QueryProfilerCpu : public QueryProfilerBase<QueryProfilerCpu>
{
public:
    QueryProfilerCpu(const Int32 thread_id, const UInt32 period)
        : QueryProfilerBase(thread_id, CLOCK_THREAD_CPUTIME_ID, period, SIGUSR2)
    {}

    static void signalHandler(int sig, siginfo_t * info, void * context)
    {
        writeTraceInfo(TimerType::Cpu, sig, info, context);
    }
};

}
