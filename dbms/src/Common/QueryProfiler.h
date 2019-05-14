#pragma once

#include <common/Backtrace.h>
#include <common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Interpreters/TraceCollector.h>

#include <signal.h>
#include <time.h>


namespace Poco
{
    class Logger;
}

namespace DB
{

enum class TimerType : UInt8
{
    Real,
    Cpu,
};

namespace
{
    void writeTraceInfo(TimerType timer_type, int /* sig */, siginfo_t * /* info */, void * context)
    {
        char buffer[DBMS_DEFAULT_BUFFER_SIZE];
        DB::WriteBufferFromFileDescriptor out(
            /* fd */ trace_pipe.fds_rw[1],
            /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE,
            /* existing_memory */ buffer
        );

        const std::string & query_id = CurrentThread::getQueryId();

        const auto signal_context = *reinterpret_cast<ucontext_t *>(context);
        const Backtrace backtrace(signal_context);

        DB::writePODBinary(backtrace, out);
        DB::writeStringBinary(query_id, out);
        DB::writeIntBinary(timer_type, out);
        out.next();
    }

    const UInt32 TIMER_PRECISION = 1e9;
}

template <typename ProfilerImpl>
class QueryProfilerBase
{
public:
    QueryProfilerBase(const Int32 thread_id, const int clock_type, const UInt32 period, const int pause_signal = SIGALRM)
        : log(&Logger::get("QueryProfiler"))
        , pause_signal(pause_signal)
    {
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

        struct sigaction sa{};
        sa.sa_sigaction = ProfilerImpl::signalHandler;
        sa.sa_flags = SA_SIGINFO | SA_RESTART;

        if (sigemptyset(&sa.sa_mask))
            throw Poco::Exception("Failed to clean signal mask for query profiler");

        if (sigaddset(&sa.sa_mask, pause_signal))
            throw Poco::Exception("Failed to add signal to mask for query profiler");

        if (sigaction(pause_signal, &sa, previous_handler))
            throw Poco::Exception("Failed to setup signal handler for query profiler");
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
