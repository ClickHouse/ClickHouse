#include "QueryProfiler.h"

#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/TraceCollector.h>
#include <Common/thread_local_rng.h>
#include <common/logger_useful.h>
#include <common/phdr_cache.h>
#include <common/errnoToString.h>

#include <random>


namespace ProfileEvents
{
    extern const Event QueryProfilerSignalOverruns;
}

namespace DB
{

namespace
{
#if defined(OS_LINUX)
    thread_local size_t write_trace_iteration = 0;
#endif

    void writeTraceInfo(TraceType trace_type, int /* sig */, siginfo_t * info, void * context)
    {
        auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

#if defined(OS_LINUX)
        if (info)
        {
            int overrun_count = info->si_overrun;

            /// Quickly drop if signal handler is called too frequently.
            /// Otherwise we may end up infinitelly processing signals instead of doing any useful work.
            ++write_trace_iteration;
            if (overrun_count)
            {
                /// But pass with some frequency to avoid drop of all traces.
                if (write_trace_iteration % (overrun_count + 1) == 0)
                {
                    ProfileEvents::increment(ProfileEvents::QueryProfilerSignalOverruns, overrun_count);
                }
                else
                {
                    ProfileEvents::increment(ProfileEvents::QueryProfilerSignalOverruns, overrun_count + 1);
                    return;
                }
            }
        }
#else
        UNUSED(info);
#endif

        const auto signal_context = *reinterpret_cast<ucontext_t *>(context);
        const StackTrace stack_trace(signal_context);

        TraceCollector::collect(trace_type, stack_trace, 0);

        errno = saved_errno;
    }

    [[maybe_unused]] constexpr UInt32 TIMER_PRECISION = 1e9;
}

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int CANNOT_DELETE_TIMER;
    extern const int NOT_IMPLEMENTED;
}

template <typename ProfilerImpl>
QueryProfilerBase<ProfilerImpl>::QueryProfilerBase(const UInt64 thread_id, const int clock_type, UInt32 period, const int pause_signal_)
    : log(&Poco::Logger::get("QueryProfiler"))
    , pause_signal(pause_signal_)
{
#if USE_UNWIND
    /// Sanity check.
    if (!hasPHDRCache())
        throw Exception("QueryProfiler cannot be used without PHDR cache, that is not available for TSan build", ErrorCodes::NOT_IMPLEMENTED);

    /// Too high frequency can introduce infinite busy loop of signal handlers. We will limit maximum frequency (with 1000 signals per second).
    if (period < 1000000)
        period = 1000000;

    struct sigaction sa{};
    sa.sa_sigaction = ProfilerImpl::signalHandler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    if (sigemptyset(&sa.sa_mask))
        throwFromErrno("Failed to clean signal mask for query profiler", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaddset(&sa.sa_mask, pause_signal))
        throwFromErrno("Failed to add signal to mask for query profiler", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaction(pause_signal, &sa, previous_handler))
        throwFromErrno("Failed to setup signal handler for query profiler", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

    try
    {
        struct sigevent sev {};
        sev.sigev_notify = SIGEV_THREAD_ID;
        sev.sigev_signo = pause_signal;

#   if defined(OS_FREEBSD)
        sev._sigev_un._threadid = thread_id;
#   else
        sev._sigev_un._tid = thread_id;
#   endif
        if (timer_create(clock_type, &sev, &timer_id))
        {
            /// In Google Cloud Run, the function "timer_create" is implemented incorrectly as of 2020-01-25.
            /// https://mybranch.dev/posts/clickhouse-on-cloud-run/
            if (errno == 0)
                throw Exception("Failed to create thread timer. The function 'timer_create' returned non-zero but didn't set errno. This is bug in your OS.",
                    ErrorCodes::CANNOT_CREATE_TIMER);

            throwFromErrno("Failed to create thread timer", ErrorCodes::CANNOT_CREATE_TIMER);
        }

        /// Randomize offset as uniform random value from 0 to period - 1.
        /// It will allow to sample short queries even if timer period is large.
        /// (For example, with period of 1 second, query with 50 ms duration will be sampled with 1 / 20 probability).
        /// It also helps to avoid interference (moire).
        UInt32 period_rand = std::uniform_int_distribution<UInt32>(0, period)(thread_local_rng);

        struct timespec interval{.tv_sec = period / TIMER_PRECISION, .tv_nsec = period % TIMER_PRECISION};
        struct timespec offset{.tv_sec = period_rand / TIMER_PRECISION, .tv_nsec = period_rand % TIMER_PRECISION};

        struct itimerspec timer_spec = {.it_interval = interval, .it_value = offset};
        if (timer_settime(timer_id, 0, &timer_spec, nullptr))
            throwFromErrno("Failed to set thread timer period", ErrorCodes::CANNOT_SET_TIMER_PERIOD);
    }
    catch (...)
    {
        tryCleanup();
        throw;
    }
#else
    UNUSED(thread_id);
    UNUSED(clock_type);
    UNUSED(period);
    UNUSED(pause_signal);

    throw Exception("QueryProfiler cannot work with stock libunwind", ErrorCodes::NOT_IMPLEMENTED);
#endif
}

template <typename ProfilerImpl>
QueryProfilerBase<ProfilerImpl>::~QueryProfilerBase()
{
    tryCleanup();
}

template <typename ProfilerImpl>
void QueryProfilerBase<ProfilerImpl>::tryCleanup()
{
#if USE_UNWIND
    if (timer_id != nullptr && timer_delete(timer_id))
        LOG_ERROR(log, "Failed to delete query profiler timer {}", errnoToString(ErrorCodes::CANNOT_DELETE_TIMER));

    if (previous_handler != nullptr && sigaction(pause_signal, previous_handler, nullptr))
        LOG_ERROR(log, "Failed to restore signal handler after query profiler {}", errnoToString(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER));
#endif
}

template class QueryProfilerBase<QueryProfilerReal>;
template class QueryProfilerBase<QueryProfilerCpu>;

QueryProfilerReal::QueryProfilerReal(const UInt64 thread_id, const UInt32 period)
    : QueryProfilerBase(thread_id, CLOCK_REALTIME, period, SIGUSR1)
{}

void QueryProfilerReal::signalHandler(int sig, siginfo_t * info, void * context)
{
    writeTraceInfo(TraceType::Real, sig, info, context);
}

QueryProfilerCpu::QueryProfilerCpu(const UInt64 thread_id, const UInt32 period)
    : QueryProfilerBase(thread_id, CLOCK_THREAD_CPUTIME_ID, period, SIGUSR2)
{}

void QueryProfilerCpu::signalHandler(int sig, siginfo_t * info, void * context)
{
    writeTraceInfo(TraceType::CPU, sig, info, context);
}

}
