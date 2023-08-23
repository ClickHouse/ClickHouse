#include "QueryProfiler.h"

#include <IO/WriteHelpers.h>
#include <Common/TraceSender.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/phdr_cache.h>
#include <base/errnoToString.h>

#include <random>

namespace CurrentMetrics
{
    extern const Metric CreatedTimersInQueryProfiler;
    extern const Metric ActiveTimersInQueryProfiler;
}

namespace ProfileEvents
{
    extern const Event QueryProfilerSignalOverruns;
    extern const Event QueryProfilerRuns;
}

namespace DB
{

namespace
{
#if defined(OS_LINUX)
    thread_local size_t write_trace_iteration = 0;
#endif
    /// Even after timer_delete() the signal can be delivered,
    /// since it does not do anything with pending signals.
    ///
    /// And so to overcome this flag is exists,
    /// to ignore delivered signals after timer_delete().
    thread_local bool signal_handler_disarmed = true;

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
                if (overrun_count > 0 && write_trace_iteration % (overrun_count + 1) == 0)
                {
                    ProfileEvents::incrementNoTrace(ProfileEvents::QueryProfilerSignalOverruns, overrun_count);
                }
                else
                {
                    ProfileEvents::incrementNoTrace(ProfileEvents::QueryProfilerSignalOverruns, std::max(0, overrun_count) + 1);
                    return;
                }
            }
        }
#else
        UNUSED(info);
#endif

        const auto signal_context = *reinterpret_cast<ucontext_t *>(context);
        const StackTrace stack_trace(signal_context);

        TraceSender::send(trace_type, stack_trace, {});
        ProfileEvents::incrementNoTrace(ProfileEvents::QueryProfilerRuns);

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
    extern const int NOT_IMPLEMENTED;
}

#ifndef __APPLE__
Timer::Timer()
    : log(&Poco::Logger::get("Timer"))
{}

void Timer::createIfNecessary(UInt64 thread_id, int clock_type, int pause_signal)
{
    if (!timer_id)
    {
        struct sigevent sev {};
        sev.sigev_notify = SIGEV_THREAD_ID;
        sev.sigev_signo = pause_signal;

#if defined(OS_FREEBSD)
        sev._sigev_un._threadid = static_cast<pid_t>(thread_id);
#elif defined(USE_MUSL)
        sev.sigev_notify_thread_id = static_cast<pid_t>(thread_id);
#else
        sev._sigev_un._tid = static_cast<pid_t>(thread_id);
#endif
        timer_t local_timer_id;
        if (timer_create(clock_type, &sev, &local_timer_id))
        {
            /// In Google Cloud Run, the function "timer_create" is implemented incorrectly as of 2020-01-25.
            /// https://mybranch.dev/posts/clickhouse-on-cloud-run/
            if (errno == 0)
                throw Exception(ErrorCodes::CANNOT_CREATE_TIMER, "Failed to create thread timer. The function "
                                "'timer_create' returned non-zero but didn't set errno. This is bug in your OS.");

            /// For example, it cannot be created if the server is run under QEMU:
            /// "Failed to create thread timer, errno: 11, strerror: Resource temporarily unavailable."

            /// You could accidentally run the server under QEMU without being aware,
            /// if you use Docker image for a different architecture,
            /// and you have the "binfmt-misc" kernel module, and "qemu-user" tools.

            /// Also, it cannot be created if the server has too many threads.

            throwFromErrno("Failed to create thread timer", ErrorCodes::CANNOT_CREATE_TIMER);
        }
        timer_id.emplace(local_timer_id);
        CurrentMetrics::add(CurrentMetrics::CreatedTimersInQueryProfiler);
    }
}

void Timer::set(UInt32 period)
{
    /// Too high frequency can introduce infinite busy loop of signal handlers. We will limit maximum frequency (with 1000 signals per second).
    if (period < 1000000)
        period = 1000000;
    /// Randomize offset as uniform random value from 0 to period - 1.
    /// It will allow to sample short queries even if timer period is large.
    /// (For example, with period of 1 second, query with 50 ms duration will be sampled with 1 / 20 probability).
    /// It also helps to avoid interference (moire).
    UInt32 period_rand = std::uniform_int_distribution<UInt32>(0, period)(thread_local_rng);

    struct timespec interval{.tv_sec = period / TIMER_PRECISION, .tv_nsec = period % TIMER_PRECISION};
    struct timespec offset{.tv_sec = period_rand / TIMER_PRECISION, .tv_nsec = period_rand % TIMER_PRECISION};

    struct itimerspec timer_spec = {.it_interval = interval, .it_value = offset};
    if (timer_settime(*timer_id, 0, &timer_spec, nullptr))
        throwFromErrno("Failed to set thread timer period", ErrorCodes::CANNOT_SET_TIMER_PERIOD);
    CurrentMetrics::add(CurrentMetrics::ActiveTimersInQueryProfiler);
}

void Timer::stop()
{
    if (timer_id)
    {
        struct timespec stop_timer{.tv_sec = 0, .tv_nsec = 0};
        struct itimerspec timer_spec = {.it_interval = stop_timer, .it_value = stop_timer};
        int err = timer_settime(*timer_id, 0, &timer_spec, nullptr);
        if (err)
            LOG_ERROR(log, "Failed to stop query profiler timer {}", errnoToString());
        chassert(!err && "Failed to stop query profiler timer");
        CurrentMetrics::sub(CurrentMetrics::ActiveTimersInQueryProfiler);
    }
}

Timer::~Timer()
{
    try
    {
        cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void Timer::cleanup()
{
    if (timer_id)
    {
        int err = timer_delete(*timer_id);
        if (err)
            LOG_ERROR(log, "Failed to delete query profiler timer {}", errnoToString());
        chassert(!err && "Failed to delete query profiler timer");

        timer_id.reset();
        CurrentMetrics::sub(CurrentMetrics::CreatedTimersInQueryProfiler);
    }
}
#endif

template <typename ProfilerImpl>
QueryProfilerBase<ProfilerImpl>::QueryProfilerBase(UInt64 thread_id, int clock_type, UInt32 period, int pause_signal_)
    : log(&Poco::Logger::get("QueryProfiler"))
    , pause_signal(pause_signal_)
{
#if defined(SANITIZER)
    UNUSED(thread_id);
    UNUSED(clock_type);
    UNUSED(period);
    UNUSED(pause_signal);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler disabled because they cannot work under sanitizers");
#elif defined(__APPLE__)
    UNUSED(thread_id);
    UNUSED(clock_type);
    UNUSED(period);
    UNUSED(pause_signal);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler cannot work on OSX");
#else
    /// Sanity check.
    if (!hasPHDRCache())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler cannot be used without PHDR cache, that is not available for TSan build");

    struct sigaction sa{};
    sa.sa_sigaction = ProfilerImpl::signalHandler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    if (sigemptyset(&sa.sa_mask))
        throwFromErrno("Failed to clean signal mask for query profiler", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaddset(&sa.sa_mask, pause_signal))
        throwFromErrno("Failed to add signal to mask for query profiler", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaction(pause_signal, &sa, nullptr))
        throwFromErrno("Failed to setup signal handler for query profiler", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

    try
    {
        timer.createIfNecessary(thread_id, clock_type, pause_signal);
        timer.set(period);
        signal_handler_disarmed = false;
    }
    catch (...)
    {
        timer.cleanup();
        throw;
    }
#endif
}

template <typename ProfilerImpl>
QueryProfilerBase<ProfilerImpl>::~QueryProfilerBase()
{
    try
    {
        cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

template <typename ProfilerImpl>
void QueryProfilerBase<ProfilerImpl>::cleanup()
{
#ifndef __APPLE__
    timer.stop();
    signal_handler_disarmed = true;
#endif
}

template class QueryProfilerBase<QueryProfilerReal>;
template class QueryProfilerBase<QueryProfilerCPU>;

QueryProfilerReal::QueryProfilerReal(UInt64 thread_id, UInt32 period)
    : QueryProfilerBase(thread_id, CLOCK_MONOTONIC, period, SIGUSR1)
{}

void QueryProfilerReal::signalHandler(int sig, siginfo_t * info, void * context)
{
    if (signal_handler_disarmed)
        return;

    DENY_ALLOCATIONS_IN_SCOPE;
    writeTraceInfo(TraceType::Real, sig, info, context);
}

QueryProfilerCPU::QueryProfilerCPU(UInt64 thread_id, UInt32 period)
    : QueryProfilerBase(thread_id, CLOCK_THREAD_CPUTIME_ID, period, SIGUSR2)
{}

void QueryProfilerCPU::signalHandler(int sig, siginfo_t * info, void * context)
{
    if (signal_handler_disarmed)
        return;

    DENY_ALLOCATIONS_IN_SCOPE;
    writeTraceInfo(TraceType::CPU, sig, info, context);
}

}
