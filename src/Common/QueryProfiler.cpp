#include <Common/QueryProfiler.h>

#include <IO/WriteHelpers.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/phdr_cache.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/StackTrace.h>
#include <Common/TraceSender.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>


namespace CurrentMetrics
{
    extern const Metric CreatedTimersInQueryProfiler;
    extern const Metric ActiveTimersInQueryProfiler;
}

namespace ProfileEvents
{
    extern const Event QueryProfilerSignalOverruns;
    extern const Event QueryProfilerConcurrencyOverruns;
    extern const Event QueryProfilerRuns;
    extern const Event QueryProfilerErrors;
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

    /// Don't permit too many threads be busy inside profiler,
    /// which could slow down the system in some environments.
    std::atomic<Int64> concurrent_invocations = 0;

    void writeTraceInfo(TraceType trace_type, int /* sig */, siginfo_t * info, void * context)
    {
        SCOPE_EXIT({ concurrent_invocations.fetch_sub(1, std::memory_order_relaxed); });
        if (concurrent_invocations.fetch_add(1, std::memory_order_relaxed) > 100)
        {
            ProfileEvents::incrementNoTrace(ProfileEvents::QueryProfilerConcurrencyOverruns);
            return;
        }

        const auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

#if defined(OS_LINUX)
        if (info)
        {
            const int overrun_count = info->si_overrun;

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
        std::optional<StackTrace> stack_trace;

#if defined(SANITIZER)
        constexpr bool sanitizer = true;
#else
        constexpr bool sanitizer = false;
#endif

        asynchronous_stack_unwinding = true;
        if (sanitizer || 0 == sigsetjmp(asynchronous_stack_unwinding_signal_jump_buffer, 1))
        {
            stack_trace.emplace(signal_context);
        }
        else
        {
            ProfileEvents::incrementNoTrace(ProfileEvents::QueryProfilerErrors);
        }
        asynchronous_stack_unwinding = false;

        if (stack_trace)
            TraceSender::send(trace_type, *stack_trace, {});

        ProfileEvents::incrementNoTrace(ProfileEvents::QueryProfilerRuns);
        errno = saved_errno;
    }

    [[maybe_unused]] constexpr UInt64 TIMER_PRECISION = 1e9;
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
    : log(getLogger("Timer"))
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

            throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Failed to create thread timer");
        }
        timer_id.emplace(local_timer_id);
        CurrentMetrics::add(CurrentMetrics::CreatedTimersInQueryProfiler);
    }
}

void Timer::set(UInt64 period)
{
    /// Too high frequency can introduce infinite busy loop of signal handlers. We will limit maximum frequency (with 1000 signals per second).
    period = std::max<UInt64>(period, 1000000);
    /// Randomize offset as uniform random value from 0 to period - 1.
    /// It will allow to sample short queries even if timer period is large.
    /// (For example, with period of 1 second, query with 50 ms duration will be sampled with 1 / 20 probability).
    /// It also helps to avoid interference (moire).
    UInt64 period_rand = std::uniform_int_distribution<UInt64>(0, period)(thread_local_rng);

    struct timespec interval{.tv_sec = time_t(period / TIMER_PRECISION), .tv_nsec = int64_t(period % TIMER_PRECISION)};
    struct timespec offset{.tv_sec = time_t(period_rand / TIMER_PRECISION), .tv_nsec = int64_t(period_rand % TIMER_PRECISION)};

    struct itimerspec timer_spec = {.it_interval = interval, .it_value = offset};
    if (timer_settime(*timer_id, 0, &timer_spec, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_TIMER_PERIOD, "Failed to set thread timer period");
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
QueryProfilerBase<ProfilerImpl>::QueryProfilerBase(
    [[maybe_unused]] UInt64 thread_id, [[maybe_unused]] int clock_type, [[maybe_unused]] UInt64 period, [[maybe_unused]] int pause_signal_)
    : log(getLogger("QueryProfiler")), pause_signal(pause_signal_)
{
#if defined(SANITIZER)
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler disabled because they cannot work under sanitizers");
#elif defined(__APPLE__)
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler cannot work on OSX");
#else
    /// Sanity check.
    if (!hasPHDRCache())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler cannot be used without PHDR cache, that is not available for TSan build");

    struct sigaction sa{};
    sa.sa_sigaction = ProfilerImpl::signalHandler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    if (sigemptyset(&sa.sa_mask))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to clean signal mask for query profiler");

    if (sigaddset(&sa.sa_mask, pause_signal))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to add signal to mask for query profiler");

    if (sigaction(pause_signal, &sa, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Failed to setup signal handler for query profiler");

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
void QueryProfilerBase<ProfilerImpl>::setPeriod([[maybe_unused]] UInt64 period_)
{
#if defined(SANITIZER)
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler disabled because they cannot work under sanitizers");
#elif defined(__APPLE__)
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler cannot work on OSX");
#else
    timer.set(period_);
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

QueryProfilerReal::QueryProfilerReal(UInt64 thread_id, UInt64 period)
    : QueryProfilerBase(thread_id, CLOCK_MONOTONIC, period, SIGUSR1)
{}

void QueryProfilerReal::signalHandler(int sig, siginfo_t * info, void * context)
{
    if (signal_handler_disarmed)
        return;

    DENY_ALLOCATIONS_IN_SCOPE;
    writeTraceInfo(TraceType::Real, sig, info, context);
}

QueryProfilerCPU::QueryProfilerCPU(UInt64 thread_id, UInt64 period)
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
