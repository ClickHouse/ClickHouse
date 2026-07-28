#include <Common/QueryProfiler.h>

#include <IO/WriteHelpers.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/phdr_cache.h>
#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/MemoryTracker.h>
#include <Common/StackTrace.h>
#include <Common/TraceSender.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Common/setThreadName.h>
#include <Common/StackTraceServiceSignal.h>
#include <csignal>

#if defined(OS_DARWIN)
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <utility>
#include <pthread.h>
#include <mach/mach.h>
#include <mach/thread_act.h>
#endif

#include "config.h"


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
            ProfileEvents::incrementSignalSafe(ProfileEvents::QueryProfilerConcurrencyOverruns);
            return;
        }

        const auto saved_errno = errno; /// We must restore previous value of errno in signal handler.

#if defined(OS_LINUX)
        if (info)
        {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
            const int overrun_count = info->si_overrun;
#pragma clang diagnostic pop

            /// Quickly drop if signal handler is called too frequently.
            /// Otherwise we may end up infinitelly processing signals instead of doing any useful work.
            ++write_trace_iteration;
            if (overrun_count)
            {
                /// But pass with some frequency to avoid drop of all traces.
                if (overrun_count > 0 && write_trace_iteration % (overrun_count + 1) == 0)
                {
                    ProfileEvents::incrementSignalSafe(ProfileEvents::QueryProfilerSignalOverruns, overrun_count);
                }
                else
                {
                    ProfileEvents::incrementSignalSafe(ProfileEvents::QueryProfilerSignalOverruns, std::max(0, overrun_count) + 1);
                    return;
                }
            }
        }
#else
        UNUSED(info);
#endif

        std::optional<StackTrace> stack_trace;

#if defined(THREAD_SANITIZER) && !defined(OS_DARWIN)
        /// Under TSan on Linux, use abseil's frame-pointer-based unwinding (via the default StackTrace
        /// constructor) instead of the ucontext_t constructor which uses libunwind. abseil validates the
        /// frame chain and does not fault, so no async-unwind recovery is needed. On macOS there is no
        /// abseil path (StackTrace always uses backtrace()), which can fault in frame-pointer-less
        /// libsystem code, so Darwin takes the recovery branch below even under TSan.
        UNUSED(context);
        stack_trace.emplace();
#else
        const auto signal_context = *reinterpret_cast<ucontext_t *>(context);
        /// The ucontext StackTrace constructor recovers internally from a fault while unwinding
        /// (e.g. off a fiber stack) and yields an empty trace in that case.
        stack_trace.emplace(signal_context);
        if (stack_trace->getSize() == 0)
            ProfileEvents::incrementSignalSafe(ProfileEvents::QueryProfilerErrors);
#endif

        /// Skip empty captures: StackTrace(ucontext) recovers from an unwind fault by returning a
        /// zero-frame trace (already counted as QueryProfilerErrors above). Sending it would add a
        /// meaningless empty row to system.trace_log and inflate the sample count.
        if (stack_trace && stack_trace->getSize() != 0)
            TraceSender::send(trace_type, *stack_trace, {});

        ProfileEvents::incrementSignalSafe(ProfileEvents::QueryProfilerRuns);
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

#if defined(SIGEV_THREAD_ID)
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
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
        sev.sigev_notify_thread_id = static_cast<pid_t>(thread_id);
#pragma clang diagnostic pop
#else
        sev._sigev_un._tid = static_cast<pid_t>(thread_id);
#endif
        timer_t local_timer_id = nullptr;
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

#if defined(OS_DARWIN)
namespace
{
    UInt64 nowMonotonicNs()
    {
        struct timespec ts{};
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<UInt64>(ts.tv_sec) * TIMER_PRECISION + static_cast<UInt64>(ts.tv_nsec);
    }

    /// Sum of user + system CPU time consumed by a thread, in nanoseconds, via the Mach kernel.
    /// macOS has no per-thread CPU timer (CLOCK_THREAD_CPUTIME_ID is self-only), so the sampler
    /// polls this to drive the CPU profiler.
    UInt64 threadCPUNs(mach_port_t mach_thread)
    {
        thread_basic_info_data_t info{};
        mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
        if (thread_info(mach_thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count) != KERN_SUCCESS)
            return 0;
        auto to_ns = [](const time_value_t & t)
        { return static_cast<UInt64>(t.seconds) * TIMER_PRECISION + static_cast<UInt64>(t.microseconds) * 1000; };
        return to_ns(info.user_time) + to_ns(info.system_time);
    }

    /// macOS has neither timer_create nor SIGEV_THREAD_ID, so per-thread periodic sampling is driven
    /// by a single background thread that delivers the pause signal to each registered thread via
    /// pthread_kill. This is the centralized equivalent of the Linux per-thread POSIX timer: the
    /// signal handler is identical, only the "who fires it" mechanism differs.
    class ProfilerSampler
    {
    public:
        static ProfilerSampler & instance()
        {
            static ProfilerSampler sampler;
            return sampler;
        }

        void addThread(pthread_t thread, int clock_type, UInt64 period_ns, int signal)
        {
            std::lock_guard lock(mutex);
            auto & reg = registrations[{reinterpret_cast<uintptr_t>(thread), signal}];
            reg.thread = thread;
            reg.mach_thread = pthread_mach_thread_np(thread);
            reg.signal = signal;
            reg.is_cpu = (clock_type == CLOCK_THREAD_CPUTIME_ID);
            armRegistration(reg, period_ns);
            ensureThreadStarted();
            cond.notify_all();
        }

        void setThreadPeriod(pthread_t thread, int signal, UInt64 period_ns)
        {
            std::lock_guard lock(mutex);
            auto it = registrations.find({reinterpret_cast<uintptr_t>(thread), signal});
            if (it == registrations.end())
                return;
            /// Re-arm from now with the new period (like the Linux Timer::set), so lowering the period
            /// takes effect immediately instead of waiting out the previous, possibly much larger,
            /// deadline (e.g. the 10s default global profiler before a query sets a smaller period).
            armRegistration(it->second, period_ns);
            cond.notify_all();
        }

        void removeThread(pthread_t thread, int signal)
        {
            std::lock_guard lock(mutex);
            registrations.erase({reinterpret_cast<uintptr_t>(thread), signal});
        }

        ~ProfilerSampler()
        {
            {
                std::lock_guard lock(mutex);
                shutdown = true;
            }
            cond.notify_all();
            if (sampler_thread.joinable())
                sampler_thread.join();
        }

    private:
        struct Registration
        {
            pthread_t thread{};
            mach_port_t mach_thread = 0;
            int signal = 0;
            bool is_cpu = false;
            UInt64 period_ns = 0;
            UInt64 next_deadline_ns = 0; /// wall-clock: next fire (real) or next poll (cpu)
            UInt64 last_cpu_ns = 0;      /// cpu time at the last poll (cpu only)
            UInt64 cpu_budget_ns = 0;    /// cpu time remaining until the next fire (cpu only)
        };

        /// Cap frequency at 1000 signals/sec, matching the Linux Timer::set floor.
        static UInt64 clampPeriod(UInt64 period_ns) { return std::max<UInt64>(period_ns, 1'000'000); }

        /// Floor for the CPU poll interval, so a thread parked just below its CPU threshold is not
        /// polled unboundedly often. Well under the 1ms minimum period, so it does not coarsen sampling.
        static constexpr UInt64 MIN_CPU_POLL_NS = 200'000;

        /// Randomized first-fire offset in [0, period], so queries shorter than the period still have a
        /// chance to be sampled (mirrors the randomized `it_value` in the Linux Timer::set).
        static UInt64 randomizedOffset(UInt64 period_ns)
        {
            return std::uniform_int_distribution<UInt64>(0, period_ns)(thread_local_rng);
        }

        /// (Re)arm a registration, mirroring the randomized initial `it_value` of the Linux POSIX timers
        /// so a query shorter than the period can still be sampled.
        void armRegistration(Registration & reg, UInt64 period_ns)
        {
            reg.period_ns = clampPeriod(period_ns);
            if (reg.is_cpu)
            {
                /// Match the Linux CLOCK_THREAD_CPUTIME_ID timer: first fire after a random [0, period]
                /// of CPU time, then every period. macOS has no CPU-time timer, so the sampler polls
                /// thread_info; scheduling the first poll at `now + budget` (budget measured in CPU time,
                /// which equals wall time for a fully busy thread) reproduces the Linux ~duration/period
                /// short-query sampling probability, unlike a fixed wall cadence.
                reg.cpu_budget_ns = randomizedOffset(reg.period_ns);
                reg.last_cpu_ns = threadCPUNs(reg.mach_thread);
                reg.next_deadline_ns = nowMonotonicNs() + std::max<UInt64>(reg.cpu_budget_ns, MIN_CPU_POLL_NS);
            }
            else
                reg.next_deadline_ns = nowMonotonicNs() + randomizedOffset(reg.period_ns);
        }

        void ensureThreadStarted()
        {
            if (!thread_started)
            {
                sampler_thread = std::thread([this] { run(); });
                thread_started = true;
            }
        }

        void run()
        {
            setThreadName(ThreadName::QUERY_PROFILER);

            std::unique_lock lock(mutex);
            while (!shutdown)
            {
                if (registrations.empty())
                {
                    cond.wait(lock);
                    continue;
                }

                UInt64 now = nowMonotonicNs();
                /// Sleep until the nearest per-registration deadline rather than on a fixed tick, so a
                /// coarse profiler (e.g. the default 10s global CPU profiler) does not wake the sampler a
                /// thousand times a second. addThread()/setThreadPeriod() wake us early via notify for a
                /// shorter period. Accumulated in the same pass that processes the due registrations.
                UInt64 sleep_ns = 3600ULL * TIMER_PRECISION;
                for (auto & [key, reg] : registrations)
                {
                    /// Each registration has its own wall-clock deadline, so a coarse profiler is only
                    /// touched at its own cadence even when a fine one keeps the sampler busy. In
                    /// particular, thread_info is not called for a long-period CPU registration until it
                    /// is actually due - no shared amplification from the shortest configured period.
                    if (now >= reg.next_deadline_ns)
                    {
                        bool fire = false;
                        if (reg.is_cpu)
                        {
                            /// Fire once the thread has consumed `cpu_budget_ns` more CPU time (see armRegistration()).
                            /// CPU time is monotonic, so a reading not above the previous one means no reliable
                            /// progress - in particular threadCPUNs() returns 0 on a transient thread_info error.
                            /// Treat that as zero consumed (never underflow into a spurious fire) and keep the
                            /// last reading (never regress it, which would over-count on the next successful poll).
                            UInt64 cpu = threadCPUNs(reg.mach_thread);
                            UInt64 consumed = cpu > reg.last_cpu_ns ? cpu - reg.last_cpu_ns : 0;
                            reg.last_cpu_ns = std::max(cpu, reg.last_cpu_ns);
                            if (consumed >= reg.cpu_budget_ns)
                            {
                                fire = true;
                                reg.cpu_budget_ns = reg.period_ns; /// next fire after a full period of CPU
                            }
                            else
                                reg.cpu_budget_ns -= consumed;
                            /// Schedule the next poll at the wall time the remaining budget would elapse under
                            /// full CPU use (a lower bound on the real fire time, so we never overshoot it);
                            /// a partly-idle thread simply re-estimates here on the next poll. The floor
                            /// bounds polling of a thread stuck just below the threshold.
                            reg.next_deadline_ns = now + std::max<UInt64>(reg.cpu_budget_ns, MIN_CPU_POLL_NS);
                        }
                        else
                        {
                            fire = true;
                            reg.next_deadline_ns = now + reg.period_ns; /// real timer fires every period
                        }

                        /// pthread_kill returns ESRCH if the thread is already gone; the owning
                        /// QueryProfiler removes its registration before the thread exits, so this is benign.
                        if (fire)
                            pthread_kill(reg.thread, reg.signal);
                    }

                    sleep_ns = std::min(sleep_ns, reg.next_deadline_ns > now ? reg.next_deadline_ns - now : UInt64(0));
                }
                cond.wait_for(lock, std::chrono::nanoseconds(sleep_ns));
            }
        }

        std::mutex mutex;
        std::condition_variable cond;
        /// Keyed by (thread, signal): one thread can run both a Real (SIGUSR1) and a CPU (SIGUSR2)
        /// profiler at once, so the signal must be part of the key or one would overwrite the other.
        std::map<std::pair<uintptr_t, int>, Registration> registrations;
        std::thread sampler_thread;
        bool thread_started = false;
        std::atomic<bool> shutdown = false;
    };
}
#endif

template <typename ProfilerImpl>
QueryProfilerBase<ProfilerImpl>::QueryProfilerBase(
    [[maybe_unused]] UInt64 thread_id, [[maybe_unused]] int clock_type, [[maybe_unused]] UInt64 period, [[maybe_unused]] int pause_signal_)
    : log(getLogger("QueryProfiler")), pause_signal(pause_signal_)
{
#if defined(SIGEV_THREAD_ID) || defined(OS_DARWIN)
    /// Under TSan we use frame-pointer-based unwinding (via abseil) which does not
    /// call dl_iterate_phdr in the signal handler, so the PHDR cache is not needed for
    /// stack capture. Symbolization happens later in a normal thread context.
#if !defined(THREAD_SANITIZER)
    if (!hasAsyncSignalSafeUnwind())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler cannot be used without async-signal-safe stack unwinding in this build");
#endif

    /// `sigaction` is `#define sigaction __sigaction` on some platforms, and
    /// `sigemptyset` / `sigaddset` are similarly macro-defined in <signal.h>,
    /// so all three trigger `-Wdisabled-macro-expansion`.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
    struct sigaction sa{};
    sa.sa_sigaction = ProfilerImpl::signalHandler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    if (sigemptyset(&sa.sa_mask))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to clean signal mask for query profiler");

    if (sigaddset(&sa.sa_mask, pause_signal))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to add signal to mask for query profiler");

#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// The Real (SIGUSR1) and CPU (SIGUSR2) profilers, system.stack_trace (STACK_TRACE_SERVICE_SIGNAL)
    /// and the debug SIGTSTP stack dumper all capture through StackTrace(ucontext) and share one
    /// thread-local stack-unwinding recovery (the asynchronous_stack_unwinding flag + sigjmp_buf in
    /// StackTrace). Block all of them while a handler runs so none can nest and clobber that buffer - a
    /// corrupted siglongjmp jumps to a garbage address, and a cleared flag turns the next unwind fault
    /// back into a fatal crash. SIGTSTP is the only one of these that returns to the interrupted handler;
    /// the fatal signals do not.
    if (sigaddset(&sa.sa_mask, QueryProfilerReal::PAUSE_SIGNAL) || sigaddset(&sa.sa_mask, QueryProfilerCPU::PAUSE_SIGNAL)
        || sigaddset(&sa.sa_mask, STACK_TRACE_SERVICE_SIGNAL) || sigaddset(&sa.sa_mask, SIGTSTP))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to add profiler signals to mask for query profiler");
#endif
#pragma clang diagnostic pop

    if (sigaction(pause_signal, &sa, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Failed to setup signal handler for query profiler");

    try
    {
#if defined(SIGEV_THREAD_ID)
        timer.createIfNecessary(thread_id, clock_type, pause_signal);
        timer.set(period);
#else
        /// macOS: a shared background thread delivers the signal via pthread_kill (see ProfilerSampler).
        ProfilerSampler::instance().addThread(pthread_self(), clock_type, period, pause_signal);
#endif
        signal_handler_disarmed = false;
    }
    catch (...)
    {
#if defined(SIGEV_THREAD_ID)
        timer.cleanup();
#else
        ProfilerSampler::instance().removeThread(pthread_self(), pause_signal);
#endif
        throw;
    }
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler requires SIGEV_THREAD_ID");
#endif
}


template <typename ProfilerImpl>
void QueryProfilerBase<ProfilerImpl>::setPeriod([[maybe_unused]] UInt64 period_)
{
#if defined(SIGEV_THREAD_ID)
    timer.set(period_);
#elif defined(OS_DARWIN)
    ProfilerSampler::instance().setThreadPeriod(pthread_self(), pause_signal, period_);
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "QueryProfiler requires SIGEV_THREAD_ID");
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
#if defined(SIGEV_THREAD_ID)
    timer.stop();
    signal_handler_disarmed = true;
#elif defined(OS_DARWIN)
    ProfilerSampler::instance().removeThread(pthread_self(), pause_signal);
    signal_handler_disarmed = true;
#endif
}

template class QueryProfilerBase<QueryProfilerReal>;
template class QueryProfilerBase<QueryProfilerCPU>;

QueryProfilerReal::QueryProfilerReal(UInt64 thread_id, UInt64 period)
    : QueryProfilerBase(thread_id, CLOCK_MONOTONIC, period, PAUSE_SIGNAL)
{}

void QueryProfilerReal::signalHandler(int sig, siginfo_t * info, void * context)
{
    if (signal_handler_disarmed)
        return;

    DENY_ALLOCATIONS_IN_SCOPE;
    writeTraceInfo(TraceType::Real, sig, info, context);
}

QueryProfilerCPU::QueryProfilerCPU(UInt64 thread_id, UInt64 period)
    : QueryProfilerBase(thread_id, CLOCK_THREAD_CPUTIME_ID, period, PAUSE_SIGNAL)
{}

void QueryProfilerCPU::signalHandler(int sig, siginfo_t * info, void * context)
{
    if (signal_handler_disarmed)
        return;

    DENY_ALLOCATIONS_IN_SCOPE;
    writeTraceInfo(TraceType::CPU, sig, info, context);
}

}
