#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/sysinfo.h>
#include <sched.h>

#include <random>

#include <common/sleep.h>
#include <common/getThreadId.h>

#include <IO/ReadHelpers.h>

#include <Common/Exception.h>
#include <Common/thread_local_rng.h>

#include <Common/ThreadFuzzer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_CREATE_TIMER;
}


ThreadFuzzer::ThreadFuzzer()
{
    initConfiguration();
    if (!isEffective())
        return;
    setup();
}


template <typename T>
static void initFromEnv(T & what, const char * name)
{
    const char * env = getenv(name);
    if (!env)
        return;
    what = parse<T>(env);
}

void ThreadFuzzer::initConfiguration()
{
    num_cpus = get_nprocs();

    initFromEnv(cpu_time_period_us, "THREAD_FUZZER_CPU_TIME_PERIOD_US");
    if (!cpu_time_period_us)
        return;
    initFromEnv(yield_probability, "THREAD_FUZZER_YIELD_PROBABILITY");
    initFromEnv(migrate_probability, "THREAD_FUZZER_MIGRATE_PROBABILITY");
    initFromEnv(sleep_probability, "THREAD_FUZZER_SLEEP_PROBABILITY");
    initFromEnv(chaos_sleep_time_us, "THREAD_FUZZER_SLEEP_TIME_US");
}

void ThreadFuzzer::signalHandler(int)
{
    auto saved_errno = errno;

    auto & fuzzer = ThreadFuzzer::instance();

    if (fuzzer.yield_probability > 0
        && std::bernoulli_distribution(fuzzer.yield_probability)(thread_local_rng))
    {
        sched_yield();
    }

    if (fuzzer.migrate_probability > 0
        && std::bernoulli_distribution(fuzzer.migrate_probability)(thread_local_rng))
    {
        int migrate_to = std::uniform_int_distribution<>(0, fuzzer.num_cpus - 1)(thread_local_rng);

        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(migrate_to, &set);

        (void)sched_setaffinity(0, sizeof(set), &set);
    }

    if (fuzzer.sleep_probability > 0
        && fuzzer.chaos_sleep_time_us > 0
        && std::bernoulli_distribution(fuzzer.sleep_probability)(thread_local_rng))
    {
        sleepForNanoseconds(fuzzer.chaos_sleep_time_us * 1000);
    }

    errno = saved_errno;
}

void ThreadFuzzer::setup()
{
    struct sigaction sa{};
    sa.sa_handler = signalHandler;
    sa.sa_flags = SA_RESTART;

    if (sigemptyset(&sa.sa_mask))
        throwFromErrno("Failed to clean signal mask for thread fuzzer", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaddset(&sa.sa_mask, SIGPROF))
        throwFromErrno("Failed to add signal to mask for thread fuzzer", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaction(SIGPROF, &sa, nullptr))
        throwFromErrno("Failed to setup signal handler for thread fuzzer", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

    static constexpr UInt32 TIMER_PRECISION = 1000000;

    struct timeval interval{.tv_sec = long(cpu_time_period_us / TIMER_PRECISION), .tv_usec = long(cpu_time_period_us % TIMER_PRECISION)};
    struct itimerval timer = {.it_interval = interval, .it_value = interval};

    if (0 != setitimer(ITIMER_PROF, &timer, nullptr))
        throwFromErrno("Failed to create profiling timer", ErrorCodes::CANNOT_CREATE_TIMER);
}


}
