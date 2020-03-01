#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/sysinfo.h>
#include <sched.h>

#include <thread>
#include <iostream>
#include <iomanip>
#include <random>

#include <common/sleep.h>
#include <common/getThreadId.h>

#include <IO/ReadHelpers.h>

#include <Common/Exception.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_CREATE_TIMER;
}


class ChaosSanitizer
{
public:
    static ChaosSanitizer & instance()
    {
        static ChaosSanitizer res;
        return res;
    }

    bool isEffective() const
    {
        return cpu_time_period_us != 0
            && (yield_probability > 0
                || migrate_probability > 0
                || (sleep_probability > 0 && chaos_sleep_time_us > 0));
    }

private:
    uint64_t cpu_time_period_us = 0;
    double yield_probability = 0;
    double migrate_probability = 0;
    double sleep_probability = 0;
    double chaos_sleep_time_us = 0;

    int num_cpus = 0;


    ChaosSanitizer()
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

    void initConfiguration()
    {
        num_cpus = get_nprocs();

        initFromEnv(cpu_time_period_us, "CHAOS_CPU_TIME_PERIOD_US");
        if (!cpu_time_period_us)
            return;
        initFromEnv(yield_probability, "CHAOS_YIELD_PROBABILITY");
        initFromEnv(migrate_probability, "CHAOS_MIGRATE_PROBABILITY");
        initFromEnv(sleep_probability, "CHAOS_SLEEP_PROBABILITY");
        initFromEnv(chaos_sleep_time_us, "CHAOS_SLEEP_TIME_US");
    }

    void message(const char * msg) const
    {
        (void)write(STDERR_FILENO, msg, strlen(msg));
        (void)write(STDERR_FILENO, "\n", 1);
    }

    void setup()
    {
        struct sigaction sa{};
        sa.sa_handler = signalHandler;
        sa.sa_flags = SA_RESTART;

        if (sigemptyset(&sa.sa_mask))
            throwFromErrno("Failed to clean signal mask for chaos_sanitizer", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

        if (sigaddset(&sa.sa_mask, SIGPROF))
            throwFromErrno("Failed to add signal to mask for chaos_sanitizer", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

        if (sigaction(SIGPROF, &sa, nullptr))
            throwFromErrno("Failed to setup signal handler for chaos_sanitizer", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

        static constexpr UInt32 TIMER_PRECISION = 1e6;

        /// Randomize offset as uniform random value from 0 to period - 1.
        /// It will allow to sample short queries even if timer period is large.
        /// (For example, with period of 1 second, query with 50 ms duration will be sampled with 1 / 20 probability).
        /// It also helps to avoid interference (moire).
        UInt32 period_rand = std::uniform_int_distribution<UInt32>(0, cpu_time_period_us)(thread_local_rng);

        struct timeval interval{.tv_sec = long(cpu_time_period_us / TIMER_PRECISION), .tv_usec = long(cpu_time_period_us % TIMER_PRECISION)};
        struct timeval offset{.tv_sec = period_rand / TIMER_PRECISION, .tv_usec = period_rand % TIMER_PRECISION};

        struct itimerval timer = {.it_interval = interval, .it_value = offset};

        if (0 != setitimer(ITIMER_PROF, &timer, nullptr))
            throwFromErrno("Failed to create profiling timer", ErrorCodes::CANNOT_CREATE_TIMER);
    }

    static void signalHandler(int);
};


void ChaosSanitizer::signalHandler(int)
{
    auto saved_errno = errno;

    // std::cerr << getThreadId() << "\n";

    auto & chaos_sanitizer = ChaosSanitizer::instance();

    if (chaos_sanitizer.yield_probability > 0
        && std::bernoulli_distribution(chaos_sanitizer.yield_probability)(thread_local_rng))
    {
        sched_yield();
    }

    if (chaos_sanitizer.migrate_probability > 0
        && std::bernoulli_distribution(chaos_sanitizer.migrate_probability)(thread_local_rng))
    {
        int migrate_to = std::uniform_int_distribution<>(0, chaos_sanitizer.num_cpus - 1)(thread_local_rng);

        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(migrate_to, &set);

        (void)sched_setaffinity(0, sizeof(set), &set);
    }

    if (chaos_sanitizer.sleep_probability > 0
        && chaos_sanitizer.chaos_sleep_time_us > 0
        && std::bernoulli_distribution(chaos_sanitizer.sleep_probability)(thread_local_rng))
    {
        // std::cerr << "Sleep in thread " << getThreadId() << "\n";
        sleepForNanoseconds(chaos_sanitizer.chaos_sleep_time_us * 1000);
    }

    errno = saved_errno;
}

}


int main(int argc, char ** argv)
{
    const size_t num_iterations = argc >= 2 ? DB::parse<size_t>(argv[1]) : 1000000000;
    const size_t num_threads = argc >= 3 ? DB::parse<size_t>(argv[2]) : 16;

    std::cerr << DB::ChaosSanitizer::instance().isEffective() << "\n";

    std::vector<std::thread> threads;

    std::atomic<size_t> counter = 0;

    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            for (size_t j = 0; j < num_iterations; ++j)
                counter.store(counter.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);  /// Intentionally wrong.
        });
    }

    for (auto & thread : threads)
        thread.join();

    std::cerr << "Result: " << counter << "\n";

    return 0;
}
