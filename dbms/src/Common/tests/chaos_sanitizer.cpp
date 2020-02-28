#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sched.h>

#include <thread>
#include <iostream>
#include <iomanip>
#include <random>

#include <common/sleep.h>
#include <common/getThreadId.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include <Common/Exception.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_DELETE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
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
        return cpu_time_period_ns != 0
            && (yield_probability > 0
                || (sleep_probability > 0 && chaos_sleep_time_ns > 0));
    }

private:
    uint64_t cpu_time_period_ns = 0;
    double yield_probability = 0;
    double sleep_probability = 0;
    double chaos_sleep_time_ns = 0;


    ChaosSanitizer()
    {
        handleError("Hello");

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
        initFromEnv(cpu_time_period_ns, "CHAOS_CPU_TIME_PERIOD_NS");
        if (!cpu_time_period_ns)
            return;
        initFromEnv(yield_probability, "CHAOS_YIELD_PROBABILITY");
        initFromEnv(sleep_probability, "CHAOS_SLEEP_PROBABILITY");
        initFromEnv(chaos_sleep_time_ns, "CHAOS_SLEEP_TIME_NS");
    }

    void handleError(const std::string & msg) const
    {
        WriteBufferFromFileDescriptor out(STDERR_FILENO, 4096);
        out.write(msg.data(), msg.size());
        out.write('\n');
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
        UInt32 period_rand = std::uniform_int_distribution<UInt32>(0, cpu_time_period_ns)(thread_local_rng);

        struct timeval interval{.tv_sec = long(cpu_time_period_ns / TIMER_PRECISION), .tv_usec = long(cpu_time_period_ns % TIMER_PRECISION)};
        struct timeval offset{.tv_sec = period_rand / TIMER_PRECISION, .tv_usec = period_rand % TIMER_PRECISION};

        struct itimerval timer = {.it_interval = interval, .it_value = offset};

        if (0 != setitimer(ITIMER_PROF, &timer, nullptr))
            throwFromErrno("Failed to create profiling timer", ErrorCodes::CANNOT_CREATE_TIMER);
    }

    static void signalHandler(int);
};

// /*thread_local*/ ChaosSanitizer chaos_sanitizer;

void ChaosSanitizer::signalHandler(int)
{
    std::uniform_real_distribution<> distribution(0.0, 1.0);

    auto & chaos_sanitizer = ChaosSanitizer::instance();

    if (chaos_sanitizer.yield_probability > 0)
    {
        double dice = distribution(thread_local_rng);
        if (dice < chaos_sanitizer.yield_probability)
            sched_yield();
    }

    if (chaos_sanitizer.sleep_probability > 0 && chaos_sanitizer.chaos_sleep_time_ns > 0)
    {
        double dice = distribution(thread_local_rng);
        if (dice < chaos_sanitizer.sleep_probability)
        {
            std::cerr << "Sleep in thread " << getThreadId() << "\n";

            sleepForNanoseconds(chaos_sanitizer.chaos_sleep_time_ns);
        }
    }
}

}


int main(int, char **)
{
    std::cerr << DB::ChaosSanitizer::instance().isEffective() << "\n";

    static constexpr size_t num_threads = 16;
    std::vector<std::thread> threads;

    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([=]
        {
            volatile size_t x = 0;
            while (++x)
//                if (x % (1 << 27) == 0)
//                    std::cerr << i << ".";
                ;
        });
    }

    for (auto & thread : threads)
        thread.join();

    return 0;
}
