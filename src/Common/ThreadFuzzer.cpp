#include <csignal>
#include <sys/time.h>
#if defined(OS_LINUX)
#   include <sys/sysinfo.h>
#endif
#include <sched.h>

#include <random>

#include <base/sleep.h>

#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>

#include <Common/Exception.h>
#include <Common/thread_local_rng.h>

#include <Common/ThreadFuzzer.h>


/// We will also wrap some thread synchronization functions to inject sleep/migration before or after.
#if defined(OS_LINUX) && !defined(THREAD_SANITIZER) && !defined(MEMORY_SANITIZER)
    #define THREAD_FUZZER_WRAP_PTHREAD 1
#else
    #define THREAD_FUZZER_WRAP_PTHREAD 0
#endif

/// Starting from glibc 2.34 there are no internal symbols without version,
/// so not __pthread_mutex_lock but __pthread_mutex_lock@2.2.5
#if defined(OS_LINUX) and !defined(USE_MUSL)
    /// You can get version from glibc/sysdeps/unix/sysv/linux/$ARCH/$BITS_OR_BYTE_ORDER/libc.abilist
    #if defined(__amd64__)
    #    define GLIBC_SYMVER "GLIBC_2.2.5"
    #elif defined(__aarch64__)
    #    define GLIBC_SYMVER "GLIBC_2.17"
    #elif defined(__riscv) && (__riscv_xlen == 64)
    #    define GLIBC_SYMVER "GLIBC_2.27"
    #elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    #    define GLIBC_SYMVER "GLIBC_2.17"
    #else
    #    error Your platform is not supported.
    #endif

    #define GLIBC_COMPAT_SYMBOL(func) __asm__(".symver " #func "," #func "@" GLIBC_SYMVER);

    GLIBC_COMPAT_SYMBOL(__pthread_mutex_unlock)
    GLIBC_COMPAT_SYMBOL(__pthread_mutex_lock)
#endif

#if THREAD_FUZZER_WRAP_PTHREAD
#    define FOR_EACH_WRAPPED_FUNCTION(M) \
        M(int, pthread_mutex_lock, pthread_mutex_t * arg) \
        M(int, pthread_mutex_unlock, pthread_mutex_t * arg)
#endif

#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

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

template <typename T>
static void initFromEnv(std::atomic<T> & what, const char * name)
{
    const char * env = getenv(name);
    if (!env)
        return;
    what.store(parse<T>(env), std::memory_order_relaxed);
}


static std::atomic<int> num_cpus = 0;

#if THREAD_FUZZER_WRAP_PTHREAD
#    define DEFINE_WRAPPER_PARAMS(RET, NAME, ...) \
        static std::atomic<double> NAME##_before_yield_probability = 0; \
        static std::atomic<double> NAME##_before_migrate_probability = 0; \
        static std::atomic<double> NAME##_before_sleep_probability = 0; \
        static std::atomic<double> NAME##_before_sleep_time_us = 0; \
\
        static std::atomic<double> NAME##_after_yield_probability = 0; \
        static std::atomic<double> NAME##_after_migrate_probability = 0; \
        static std::atomic<double> NAME##_after_sleep_probability = 0; \
        static std::atomic<double> NAME##_after_sleep_time_us = 0;

FOR_EACH_WRAPPED_FUNCTION(DEFINE_WRAPPER_PARAMS)

#    undef DEFINE_WRAPPER_PARAMS
#endif

void ThreadFuzzer::initConfiguration()
{
#if defined(OS_LINUX)
    num_cpus.store(get_nprocs(), std::memory_order_relaxed);
#else
    (void)num_cpus;
#endif

    initFromEnv(cpu_time_period_us, "THREAD_FUZZER_CPU_TIME_PERIOD_US");
    initFromEnv(yield_probability, "THREAD_FUZZER_YIELD_PROBABILITY");
    initFromEnv(migrate_probability, "THREAD_FUZZER_MIGRATE_PROBABILITY");
    initFromEnv(sleep_probability, "THREAD_FUZZER_SLEEP_PROBABILITY");
    initFromEnv(sleep_time_us, "THREAD_FUZZER_SLEEP_TIME_US");

#if THREAD_FUZZER_WRAP_PTHREAD
#    define INIT_WRAPPER_PARAMS(RET, NAME, ...) \
        initFromEnv(NAME##_before_yield_probability, "THREAD_FUZZER_" #NAME "_BEFORE_YIELD_PROBABILITY"); \
        initFromEnv(NAME##_before_migrate_probability, "THREAD_FUZZER_" #NAME "_BEFORE_MIGRATE_PROBABILITY"); \
        initFromEnv(NAME##_before_sleep_probability, "THREAD_FUZZER_" #NAME "_BEFORE_SLEEP_PROBABILITY"); \
        initFromEnv(NAME##_before_sleep_time_us, "THREAD_FUZZER_" #NAME "_BEFORE_SLEEP_TIME_US"); \
\
        initFromEnv(NAME##_after_yield_probability, "THREAD_FUZZER_" #NAME "_AFTER_YIELD_PROBABILITY"); \
        initFromEnv(NAME##_after_migrate_probability, "THREAD_FUZZER_" #NAME "_AFTER_MIGRATE_PROBABILITY"); \
        initFromEnv(NAME##_after_sleep_probability, "THREAD_FUZZER_" #NAME "_AFTER_SLEEP_PROBABILITY"); \
        initFromEnv(NAME##_after_sleep_time_us, "THREAD_FUZZER_" #NAME "_AFTER_SLEEP_TIME_US");

    FOR_EACH_WRAPPED_FUNCTION(INIT_WRAPPER_PARAMS)

#    undef INIT_WRAPPER_PARAMS
#endif
}


bool ThreadFuzzer::isEffective() const
{
    if (!isStarted())
        return false;

#if THREAD_FUZZER_WRAP_PTHREAD
#    define CHECK_WRAPPER_PARAMS(RET, NAME, ...) \
        if (NAME##_before_yield_probability.load(std::memory_order_relaxed)) \
            return true; \
        if (NAME##_before_migrate_probability.load(std::memory_order_relaxed)) \
            return true; \
        if (NAME##_before_sleep_probability.load(std::memory_order_relaxed)) \
            return true; \
        if (NAME##_before_sleep_time_us.load(std::memory_order_relaxed)) \
            return true; \
\
        if (NAME##_after_yield_probability.load(std::memory_order_relaxed)) \
            return true; \
        if (NAME##_after_migrate_probability.load(std::memory_order_relaxed)) \
            return true; \
        if (NAME##_after_sleep_probability.load(std::memory_order_relaxed)) \
            return true; \
        if (NAME##_after_sleep_time_us.load(std::memory_order_relaxed)) \
            return true;

    FOR_EACH_WRAPPED_FUNCTION(CHECK_WRAPPER_PARAMS)

#    undef INIT_WRAPPER_PARAMS
#endif

    return cpu_time_period_us != 0
        && (yield_probability > 0
            || migrate_probability > 0
            || (sleep_probability > 0 && sleep_time_us > 0));
}

void ThreadFuzzer::stop()
{
    started.store(false, std::memory_order_relaxed);
}

void ThreadFuzzer::start()
{
    started.store(true, std::memory_order_relaxed);
}

bool ThreadFuzzer::isStarted()
{
    return started.load(std::memory_order_relaxed);
}

static void injection(
    double yield_probability,
    double migrate_probability,
    double sleep_probability,
    double sleep_time_us [[maybe_unused]])
{
    DENY_ALLOCATIONS_IN_SCOPE;
    if (!ThreadFuzzer::isStarted())
        return;

    if (yield_probability > 0
        && std::bernoulli_distribution(yield_probability)(thread_local_rng))
    {
        sched_yield();
    }

#if defined(OS_LINUX)
    int num_cpus_loaded = num_cpus.load(std::memory_order_relaxed);
    if (num_cpus_loaded > 0
        && migrate_probability > 0
        && std::bernoulli_distribution(migrate_probability)(thread_local_rng))
    {
        int migrate_to = std::uniform_int_distribution<>(0, num_cpus_loaded - 1)(thread_local_rng);

        cpu_set_t set{};
        CPU_ZERO(&set);
        CPU_SET(migrate_to, &set);

        (void)sched_setaffinity(0, sizeof(set), &set);
    }
#else
    UNUSED(migrate_probability);
#endif

    if (sleep_probability > 0
        && sleep_time_us > 0
        && std::bernoulli_distribution(sleep_probability)(thread_local_rng))
    {
        sleepForNanoseconds(sleep_time_us * 1000);
    }
}


void ThreadFuzzer::signalHandler(int)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno;

    auto & fuzzer = ThreadFuzzer::instance();
    injection(fuzzer.yield_probability, fuzzer.migrate_probability, fuzzer.sleep_probability, fuzzer.sleep_time_us);

    errno = saved_errno;
}

void ThreadFuzzer::setup() const
{
    struct sigaction sa{};
    sa.sa_handler = signalHandler;
    sa.sa_flags = SA_RESTART;

#if defined(OS_LINUX)
    if (sigemptyset(&sa.sa_mask))
        throwFromErrno("Failed to clean signal mask for thread fuzzer", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaddset(&sa.sa_mask, SIGPROF))
        throwFromErrno("Failed to add signal to mask for thread fuzzer", ErrorCodes::CANNOT_MANIPULATE_SIGSET);
#else
    // the two following functions always return 0 under mac
    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGPROF);
#endif

    if (sigaction(SIGPROF, &sa, nullptr))
        throwFromErrno("Failed to setup signal handler for thread fuzzer", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

    static constexpr UInt32 timer_precision = 1000000;

    struct timeval interval;
    interval.tv_sec = cpu_time_period_us / timer_precision;
    interval.tv_usec = cpu_time_period_us % timer_precision;

    struct itimerval timer = {.it_interval = interval, .it_value = interval};

    if (0 != setitimer(ITIMER_PROF, &timer, nullptr))
        throwFromErrno("Failed to create profiling timer", ErrorCodes::CANNOT_CREATE_TIMER);
}


/// We expect that for every function like pthread_mutex_lock there is the same function with two underscores prefix.
/// NOTE We cannot use dlsym(... RTLD_NEXT), because it will call pthread_mutex_lock and it will lead to infinite recursion.

#if THREAD_FUZZER_WRAP_PTHREAD
#    define MAKE_WRAPPER(RET, NAME, ...) \
        extern "C" RET __##NAME(__VA_ARGS__); /* NOLINT */ \
        extern "C" RET NAME(__VA_ARGS__) /* NOLINT */ \
        { \
            injection( \
                NAME##_before_yield_probability.load(std::memory_order_relaxed), \
                NAME##_before_migrate_probability.load(std::memory_order_relaxed), \
                NAME##_before_sleep_probability.load(std::memory_order_relaxed), \
                NAME##_before_sleep_time_us.load(std::memory_order_relaxed)); \
\
            auto && ret{__##NAME(arg)}; \
\
            injection( \
                NAME##_after_yield_probability.load(std::memory_order_relaxed), \
                NAME##_after_migrate_probability.load(std::memory_order_relaxed), \
                NAME##_after_sleep_probability.load(std::memory_order_relaxed), \
                NAME##_after_sleep_time_us.load(std::memory_order_relaxed)); \
\
            return ret; \
        }

FOR_EACH_WRAPPED_FUNCTION(MAKE_WRAPPER)

#    undef MAKE_WRAPPER
#endif
}
