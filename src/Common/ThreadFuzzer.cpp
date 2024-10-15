// NOLINTBEGIN(readability-inconsistent-declaration-parameter-name,readability-else-after-return)

#include <csignal>
#include <sys/time.h>
#if defined(OS_LINUX)
#   include <sys/sysinfo.h>
#endif
#include <sched.h>

#include <random>

#include <base/sleep.h>

#include <IO/ReadHelpers.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadFuzzer.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include "config.h" // USE_JEMALLOC


/// We will also wrap some thread synchronization functions to inject sleep/migration before or after.
#if defined(OS_LINUX) && !defined(THREAD_SANITIZER) && !defined(MEMORY_SANITIZER)
    #define THREAD_FUZZER_WRAP_PTHREAD 1
#else
    #define THREAD_FUZZER_WRAP_PTHREAD 0
#endif

#if THREAD_FUZZER_WRAP_PTHREAD
#    define FOR_EACH_WRAPPED_FUNCTION(M) \
        M(int, pthread_mutex_lock, pthread_mutex_t * arg) \
        M(int, pthread_mutex_unlock, pthread_mutex_t * arg)
#endif

#pragma clang diagnostic ignored "-Wreserved-identifier"

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
    if (needsSetup())
        setup();

    if (!isEffective())
    {
        /// It has no effect - disable it
        stop();
        return;
    }
}

template <typename T>
static void initFromEnv(T & what, const char * name)
{
    const char * env = getenv(name); // NOLINT(concurrency-mt-unsafe)
    if (!env)
        return;
    what = parse<T>(env);
}

template <typename T>
static void initFromEnv(std::atomic<T> & what, const char * name)
{
    const char * env = getenv(name); // NOLINT(concurrency-mt-unsafe)
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
        static std::atomic<double> NAME##_before_sleep_time_us_max = 0; \
\
        static std::atomic<double> NAME##_after_yield_probability = 0; \
        static std::atomic<double> NAME##_after_migrate_probability = 0; \
        static std::atomic<double> NAME##_after_sleep_probability = 0; \
        static std::atomic<double> NAME##_after_sleep_time_us_max = 0;

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
    initFromEnv(sleep_time_us_max, "THREAD_FUZZER_SLEEP_TIME_US_MAX");
    initFromEnv(explicit_sleep_probability, "THREAD_FUZZER_EXPLICIT_SLEEP_PROBABILITY");
    initFromEnv(explicit_memory_exception_probability, "THREAD_FUZZER_EXPLICIT_MEMORY_EXCEPTION_PROBABILITY");

#if THREAD_FUZZER_WRAP_PTHREAD
#    define INIT_WRAPPER_PARAMS(RET, NAME, ...) \
        initFromEnv(NAME##_before_yield_probability, "THREAD_FUZZER_" #NAME "_BEFORE_YIELD_PROBABILITY"); \
        initFromEnv(NAME##_before_migrate_probability, "THREAD_FUZZER_" #NAME "_BEFORE_MIGRATE_PROBABILITY"); \
        initFromEnv(NAME##_before_sleep_probability, "THREAD_FUZZER_" #NAME "_BEFORE_SLEEP_PROBABILITY"); \
        initFromEnv(NAME##_before_sleep_time_us_max, "THREAD_FUZZER_" #NAME "_BEFORE_SLEEP_TIME_US_MAX"); \
\
        initFromEnv(NAME##_after_yield_probability, "THREAD_FUZZER_" #NAME "_AFTER_YIELD_PROBABILITY"); \
        initFromEnv(NAME##_after_migrate_probability, "THREAD_FUZZER_" #NAME "_AFTER_MIGRATE_PROBABILITY"); \
        initFromEnv(NAME##_after_sleep_probability, "THREAD_FUZZER_" #NAME "_AFTER_SLEEP_PROBABILITY"); \
        initFromEnv(NAME##_after_sleep_time_us_max, "THREAD_FUZZER_" #NAME "_AFTER_SLEEP_TIME_US_MAX");
    FOR_EACH_WRAPPED_FUNCTION(INIT_WRAPPER_PARAMS)

#    undef INIT_WRAPPER_PARAMS
#endif
}


bool ThreadFuzzer::needsSetup() const
{
    return cpu_time_period_us != 0
        && (yield_probability > 0 || migrate_probability > 0 || (sleep_probability > 0 && sleep_time_us_max > 0));
}

bool ThreadFuzzer::isEffective() const
{
    if (needsSetup())
        return true;

#if THREAD_FUZZER_WRAP_PTHREAD
#    define CHECK_WRAPPER_PARAMS(RET, NAME, ...) \
        if (NAME##_before_yield_probability.load(std::memory_order_relaxed) > 0.0) \
            return true; \
        if (NAME##_before_migrate_probability.load(std::memory_order_relaxed) > 0.0) \
            return true; \
        if (NAME##_before_sleep_probability.load(std::memory_order_relaxed) > 0.0) \
            return true; \
        if (NAME##_before_sleep_time_us_max.load(std::memory_order_relaxed) > 0.0) \
            return true; \
\
        if (NAME##_after_yield_probability.load(std::memory_order_relaxed) > 0.0) \
            return true; \
        if (NAME##_after_migrate_probability.load(std::memory_order_relaxed) > 0.0) \
            return true; \
        if (NAME##_after_sleep_probability.load(std::memory_order_relaxed) > 0.0) \
            return true; \
        if (NAME##_after_sleep_time_us_max.load(std::memory_order_relaxed) > 0.0) \
            return true;

    FOR_EACH_WRAPPED_FUNCTION(CHECK_WRAPPER_PARAMS)

#    undef INIT_WRAPPER_PARAMS
#endif

    if (explicit_sleep_probability > 0 && sleep_time_us_max > 0)
        return true;

    if (explicit_memory_exception_probability > 0)
        return true;

    return false;
}

void ThreadFuzzer::stop()
{
    started.store(false, std::memory_order_relaxed);
}

void ThreadFuzzer::start()
{
    if (!instance().isEffective())
        return;
    started.store(true, std::memory_order_relaxed);
}

bool ThreadFuzzer::isStarted()
{
    return started.load(std::memory_order_relaxed);
}

static void injectionImpl(
    double yield_probability,
    double migrate_probability,
    double sleep_probability,
    double sleep_time_us_max)
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

    if (sleep_probability > 0 && sleep_time_us_max > 0.001 && std::bernoulli_distribution(sleep_probability)(thread_local_rng))
    {
        sleepForNanoseconds((thread_local_rng() % static_cast<uint64_t>(sleep_time_us_max * 1000)));
    }
}

static ALWAYS_INLINE void injection(
    double yield_probability,
    double migrate_probability,
    double sleep_probability,
    double sleep_time_us_max)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    if (!ThreadFuzzer::isStarted())
        return;

    injectionImpl(yield_probability, migrate_probability, sleep_probability, sleep_time_us_max);
}

void ThreadFuzzer::maybeInjectSleep()
{
    auto & fuzzer = ThreadFuzzer::instance();
    injection(fuzzer.yield_probability, fuzzer.migrate_probability, fuzzer.explicit_sleep_probability, fuzzer.sleep_time_us_max);
}

/// Sometimes maybeInjectSleep() is not enough and we need to inject an exception.
/// The most suitable exception for this purpose is MEMORY_LIMIT_EXCEEDED: it can be thrown almost from everywhere.
/// NOTE We also have a query setting fault_probability, but it does not work for background operations (maybe we should fix it).
void ThreadFuzzer::maybeInjectMemoryLimitException()
{
    auto & fuzzer = ThreadFuzzer::instance();
    if (fuzzer.explicit_memory_exception_probability <= 0.0)
        return;
    std::bernoulli_distribution fault(fuzzer.explicit_memory_exception_probability);
    if (fault(thread_local_rng))
        CurrentMemoryTracker::injectFault();
}

void ThreadFuzzer::signalHandler(int)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno;
    auto & fuzzer = ThreadFuzzer::instance();
    injection(fuzzer.yield_probability, fuzzer.migrate_probability, fuzzer.sleep_probability, fuzzer.sleep_time_us_max);
    errno = saved_errno;
}

void ThreadFuzzer::setup() const
{
    struct sigaction sa{};
    sa.sa_handler = signalHandler;
    sa.sa_flags = SA_RESTART;

#if defined(OS_LINUX)
    if (sigemptyset(&sa.sa_mask))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to clean signal mask for thread fuzzer");

    if (sigaddset(&sa.sa_mask, SIGPROF))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Failed to add signal to mask for thread fuzzer");
#else
    // the two following functions always return 0 under mac
    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGPROF);
#endif

    if (sigaction(SIGPROF, &sa, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Failed to setup signal handler for thread fuzzer");

    static constexpr UInt32 timer_precision = 1000000;

    struct timeval interval;
    interval.tv_sec = cpu_time_period_us / timer_precision;
    interval.tv_usec = cpu_time_period_us % timer_precision;

    struct itimerval timer = {.it_interval = interval, .it_value = interval};

    if (0 != setitimer(ITIMER_PROF, &timer, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Failed to create profiling timer");
}


#if THREAD_FUZZER_WRAP_PTHREAD
#define INJECTION_BEFORE(NAME) \
    injectionImpl(                                                         \
        NAME##_before_yield_probability.load(std::memory_order_relaxed),   \
        NAME##_before_migrate_probability.load(std::memory_order_relaxed), \
        NAME##_before_sleep_probability.load(std::memory_order_relaxed),   \
        NAME##_before_sleep_time_us_max.load(std::memory_order_relaxed));
#define INJECTION_AFTER(NAME) \
    injectionImpl(                                                         \
        NAME##_after_yield_probability.load(std::memory_order_relaxed),    \
        NAME##_after_migrate_probability.load(std::memory_order_relaxed),  \
        NAME##_after_sleep_probability.load(std::memory_order_relaxed),    \
        NAME##_after_sleep_time_us_max.load(std::memory_order_relaxed));

/// ThreadFuzzer intercepts pthread_mutex_lock()/pthread_mutex_unlock().
///
/// glibc/musl exports internal symbol
/// (__pthread_mutex_lock/__pthread_mutex_unlock) that can be used instead of
/// obtaining real symbol with dlsym(RTLD_NEXT).
///
/// But, starting from glibc 2.34 there are no internal symbols without
/// version, so not __pthread_mutex_lock but __pthread_mutex_lock@2.2.5 (see
/// GLIBC_COMPAT_SYMBOL macro).
///
/// While ASan intercepts those symbols too (using RTLD_NEXT), and not only
/// public (pthread_mutex_{un,lock}, but also internal
/// (__pthread_mutex_{un,}lock).
///
/// However, since glibc 2.36, dlsym(RTLD_NEXT, "__pthread_mutex_lock") returns
/// NULL, because starting from 2.36 it does not return internal symbols with
/// RTLD_NEXT (see [1] and [2]).
///
///   [1]: https://sourceware.org/git/?p=glibc.git;a=commit;h=efa7936e4c91b1c260d03614bb26858fbb8a0204
///   [2]: https://gist.github.com/azat/3b5f2ae6011bef2ae86392cea7789eb7
///
/// And this, creates a problem for ThreadFuzzer, since it cannot use internal
/// symbol anymore (__pthread_mutex_lock), because it is intercepted by ASan,
/// which will call NULL.
///
/// This issue had been fixed for clang 16 [3], but it hadn't been released yet.
///
///   [3]: https://reviews.llvm.org/D140957
///
/// So to fix this, we will use dlsym(RTLD_NEXT) for the ASan build.
///
/// Note, that we cannot use it for release builds, since:
/// - glibc < 2.36 has allocation in dlsym()
/// - release build uses jemalloc
/// - jemalloc has mutexes for allocations
/// And all of this will lead to endless recursion here (note, that it wasn't
/// be a problem if only one of functions had been intercepted, since jemalloc
/// has a guard to not initialize multiple times, but because both intercepted,
/// the endless recursion takes place, you can find an example in [4]).
///
///   [4]: https://gist.github.com/azat/588d9c72c1e70fc13ebe113197883aa2

/// Starting from glibc 2.34 there are no internal symbols without version,
/// so not __pthread_mutex_lock but __pthread_mutex_lock@2.2.5
#if defined(OS_LINUX) and !defined(USE_MUSL) and !defined(__loongarch64)
    /// You can get version from glibc/sysdeps/unix/sysv/linux/$ARCH/$BITS_OR_BYTE_ORDER/libc.abilist
    #if defined(__amd64__)
    #    define GLIBC_SYMVER "GLIBC_2.2.5"
    #elif defined(__aarch64__)
    #    define GLIBC_SYMVER "GLIBC_2.17"
    #elif defined(__riscv) && (__riscv_xlen == 64)
    #    define GLIBC_SYMVER "GLIBC_2.27"
    #elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    #    define GLIBC_SYMVER "GLIBC_2.17"
    #elif (defined(__S390X__) || defined(__s390x__))
    #    define GLIBC_SYMVER "GLIBC_2.2"
    #else
    #    error Your platform is not supported.
    #endif

    #define GLIBC_COMPAT_SYMBOL(func) __asm__(".symver " #func "," #func "@" GLIBC_SYMVER);

    GLIBC_COMPAT_SYMBOL(__pthread_mutex_unlock)
    GLIBC_COMPAT_SYMBOL(__pthread_mutex_lock)
#endif

/// The loongarch64's glibc_version is 2.36
#if defined(ADDRESS_SANITIZER) || defined(__loongarch64)
#if USE_JEMALLOC
#error "ASan cannot be used with jemalloc"
#endif
#if defined(USE_MUSL)
#error "ASan cannot be used with musl"
#endif
#include <dlfcn.h>

static void * getFunctionAddress(const char * name)
{
    void * address = dlsym(RTLD_NEXT, name);
    chassert(address && "Cannot obtain function address");
    return address;
}
#define MAKE_WRAPPER_USING_DLSYM(RET, NAME, ...)                                  \
    static constinit RET(*real_##NAME)(__VA_ARGS__) = nullptr;                    \
    extern "C" RET NAME(__VA_ARGS__)                                              \
    {                                                                             \
        bool thread_fuzzer_enabled = ThreadFuzzer::isStarted();                   \
        if (thread_fuzzer_enabled)                                                \
            INJECTION_BEFORE(NAME);                                               \
        if (unlikely(!real_##NAME)) {                                             \
            real_##NAME =                                                         \
                reinterpret_cast<RET(*)(__VA_ARGS__)>(getFunctionAddress(#NAME)); \
        }                                                                         \
        auto && ret{real_##NAME(arg)};                                            \
        if (thread_fuzzer_enabled)                                                \
            INJECTION_AFTER(NAME);                                                \
        return ret;                                                               \
    }
FOR_EACH_WRAPPED_FUNCTION(MAKE_WRAPPER_USING_DLSYM)
#undef MAKE_WRAPPER_USING_DLSYM
#else
#define MAKE_WRAPPER_USING_INTERNAL_SYMBOLS(RET, NAME, ...) \
    extern "C" RET __##NAME(__VA_ARGS__);                   \
    extern "C" RET NAME(__VA_ARGS__)                        \
    {                                                       \
        if (!ThreadFuzzer::isStarted())                     \
        {                                                   \
            return __##NAME(arg);                           \
        }                                                   \
        else                                                \
        {                                                   \
            INJECTION_BEFORE(NAME);                         \
            auto && ret{__##NAME(arg)};                     \
            INJECTION_AFTER(NAME);                          \
            return ret;                                     \
        }                                                   \
    }
FOR_EACH_WRAPPED_FUNCTION(MAKE_WRAPPER_USING_INTERNAL_SYMBOLS)
#undef MAKE_WRAPPER_USING_INTERNAL_SYMBOLS
#endif

#endif
}

// NOLINTEND(readability-inconsistent-declaration-parameter-name,readability-else-after-return)
