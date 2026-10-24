#include <allocator/Init.h>

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentOps.h>
#include <allocator/FixedPoint.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Mallctl.h>
#include <allocator/MallocConf.h>
#include <allocator/Mutex.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/ProfilingHooks.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>
#include <allocator/Spin.h>
#include <allocator/Stats.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#if defined(__FreeBSD__)
#include <pthread_np.h>
#include <sys/cpuset.h>
#endif

namespace jemalloc
{

namespace
{

/// When `malloc_slow` is true, set the corresponding bits for sanity check. jemalloc: flag_opt_* (anonymous enum)
enum : uint8_t
{
    flag_option_junk_alloc = (1U),
    flag_option_junk_free = (1U << 1),
    flag_option_zero = (1U << 2),
    flag_option_utrace = (1U << 3),
    flag_option_abort_on_out_of_memory = (1U << 4),
};

/// jemalloc: malloc_slow_flags (static)
constinit uint8_t malloc_slow_flags = 0;

/// Used to let the initializing thread recursively allocate. With `JEMALLOC_THREADED_INIT` it is the initializing
/// thread (0: none), otherwise a flag. jemalloc: malloc_initializer (static), NO_INITIALIZER, INITIALIZER
using MallocInitializer = std::conditional_t<config::threaded_init, pthread_t, bool>;
constinit MallocInitializer malloc_initializer{};

/// jemalloc: INITIALIZER
MallocInitializer initializerSelf()
{
    if constexpr (config::threaded_init)
        return pthread_self();
    else
        return true;
}

/// A template, so that the discarded branch (`pthread_equal` of a `bool`) is not checked.
template <typename Initializer>
bool isInitializerImpl(Initializer initializer)
{
    if constexpr (std::is_same_v<Initializer, bool>)
        return initializer;
    else
        return pthread_equal(initializer, pthread_self());
}

/// jemalloc: IS_INITIALIZER
bool isInitializer()
{
    return isInitializerImpl(malloc_initializer);
}

/// jemalloc: malloc_initializer != NO_INITIALIZER
bool hasInitializer()
{
    if constexpr (config::threaded_init)
        return malloc_initializer != MallocInitializer{};
    else
        return malloc_initializer;
}

/// Used to avoid initialization races. jemalloc: init_lock (static, MALLOC_MUTEX_INITIALIZER, WITNESS_RANK_INIT)
constinit Mutex init_lock;

/// jemalloc: stats_print_atexit
void statsPrintAtexit()
{
    if constexpr (config::stats)
    {
        ThreadState * thread_state = ThreadState::threadStateFetch();

        /// Merge stats from extant threads. This is racy, since individual threads do not lock when recording tcache
        /// stats events. As a consequence, the final stats may be slightly out of date by the time they are reported,
        /// if other threads continue to allocate.
        for (unsigned i = 0, num_arenas = numArenasTotalGet(); i < num_arenas; ++i)
        {
            Arena * arena = arenaGet(thread_state, i, false);
            if (arena != nullptr)
            {
                arena->thread_cache_list_mutex.lock(thread_state);
                arena->thread_cache_list.forEach([&](ThreadCacheSlow * thread_cache_slow)
                                                 { threadCacheStatsMerge(thread_state, thread_cache_slow->thread_cache, arena); });
                arena->thread_cache_list_mutex.unlock(thread_state);
            }
        }
    }
    mallocStatsPrint(nullptr, nullptr, options.stats_print_options);
}

/// The affinity mask of the process (the return value of the system call is not checked, like in jemalloc).
#if defined(__FreeBSD__)
using CPUSet = cpuset_t;
#elif defined(__linux__)
using CPUSet = cpu_set_t;
#endif

#if defined(__linux__) || defined(__FreeBSD__)
long affinityCPUCount()
{
    CPUSet set;
    if constexpr (config::have_sched_setaffinity)
        sched_getaffinity(0, sizeof(set), &set);
    else
        pthread_getaffinity_np(pthread_self(), sizeof(set), &set);
    return CPU_COUNT(&set);
}
#endif

}

bool mallocIsInitializer()
{
    return isInitializer();
}

/// jemalloc: malloc_ncpus
unsigned mallocNumCPUs()
{
    long result;
#if defined(__linux__) || defined(__FreeBSD__)
    /// glibc's `sysconf` uses `isspace`. glibc allocates for the first time *before* setting up the `isspace` tables.
    /// Therefore we need a different method to get the number of CPUs. The affinity approach is also preferred when
    /// only a subset of CPUs is available, to avoid using more arenas than necessary.
    result = affinityCPUCount();
#else
    result = sysconf(_SC_NPROCESSORS_ONLN);
#endif
    return (result == -1) ? 1 : unsigned(result);
}

/// Ensure that the number of CPUs is deterministic, i.e. it is the same based on: the affinity mask,
/// `_SC_NPROCESSORS_ONLN`, `_SC_NPROCESSORS_CONF`, since otherwise tricky things are possible with percpu arenas in use.
/// jemalloc: malloc_cpu_count_is_deterministic
bool mallocCPUCountIsDeterministic()
{
    long cpu_online = sysconf(_SC_NPROCESSORS_ONLN);
    long cpu_configuration = sysconf(_SC_NPROCESSORS_CONF);
    if (cpu_online != cpu_configuration)
        return false;
#if defined(__linux__) || defined(__FreeBSD__)
    long cpu_affinity = affinityCPUCount();
    if (cpu_affinity != cpu_configuration)
        return false;
#endif
    return true;
}

namespace
{

/// Combine the runtime options into `malloc_slow` for the fast path. Called after processing all the options.
/// jemalloc: malloc_slow_flag_init
void mallocSlowFlagInit()
{
    malloc_slow_flags |= (options.junk_alloc ? flag_option_junk_alloc : 0) | (options.junk_free ? flag_option_junk_free : 0)
        | (options.zero ? flag_option_zero : 0) | (options.utrace ? flag_option_utrace : 0)
        | (options.abort_on_out_of_memory ? flag_option_abort_on_out_of_memory : 0);

    malloc_slow = (malloc_slow_flags != 0);
}

/// jemalloc: malloc_init_hard_needed
bool mallocInitHardNeeded()
{
    if (mallocInitialized() || (isInitializer() && malloc_init_state == malloc_init_recursible))
    {
        /// Another thread initialized the allocator before this one acquired `init_lock`, or this thread is the
        /// initializing thread, and it is recursively allocating.
        return false;
    }
    if constexpr (config::threaded_init)
    {
        if (hasInitializer() && !isInitializer())
        {
            /// Busy-wait until the initializing thread completes.
            Spin spinner;
            do
            {
                init_lock.unlock(nullptr);
                spinner.adaptive();
                init_lock.lock(nullptr);
            } while (!mallocInitialized());
            return false;
        }
    }
    return true;
}

/// jemalloc: malloc_init_hard_a0_locked
bool mallocInitHardA0Locked()
{
    malloc_initializer = initializerSelf();

    SizeClassData size_class_data{};

    /// Ordering here is somewhat tricky; we need `sizeClassBoot` first, since that determines what the size classes will be,
    /// and then `mallocConfInit`, since any slab size tweaking will need to be done before `sizeBoot` and `binInfoBoot`,
    /// which assume that the values they read out of `size_class_data` are final.
    sizeClassBoot(size_class_data);
    unsigned bin_shard_sizes[SIZE_CLASS_NUM_BINS];
    binShardSizesBoot(bin_shard_sizes);
    /// `prof_boot0` only initializes `opt_prof_prefix` (constant-initialized here) before the options are parsed.
    char readlink_buf[MALLOC_CONF_READLINK_BUF_SIZE];
    readlink_buf[0] = '\0';
    mallocConfInit(size_class_data, bin_shard_sizes, readlink_buf);
    sanitizerInit(options.log2_sanitizer_use_after_free_align);
    sizeBoot(size_class_data, options.cache_oblivious);
    binInfoBoot(size_class_data, bin_shard_sizes);

    if (options.stats_print)
    {
        /// Print statistics at exit.
        if (atexit(statsPrintAtexit) != 0)
        {
            writeMessage("<jemalloc>: Error in atexit()\n");
            if (options.abort)
                abort();
        }
    }

    if (statsBoot())
        return true;
    if (pages::boot())
        return true;
    if (baseBoot(nullptr))
        return true;
    /// `arena_extent_map_global` is static, hence zeroed.
    if (arena_extent_map_global.init(base0Get(), /* zeroed */ true))
        return true;
    if (extentBoot())
        return true;
    if (mallctlBoot())
        return true;
    if constexpr (config::profiling)
        profilingBoot1();
    hugePageAllocatorDisableUnsupported();
    if (arenaBoot(&size_class_data, base0Get(), options.huge_page_allocator))
        return true;
    if (threadCacheBoot(nullptr, base0Get()))
        return true;
    if (arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive))
        return true;
    /// `hook_boot` and `experimental_thread_events_boot` (the user thread event registry) are dropped.

    /// Create enough scaffolding to allow recursive allocation in `mallocNumCPUs`.
    num_arenas_auto = 1;
    manual_arena_base = num_arenas_auto + 1;
    for (unsigned i = 0; i < num_arenas_auto; ++i)
        arenas[i].store(nullptr, std::memory_order_relaxed);
    /// Initialize one arena here. The rest are lazily created in `arenaChooseHard`.
    if (arenaInit(nullptr, 0, &arena_config_default) == nullptr)
        return true;
    a0 = arenaGet(nullptr, 0, false);

    hugePageAllocatorDisableUnsupported();

    malloc_init_state = malloc_init_a0_initialized;

    size_t buf_len = strlen(readlink_buf);
    if (buf_len > 0)
    {
        void * readlink_allocated = arena0InternalAllocate(buf_len + 1, false, true);
        if (readlink_allocated != nullptr)
        {
            memcpy(readlink_allocated, readlink_buf, buf_len + 1);
            options.malloc_conf_symlink = static_cast<const char *>(readlink_allocated);
        }
    }

    return false;
}

/// jemalloc: malloc_init_hard_a0
bool mallocInitHardA0()
{
    init_lock.lock(nullptr);
    bool result = mallocInitHardA0Locked();
    init_lock.unlock(nullptr);
    return result;
}

/// Initialize data structures which may trigger recursive allocation.
/// jemalloc: malloc_init_hard_recursible
bool mallocInitHardRecursible()
{
    malloc_init_state = malloc_init_recursible;

    num_cpus = mallocNumCPUs();
    if (options.per_cpu_arena != PerCPUArenaMode::Disabled)
    {
        bool cpu_count_is_deterministic = mallocCPUCountIsDeterministic();
        if (!cpu_count_is_deterministic)
        {
            /// If the number of CPUs is not deterministic, and narenas is not specified, disable per CPU arenas since
            /// they may not detect CPU IDs properly.
            if (options.num_arenas == 0)
            {
                options.per_cpu_arena = PerCPUArenaMode::Disabled;
                writeMessage("<jemalloc>: Number of CPUs detected is not deterministic. Per-CPU arena disabled.\n");
                if (options.abort_configuration)
                    mallocAbortInvalidConfiguration();
                if (options.abort)
                    abort();
            }
        }
    }

    if constexpr (config::have_pthread_atfork && !config::mutex_init_callback && !config::zone)
    {
        /// LinuxThreads' `pthread_atfork` allocates.
        if (pthread_atfork(jemallocPrefork, jemallocPostforkParent, jemallocPostforkChild) != 0)
        {
            writeMessage("<jemalloc>: Error in pthread_atfork()\n");
            if (options.abort)
                abort();
            return true;
        }
    }

    if (backgroundThreadBoot0())
        return true;

    return false;
}

/// jemalloc: malloc_narenas_default
unsigned mallocNumArenasDefault()
{
    ALLOCATOR_ASSERT(num_cpus > 0);
    /// For SMP systems, create more than one arena per CPU by default.
    if (num_cpus > 1)
    {
        FixedPoint fixed_point_num_cpus = fixed_point::initInt(num_cpus);
        FixedPoint goal = fixed_point::multiply(fixed_point_num_cpus, options.num_arenas_ratio);
        uint32_t int_goal = fixed_point::roundNearest(goal);
        if (int_goal == 0)
            return 1;
        return int_goal;
    }
    return 1;
}

/// jemalloc: percpu_arena_as_initialized
PerCPUArenaMode perCPUArenaAsInitialized(PerCPUArenaMode mode)
{
    ALLOCATOR_ASSERT(!mallocInitialized());
    ALLOCATOR_ASSERT(unsigned(mode) <= unsigned(PerCPUArenaMode::Disabled));

    if (mode != PerCPUArenaMode::Disabled)
        mode = PerCPUArenaMode(unsigned(mode) + per_cpu_arena_mode_enabled_base);

    return mode;
}

/// jemalloc: malloc_init_narenas
bool mallocInitNumArenas(ThreadState * thread_state)
{
    ALLOCATOR_ASSERT(num_cpus > 0);

    if (options.per_cpu_arena != PerCPUArenaMode::Disabled)
    {
        bool getcpu_unavailable;
        if constexpr (config::have_per_cpu_arena)
            getcpu_unavailable = mallocGetcpu() < 0;
        else
            getcpu_unavailable = true;

        if (getcpu_unavailable)
        {
            options.per_cpu_arena = PerCPUArenaMode::Disabled;
            printMessage(
                "<jemalloc>: perCPU arena getcpu() not available. Setting narenas to %u.\n",
                options.num_arenas ? options.num_arenas : mallocNumArenasDefault());
            if (options.abort)
                abort();
        }
        else
        {
            if (num_cpus >= MALLOCX_ARENA_LIMIT)
            {
                printMessage("<jemalloc>: narenas w/ percpuarena beyond limit (%d)\n", int(num_cpus));
                if (options.abort)
                    abort();
                return true;
            }
            /// NB: `opt.percpu_arena` isn't fully initialized yet.
            if (perCPUArenaAsInitialized(options.per_cpu_arena) == PerCPUArenaMode::PerPhysicalCPU && num_cpus % 2 != 0)
            {
                printMessage(
                    "<jemalloc>: invalid configuration -- per physical CPU arena with odd number (%u) of CPUs (no hyper "
                    "threading?).\n",
                    num_cpus);
                if (options.abort)
                    abort();
            }
            unsigned n = perCPUArenaIdxLimit(perCPUArenaAsInitialized(options.per_cpu_arena));
            if (options.num_arenas < n)
            {
                /// If narenas is specified with percpu_arena enabled, actual narenas is set as the greater of the two.
                /// `perCPUArenaChoose` will be free to use any of the arenas based on CPU id. This is conservative (at
                /// a small cost) but ensures correctness.
                ///
                /// If for some reason the ncpus determined at boot is not the actual number (e.g. because of affinity
                /// setting from numactl), reserving narenas this way provides a workaround for percpu_arena.
                options.num_arenas = n;
            }
        }
    }
    if (options.num_arenas == 0)
        options.num_arenas = mallocNumArenasDefault();
    ALLOCATOR_ASSERT(options.num_arenas > 0);

    num_arenas_auto = options.num_arenas;
    /// Limit the number of arenas to the indexing range of MALLOCX_ARENA().
    if (num_arenas_auto >= MALLOCX_ARENA_LIMIT)
    {
        num_arenas_auto = MALLOCX_ARENA_LIMIT - 1;
        printMessage("<jemalloc>: Reducing narenas to limit (%d)\n", int(num_arenas_auto));
    }
    numArenasTotalSet(num_arenas_auto);
    if (arenaInitHuge(thread_state, a0))
        numArenasTotalIncrement();
    manual_arena_base = numArenasTotalGet();

    return false;
}

/// jemalloc: malloc_init_percpu
void mallocInitPerCPU()
{
    options.per_cpu_arena = perCPUArenaAsInitialized(options.per_cpu_arena);
}

/// jemalloc: malloc_init_hard_finish
bool mallocInitHardFinish()
{
    if (Mutex::boot())
        return true;

    malloc_init_state = malloc_init_initialized;
    mallocSlowFlagInit();

    return false;
}

/// jemalloc: malloc_init_hard_cleanup
void mallocInitHardCleanup(ThreadState * thread_state, bool reentrancy_set)
{
    init_lock.assertOwner(thread_state);
    init_lock.unlock(thread_state);
    if (reentrancy_set)
    {
        ALLOCATOR_ASSERT(thread_state != nullptr);
        ALLOCATOR_ASSERT(thread_state->reentrancyLevel() > 0);
        postReentrancy(*thread_state);
    }
}

}

/// jemalloc: malloc_init_a0
bool mallocInitA0()
{
    if (ALLOCATOR_UNLIKELY(malloc_init_state == malloc_init_uninitialized))
        return mallocInitHardA0();
    return false;
}

/// jemalloc: malloc_init_hard
bool mallocInitHard()
{
    static_assert(THREAD_CACHE_MAX_CLASS_LIMIT <= USABLE_SIZE_GROW_SLOW_THRESHOLD);
    static_assert(SIZE_CLASS_LOOKUP_MAX_CLASS <= USABLE_SIZE_GROW_SLOW_THRESHOLD);

    init_lock.lock(nullptr);

    if (!mallocInitHardNeeded())
    {
        mallocInitHardCleanup(nullptr, false);
        return false;
    }

    if (malloc_init_state != malloc_init_a0_initialized && mallocInitHardA0Locked())
    {
        mallocInitHardCleanup(nullptr, false);
        return true;
    }

    init_lock.unlock(nullptr);
    /// Recursive allocation relies on functional tsd.
    ThreadState * thread_state = ThreadState::mallocThreadStateBoot0();
    if (thread_state == nullptr)
        return true;
    if (mallocInitHardRecursible())
        return true;

    init_lock.lock(thread_state);
    /// Set reentrancy level to 1 during init.
    preReentrancy(*thread_state, nullptr);
    /// Initialize narenas before `profilingBoot2` (for allocation).
    if (mallocInitNumArenas(thread_state) || backgroundThreadBoot1(thread_state, base0Get()))
    {
        mallocInitHardCleanup(thread_state, true);
        return true;
    }
    /// `opt.hpa` (`pa_shard_enable_hpa` of arena 0) is always false here: HPA is dropped (`hugePageAllocatorDisableUnsupported`).
    ALLOCATOR_ASSERT(!options.huge_page_allocator);
    if (config::profiling && profilingBoot2(*thread_state, base0Get()))
    {
        mallocInitHardCleanup(thread_state, true);
        return true;
    }

    mallocInitPerCPU();

    if (mallocInitHardFinish())
    {
        mallocInitHardCleanup(thread_state, true);
        return true;
    }
    postReentrancy(*thread_state);
    init_lock.unlock(thread_state);

    ThreadState::mallocThreadStateBoot1();
    /// Update TSD after tsd_boot1.
    thread_state = &ThreadState::fetch();
    if (options.background_thread)
    {
        ALLOCATOR_ASSERT(config::background_thread);
        /// Need to finish init & unlock first before creating background threads (`pthread_create` depends on
        /// malloc). `backgroundThreadMallctlInit` (which sets `is_threaded`) needs to be called without holding any lock.
        backgroundThreadMallctlInit(thread_state);
        if (backgroundThreadCreate(*thread_state, 0))
            return true;
    }
    return false;
}

}
