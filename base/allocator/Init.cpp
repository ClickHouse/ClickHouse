#include <allocator/Init.h>

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/Conf.h>
#include <allocator/Ctl.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentOps.h>
#include <allocator/FixedPoint.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Mutex.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/ProfHooks.h>
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
#    include <pthread_np.h>
#    include <sys/cpuset.h>
#endif

namespace jemalloc
{

namespace
{

/// When `malloc_slow` is true, set the corresponding bits for sanity check. jemalloc: flag_opt_* (anonymous enum)
enum : uint8_t
{
    flag_opt_junk_alloc = (1U),
    flag_opt_junk_free = (1U << 1),
    flag_opt_zero = (1U << 2),
    flag_opt_utrace = (1U << 3),
    flag_opt_xmalloc = (1U << 4),
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
        ThreadState * tsdn = ThreadState::tsdnFetch();

        /// Merge stats from extant threads. This is racy, since individual threads do not lock when recording tcache
        /// stats events. As a consequence, the final stats may be slightly out of date by the time they are reported,
        /// if other threads continue to allocate.
        for (unsigned i = 0, narenas = narenasTotalGet(); i < narenas; ++i)
        {
            Arena * arena = arenaGet(tsdn, i, false);
            if (arena != nullptr)
            {
                arena->tcache_ql_mtx.lock(tsdn);
                arena->tcache_ql.forEach([&](ThreadCacheSlow * tcache_slow) { tcacheStatsMerge(tsdn, tcache_slow->tcache, arena); });
                arena->tcache_ql_mtx.unlock(tsdn);
            }
        }
    }
    mallocStatsPrint(nullptr, nullptr, opt.stats_print_opts);
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
unsigned mallocNcpus()
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
    long cpu_onln = sysconf(_SC_NPROCESSORS_ONLN);
    long cpu_conf = sysconf(_SC_NPROCESSORS_CONF);
    if (cpu_onln != cpu_conf)
        return false;
#if defined(__linux__) || defined(__FreeBSD__)
    long cpu_affinity = affinityCPUCount();
    if (cpu_affinity != cpu_conf)
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
    malloc_slow_flags |= (opt.junk_alloc ? flag_opt_junk_alloc : 0) | (opt.junk_free ? flag_opt_junk_free : 0)
        | (opt.zero ? flag_opt_zero : 0) | (opt.utrace ? flag_opt_utrace : 0) | (opt.xmalloc ? flag_opt_xmalloc : 0);

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

    SizeClassData sc_data{};

    /// Ordering here is somewhat tricky; we need `scBoot` first, since that determines what the size classes will be,
    /// and then `mallocConfInit`, since any slab size tweaking will need to be done before `szBoot` and `binInfoBoot`,
    /// which assume that the values they read out of `sc_data` are final.
    scBoot(sc_data);
    unsigned bin_shard_sizes[SC_NBINS];
    binShardSizesBoot(bin_shard_sizes);
    /// `prof_boot0` only initializes `opt_prof_prefix` (constant-initialized here) before the options are parsed.
    char readlink_buf[MALLOC_CONF_READLINK_BUF_SIZE];
    readlink_buf[0] = '\0';
    mallocConfInit(sc_data, bin_shard_sizes, readlink_buf);
    sanInit(opt.lg_san_uaf_align);
    szBoot(sc_data, opt.cache_oblivious);
    binInfoBoot(sc_data, bin_shard_sizes);

    if (opt.stats_print)
    {
        /// Print statistics at exit.
        if (atexit(statsPrintAtexit) != 0)
        {
            writeMessage("<jemalloc>: Error in atexit()\n");
            if (opt.abort)
                abort();
        }
    }

    if (statsBoot())
        return true;
    if (pages::boot())
        return true;
    if (baseBoot(nullptr))
        return true;
    /// `arena_emap_global` is static, hence zeroed.
    if (arena_emap_global.init(b0get(), /* zeroed */ true))
        return true;
    if (extentBoot())
        return true;
    if (ctlBoot())
        return true;
    if constexpr (config::prof)
        profBoot1();
    hpaDisableUnsupported();
    if (arenaBoot(&sc_data, b0get(), opt.hpa))
        return true;
    if (tcacheBoot(nullptr, b0get()))
        return true;
    if (arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive))
        return true;
    /// `hook_boot` and `experimental_thread_events_boot` (the user thread event registry) are dropped.

    /// Create enough scaffolding to allow recursive allocation in `mallocNcpus`.
    narenas_auto = 1;
    manual_arena_base = narenas_auto + 1;
    for (unsigned i = 0; i < narenas_auto; ++i)
        arenas[i].store(nullptr, std::memory_order_relaxed);
    /// Initialize one arena here. The rest are lazily created in `arenaChooseHard`.
    if (arenaInit(nullptr, 0, &arena_config_default) == nullptr)
        return true;
    a0 = arenaGet(nullptr, 0, false);

    hpaDisableUnsupported();

    malloc_init_state = malloc_init_a0_initialized;

    size_t buf_len = strlen(readlink_buf);
    if (buf_len > 0)
    {
        void * readlink_allocated = a0ialloc(buf_len + 1, false, true);
        if (readlink_allocated != nullptr)
        {
            memcpy(readlink_allocated, readlink_buf, buf_len + 1);
            opt.malloc_conf_symlink = static_cast<const char *>(readlink_allocated);
        }
    }

    return false;
}

/// jemalloc: malloc_init_hard_a0
bool mallocInitHardA0()
{
    init_lock.lock(nullptr);
    bool ret = mallocInitHardA0Locked();
    init_lock.unlock(nullptr);
    return ret;
}

/// Initialize data structures which may trigger recursive allocation.
/// jemalloc: malloc_init_hard_recursible
bool mallocInitHardRecursible()
{
    malloc_init_state = malloc_init_recursible;

    ncpus = mallocNcpus();
    if (opt.percpu_arena != PercpuArenaMode::Disabled)
    {
        bool cpu_count_is_deterministic = mallocCPUCountIsDeterministic();
        if (!cpu_count_is_deterministic)
        {
            /// If the number of CPUs is not deterministic, and narenas is not specified, disable per CPU arenas since
            /// they may not detect CPU IDs properly.
            if (opt.narenas == 0)
            {
                opt.percpu_arena = PercpuArenaMode::Disabled;
                writeMessage("<jemalloc>: Number of CPUs detected is not deterministic. Per-CPU arena disabled.\n");
                if (opt.abort_conf)
                    mallocAbortInvalidConf();
                if (opt.abort)
                    abort();
            }
        }
    }

    if constexpr (config::have_pthread_atfork && !config::mutex_init_cb && !config::zone)
    {
        /// LinuxThreads' `pthread_atfork` allocates.
        if (pthread_atfork(jemallocPrefork, jemallocPostforkParent, jemallocPostforkChild) != 0)
        {
            writeMessage("<jemalloc>: Error in pthread_atfork()\n");
            if (opt.abort)
                abort();
            return true;
        }
    }

    if (backgroundThreadBoot0())
        return true;

    return false;
}

/// jemalloc: malloc_narenas_default
unsigned mallocNarenasDefault()
{
    JE_ASSERT(ncpus > 0);
    /// For SMP systems, create more than one arena per CPU by default.
    if (ncpus > 1)
    {
        FixedPoint fxp_ncpus = fxp::initInt(ncpus);
        FixedPoint goal = fxp::mul(fxp_ncpus, opt.narenas_ratio);
        uint32_t int_goal = fxp::roundNearest(goal);
        if (int_goal == 0)
            return 1;
        return int_goal;
    }
    return 1;
}

/// jemalloc: percpu_arena_as_initialized
PercpuArenaMode percpuArenaAsInitialized(PercpuArenaMode mode)
{
    JE_ASSERT(!mallocInitialized());
    JE_ASSERT(unsigned(mode) <= unsigned(PercpuArenaMode::Disabled));

    if (mode != PercpuArenaMode::Disabled)
        mode = PercpuArenaMode(unsigned(mode) + percpu_arena_mode_enabled_base);

    return mode;
}

/// jemalloc: malloc_init_narenas
bool mallocInitNarenas(ThreadState * tsdn)
{
    JE_ASSERT(ncpus > 0);

    if (opt.percpu_arena != PercpuArenaMode::Disabled)
    {
        bool getcpu_unavailable;
        if constexpr (config::have_percpu_arena)
            getcpu_unavailable = mallocGetcpu() < 0;
        else
            getcpu_unavailable = true;

        if (getcpu_unavailable)
        {
            opt.percpu_arena = PercpuArenaMode::Disabled;
            printMessage(
                "<jemalloc>: perCPU arena getcpu() not available. Setting narenas to %u.\n",
                opt.narenas ? opt.narenas : mallocNarenasDefault());
            if (opt.abort)
                abort();
        }
        else
        {
            if (ncpus >= MALLOCX_ARENA_LIMIT)
            {
                printMessage("<jemalloc>: narenas w/ percpuarena beyond limit (%d)\n", int(ncpus));
                if (opt.abort)
                    abort();
                return true;
            }
            /// NB: `opt.percpu_arena` isn't fully initialized yet.
            if (percpuArenaAsInitialized(opt.percpu_arena) == PercpuArenaMode::PerPhycpu && ncpus % 2 != 0)
            {
                printMessage(
                    "<jemalloc>: invalid configuration -- per physical CPU arena with odd number (%u) of CPUs (no hyper "
                    "threading?).\n",
                    ncpus);
                if (opt.abort)
                    abort();
            }
            unsigned n = percpuArenaIndLimit(percpuArenaAsInitialized(opt.percpu_arena));
            if (opt.narenas < n)
            {
                /// If narenas is specified with percpu_arena enabled, actual narenas is set as the greater of the two.
                /// `percpuArenaChoose` will be free to use any of the arenas based on CPU id. This is conservative (at
                /// a small cost) but ensures correctness.
                ///
                /// If for some reason the ncpus determined at boot is not the actual number (e.g. because of affinity
                /// setting from numactl), reserving narenas this way provides a workaround for percpu_arena.
                opt.narenas = n;
            }
        }
    }
    if (opt.narenas == 0)
        opt.narenas = mallocNarenasDefault();
    JE_ASSERT(opt.narenas > 0);

    narenas_auto = opt.narenas;
    /// Limit the number of arenas to the indexing range of MALLOCX_ARENA().
    if (narenas_auto >= MALLOCX_ARENA_LIMIT)
    {
        narenas_auto = MALLOCX_ARENA_LIMIT - 1;
        printMessage("<jemalloc>: Reducing narenas to limit (%d)\n", int(narenas_auto));
    }
    narenasTotalSet(narenas_auto);
    if (arenaInitHuge(tsdn, a0))
        narenasTotalInc();
    manual_arena_base = narenasTotalGet();

    return false;
}

/// jemalloc: malloc_init_percpu
void mallocInitPercpu()
{
    opt.percpu_arena = percpuArenaAsInitialized(opt.percpu_arena);
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
void mallocInitHardCleanup(ThreadState * tsdn, bool reentrancy_set)
{
    init_lock.assertOwner(tsdn);
    init_lock.unlock(tsdn);
    if (reentrancy_set)
    {
        JE_ASSERT(tsdn != nullptr);
        JE_ASSERT(tsdn->reentrancyLevel() > 0);
        postReentrancy(*tsdn);
    }
}

}

/// jemalloc: malloc_init_a0
bool mallocInitA0()
{
    if (JE_UNLIKELY(malloc_init_state == malloc_init_uninitialized))
        return mallocInitHardA0();
    return false;
}

/// jemalloc: malloc_init_hard
bool mallocInitHard()
{
    static_assert(TCACHE_MAXCLASS_LIMIT <= USIZE_GROW_SLOW_THRESHOLD);
    static_assert(SC_LOOKUP_MAXCLASS <= USIZE_GROW_SLOW_THRESHOLD);

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
    ThreadState * tsd = ThreadState::mallocTsdBoot0();
    if (tsd == nullptr)
        return true;
    if (mallocInitHardRecursible())
        return true;

    init_lock.lock(tsd);
    /// Set reentrancy level to 1 during init.
    preReentrancy(*tsd, nullptr);
    /// Initialize narenas before `profBoot2` (for allocation).
    if (mallocInitNarenas(tsd) || backgroundThreadBoot1(tsd, b0get()))
    {
        mallocInitHardCleanup(tsd, true);
        return true;
    }
    /// `opt.hpa` (`pa_shard_enable_hpa` of arena 0) is always false here: HPA is dropped (`hpaDisableUnsupported`).
    JE_ASSERT(!opt.hpa);
    if (config::prof && profBoot2(*tsd, b0get()))
    {
        mallocInitHardCleanup(tsd, true);
        return true;
    }

    mallocInitPercpu();

    if (mallocInitHardFinish())
    {
        mallocInitHardCleanup(tsd, true);
        return true;
    }
    postReentrancy(*tsd);
    init_lock.unlock(tsd);

    ThreadState::mallocTsdBoot1();
    /// Update TSD after tsd_boot1.
    tsd = &ThreadState::fetch();
    if (opt.background_thread)
    {
        JE_ASSERT(config::background_thread);
        /// Need to finish init & unlock first before creating background threads (`pthread_create` depends on
        /// malloc). `backgroundThreadCtlInit` (which sets `isthreaded`) needs to be called without holding any lock.
        backgroundThreadCtlInit(tsd);
        if (backgroundThreadCreate(*tsd, 0))
            return true;
    }
    return false;
}

}
