#pragma once

/// The global arena table and arena selection.
/// jemalloc: the arena parts of `src/jemalloc.c` (`arenas`, `narenas_total`, `narenas_auto`, `manual_arena_base`,
/// `arenas_lock`, `arena_init`, `arena_bind`, `arena_migrate`, `arena_choose_hard`, `arena_cleanup`, `a0*`,
/// `bootstrap_*`), `jemalloc_internal_inlines_a.h` (`malloc_getcpu`, `percpu_arena_*`, `arena_get`),
/// `jemalloc_internal_inlines_b.h` (`arena_choose*`, `arena_is_auto`), and `arena_get_from_edata`,
/// `arena_choose_maybe_huge` from `arena_inlines_b.h`.
///
/// The tcache association done by `arena_choose_impl` on first use and by `percpu_arena_update` is out of line
/// (Arenas.cpp includes ThreadCache.h, which includes this header).

#include <allocator/Arena.h>
#include <allocator/Common.h>
#include <allocator/Mutex.h>
#include <allocator/Options.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstdint>
#include <sched.h>

namespace jemalloc
{

/// Arenas that are used to service external requests. Not all elements of the arenas array are necessarily used;
/// arenas are created lazily as needed.
///
/// `arenas[0 .. narenas_auto)` are used for automatic multiplexing of threads and arenas.
/// `arenas[narenas_auto .. narenas_total)` are only used if the application takes some action to create them and
/// allocate from them.
/// jemalloc: arenas
extern constinit std::atomic<Arena *> arenas[MALLOCX_ARENA_LIMIT];

/// Read-only after initialization. jemalloc: narenas_auto
extern constinit unsigned narenas_auto;

/// Read-only after initialization (the first manual arena index: `narenas_auto` + the huge arena if enabled).
/// jemalloc: manual_arena_base
extern constinit unsigned manual_arena_base;

/// `arenas[0]`, read-only after initialization (set by the a0 initialization). jemalloc: a0 (static)
extern constinit Arena * a0;

/// Protects arenas initialization. "arenas", `MutexRank::ARENAS`; initialized by the a0 initialization.
/// jemalloc: arenas_lock (static)
extern constinit Mutex arenas_lock;

/// Use `narenasTotalGet` etc. jemalloc: narenas_total (static)
extern constinit std::atomic<unsigned> narenas_total;

/// jemalloc: arena_set
JE_ALWAYS_INLINE void arenaSet(unsigned ind, Arena * arena)
{
    arenas[ind].store(arena, std::memory_order_release);
}

/// jemalloc: narenas_total_set
JE_ALWAYS_INLINE void narenasTotalSet(unsigned narenas)
{
    narenas_total.store(narenas, std::memory_order_release);
}

/// jemalloc: narenas_total_inc
JE_ALWAYS_INLINE void narenasTotalInc()
{
    narenas_total.fetch_add(1, std::memory_order_release);
}

/// jemalloc: narenas_total_get
JE_ALWAYS_INLINE unsigned narenasTotalGet()
{
    return narenas_total.load(std::memory_order_acquire);
}

/// Create a new arena and insert it into the arenas array at index `ind` (under `arenas_lock`), then create its
/// background thread. Returns null on failure.
/// jemalloc: arena_init
Arena * arenaInit(ThreadState * tsdn, unsigned ind, const ArenaConfig * config);

/// The same without the lock and without the background thread (the caller holds `arenas_lock`).
/// jemalloc: arena_init_locked (static)
Arena * arenaInitLocked(ThreadState * tsdn, unsigned ind, const ArenaConfig * config);

/// jemalloc: arena_get
JE_ALWAYS_INLINE Arena * arenaGet(ThreadState * tsdn, unsigned ind, bool init_if_missing)
{
    JE_ASSERT(ind < MALLOCX_ARENA_LIMIT);

    Arena * ret = arenas[ind].load(std::memory_order_acquire);
    if (JE_UNLIKELY(ret == nullptr))
    {
        if (init_if_missing)
            ret = arenaInit(tsdn, ind, &arena_config_default);
    }
    return ret;
}

/// jemalloc: arena_is_auto
JE_ALWAYS_INLINE bool arenaIsAuto(const Arena * arena)
{
    JE_ASSERT(narenas_auto > 0);
    return arenaIndGet(arena) < manual_arena_base;
}

/// jemalloc: arena_get_from_edata
JE_ALWAYS_INLINE Arena * arenaGetFromEdata(const Extent * edata)
{
    return arenas[edata->arenaInd()].load(std::memory_order_relaxed);
}

/// --- CPU id, percpu arenas (jemalloc_internal_inlines_a.h) ---------------------------------------------------------

/// `ncpus` (the number of CPUs, `malloc_ncpus`) is declared in Mutex.h (the spin limit depends on it) and defined in
/// Init.cpp.

/// jemalloc: malloc_cpuid_t
using malloc_cpuid_t = int;

/// The current CPU. On Darwin (no `sched_getcpu`), reads the CPU number like `_os_cpu_number` does, from the low 12
/// bits of `tpidr_el0` (arm64) or the IDT base (x86) (the fork's patch; requires macOS 12+).
/// jemalloc: malloc_getcpu
JE_ALWAYS_INLINE malloc_cpuid_t mallocGetcpu()
{
    JE_ASSERT(config::have_percpu_arena);
#if defined(__linux__) || (defined(__FreeBSD__) && defined(__powerpc64__))
    return malloc_cpuid_t(sched_getcpu());
#elif defined(__APPLE__) && defined(__aarch64__)
    uint64_t cpu;
    __asm__ __volatile__("mrs %0, tpidr_el0" : "=r"(cpu));
    return malloc_cpuid_t(cpu & 0xfff);
#elif defined(__APPLE__) && defined(__x86_64__)
    struct
    {
        uintptr_t p1;
        uintptr_t p2;
    } idtr;
    __asm__ __volatile__("sidt %0" : "=m"(idtr));
    return malloc_cpuid_t(idtr.p1 & 0xfff);
#else
    JE_NOT_REACHED();
    return -1;
#endif
}

/// Return the chosen arena index based on current cpu.
/// jemalloc: percpu_arena_choose
JE_ALWAYS_INLINE unsigned percpuArenaChoose()
{
    JE_ASSERT(config::have_percpu_arena && percpuArenaEnabled(opt.percpu_arena));

    malloc_cpuid_t cpuid = mallocGetcpu();
    JE_ASSERT(cpuid >= 0);

    unsigned arena_ind;
    if ((opt.percpu_arena == PercpuArenaMode::Percpu) || (unsigned(cpuid) < ncpus / 2))
    {
        arena_ind = unsigned(cpuid);
    }
    else
    {
        JE_ASSERT(opt.percpu_arena == PercpuArenaMode::PerPhycpu);
        /// Hyper threads on the same physical CPU share arena.
        arena_ind = unsigned(cpuid) - ncpus / 2;
    }

    return arena_ind;
}

/// Return the limit of percpu auto arena range, i.e. arenas[0 .. ind_limit).
/// jemalloc: percpu_arena_ind_limit
JE_ALWAYS_INLINE unsigned percpuArenaIndLimit(PercpuArenaMode mode)
{
    JE_ASSERT(config::have_percpu_arena && percpuArenaEnabled(mode));
    if (mode == PercpuArenaMode::PerPhycpu && ncpus > 1)
    {
        if (ncpus % 2)
        {
            /// This likely means a misconfig.
            return ncpus / 2 + 1;
        }
        return ncpus / 2;
    }
    return ncpus;
}

/// Migrates the thread to the arena of `cpu` (and reassociates its tcache).
/// jemalloc: percpu_arena_update
void percpuArenaUpdate(ThreadState & tsd, unsigned cpu);

/// --- Binding (jemalloc.c) ------------------------------------------------------------------------------------------

/// jemalloc: arena_bind (static)
void arenaBind(ThreadState & tsd, unsigned ind, bool internal);

/// jemalloc: arena_migrate
void arenaMigrate(ThreadState & tsd, Arena * oldarena, Arena * newarena);

/// jemalloc: arena_unbind (static)
void arenaUnbind(ThreadState & tsd, unsigned ind, bool internal);

/// Slow path, called only by `arenaChooseImpl`.
/// jemalloc: arena_choose_hard
Arena * arenaChooseHard(ThreadState & tsd, bool internal);

/// The first-use part of `arena_choose_impl`: `arena_choose_hard` and the association of the thread's tcache.
Arena * arenaChooseFirstUse(ThreadState & tsd, bool internal);

/// Choose an arena based on a per-thread value.
/// jemalloc: arena_choose_impl
JE_ALWAYS_INLINE Arena * arenaChooseImpl(ThreadState & tsd, Arena * arena, bool internal)
{
    if (arena != nullptr)
        return arena;

    /// During reentrancy, arena 0 is the safest bet.
    if (JE_UNLIKELY(tsd.reentrancyLevel() > 0))
        return arenaGet(&tsd, 0, true);

    Arena * ret = internal ? tsd.iarena : tsd.arena;
    if (JE_UNLIKELY(ret == nullptr))
        ret = arenaChooseFirstUse(tsd, internal);

    /// Note that for percpu arena, if the current arena is outside of the auto percpu arena range, (i.e. thread is
    /// assigned to a manually managed arena), then percpu arena is skipped.
    if (config::have_percpu_arena && percpuArenaEnabled(opt.percpu_arena) && !internal
        && (arenaIndGet(ret) < percpuArenaIndLimit(opt.percpu_arena)) && (ret->last_thd != &tsd))
    {
        unsigned ind = percpuArenaChoose();
        if (arenaIndGet(ret) != ind)
        {
            percpuArenaUpdate(tsd, ind);
            ret = tsd.arena;
        }
        ret->last_thd = &tsd;
    }

    return ret;
}

/// jemalloc: arena_choose
JE_ALWAYS_INLINE Arena * arenaChoose(ThreadState & tsd, Arena * arena)
{
    return arenaChooseImpl(tsd, arena, false);
}

/// jemalloc: arena_ichoose
JE_ALWAYS_INLINE Arena * arenaIchoose(ThreadState & tsd, Arena * arena)
{
    return arenaChooseImpl(tsd, arena, true);
}

/// For huge allocations, use the dedicated huge arena if both are true: 1) is using auto arena selection (i.e.
/// arena == null), and 2) the thread is not assigned to a manual arena.
/// jemalloc: arena_choose_maybe_huge
JE_ALWAYS_INLINE Arena * arenaChooseMaybeHuge(ThreadState & tsd, Arena * arena, size_t size)
{
    if (arena != nullptr)
        return arena;

    Arena * tsd_arena = tsd.arena;
    if (tsd_arena == nullptr)
        tsd_arena = arenaChoose(tsd, nullptr);

    size_t threshold = tsd_arena->pa_shard.pac.oversize_threshold.load(std::memory_order_relaxed);
    if (JE_UNLIKELY(size >= threshold) && arenaIsAuto(tsd_arena))
        return arenaChooseHuge(tsd);

    return tsd_arena;
}

/// `arenaCleanup`, `iarenaCleanup` (jemalloc: arena_cleanup, iarena_cleanup) are declared in ThreadState.h.

/// --- Bootstrap allocation (jemalloc.c) -----------------------------------------------------------------------------

/// Initializes the allocator up to arena 0 if it is not yet initialized. Returns true on error.
/// Defined in Init.cpp. jemalloc: malloc_init_a0
bool mallocInitA0();

/// The `a0*` functions are used instead of `i{d,}alloc` in situations that cannot tolerate TLS variable access.
/// `a0malloc`, `a0dalloc` are declared in ThreadState.h.
/// jemalloc: a0ialloc (static)
void * a0ialloc(size_t size, bool zero, bool is_internal);
/// jemalloc: a0idalloc (static)
void a0idalloc(void * ptr, bool is_internal);

/// FreeBSD's libc uses the `bootstrap_*` functions in bootstrap-sensitive situations that cannot tolerate TLS
/// variable access (TLS allocation and very early internal data structure initialization).
/// jemalloc: bootstrap_malloc, bootstrap_free (`bootstrapCalloc` is declared in Mutex.h)
void * bootstrapMalloc(size_t size);
void bootstrapFree(void * ptr);

/// Provided by BackgroundThread (jemalloc: background_thread_create). Returns true on error.
bool backgroundThreadCreate(ThreadState & tsd, unsigned arena_ind);

}
