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
/// `arenas[0 .. num_arenas_auto)` are used for automatic multiplexing of threads and arenas.
/// `arenas[num_arenas_auto .. num_arenas_total)` are only used if the application takes some action to create them and
/// allocate from them.
/// jemalloc: arenas
extern constinit std::atomic<Arena *> arenas[MALLOCX_ARENA_LIMIT];

/// Read-only after initialization. jemalloc: narenas_auto
extern constinit unsigned num_arenas_auto;

/// Read-only after initialization (the first manual arena index: `num_arenas_auto` + the huge arena if enabled).
/// jemalloc: manual_arena_base
extern constinit unsigned manual_arena_base;

/// `arenas[0]`, read-only after initialization (set by the a0 initialization). jemalloc: a0 (static)
extern constinit Arena * a0;

/// Protects arenas initialization. "arenas", `MutexRank::ARENAS`; initialized by the a0 initialization.
/// jemalloc: arenas_lock (static)
extern constinit Mutex arenas_lock;

/// Use `numArenasTotalGet` etc. jemalloc: narenas_total (static)
extern constinit std::atomic<unsigned> num_arenas_total;

/// jemalloc: arena_set
ALLOCATOR_ALWAYS_INLINE void arenaSet(unsigned idx, Arena * arena)
{
    arenas[idx].store(arena, std::memory_order_release);
}

/// jemalloc: narenas_total_set
ALLOCATOR_ALWAYS_INLINE void numArenasTotalSet(unsigned num_arenas)
{
    num_arenas_total.store(num_arenas, std::memory_order_release);
}

/// jemalloc: narenas_total_inc
ALLOCATOR_ALWAYS_INLINE void numArenasTotalIncrement()
{
    num_arenas_total.fetch_add(1, std::memory_order_release);
}

/// jemalloc: narenas_total_get
ALLOCATOR_ALWAYS_INLINE unsigned numArenasTotalGet()
{
    return num_arenas_total.load(std::memory_order_acquire);
}

/// Create a new arena and insert it into the arenas array at index `idx` (under `arenas_lock`), then create its
/// background thread. Returns null on failure.
/// jemalloc: arena_init
Arena * arenaInit(ThreadState * thread_state, unsigned idx, const ArenaConfig * config);

/// The same without the lock and without the background thread (the caller holds `arenas_lock`).
/// jemalloc: arena_init_locked (static)
Arena * arenaInitLocked(ThreadState * thread_state, unsigned idx, const ArenaConfig * config);

/// jemalloc: arena_get
ALLOCATOR_ALWAYS_INLINE Arena * arenaGet(ThreadState * thread_state, unsigned idx, bool init_if_missing)
{
    ALLOCATOR_ASSERT(idx < MALLOCX_ARENA_LIMIT);

    Arena * result = arenas[idx].load(std::memory_order_acquire);
    if (ALLOCATOR_UNLIKELY(result == nullptr))
    {
        if (init_if_missing)
            result = arenaInit(thread_state, idx, &arena_config_default);
    }
    return result;
}

/// jemalloc: arena_is_auto
ALLOCATOR_ALWAYS_INLINE bool arenaIsAuto(const Arena * arena)
{
    ALLOCATOR_ASSERT(num_arenas_auto > 0);
    return arenaIdxGet(arena) < manual_arena_base;
}

/// jemalloc: arena_get_from_edata
ALLOCATOR_ALWAYS_INLINE Arena * arenaGetFromExtent(const Extent * extent)
{
    return arenas[extent->arenaIdx()].load(std::memory_order_relaxed);
}

/// --- CPU id, percpu arenas (jemalloc_internal_inlines_a.h) ---------------------------------------------------------

/// `num_cpus` (the number of CPUs, `malloc_ncpus`) is declared in Mutex.h (the spin limit depends on it) and defined in
/// Init.cpp.

/// jemalloc: malloc_cpuid_t
using CPUIDType = int;

/// The current CPU. On Darwin (no `sched_getcpu`), reads the CPU number like `_os_cpu_number` does, from the low 12
/// bits of `tpidr_el0` (arm64) or the IDT base (x86) (the fork's patch; requires macOS 12+).
/// jemalloc: malloc_getcpu
ALLOCATOR_ALWAYS_INLINE CPUIDType mallocGetcpu()
{
    ALLOCATOR_ASSERT(config::have_per_cpu_arena);
#if defined(__linux__) || (defined(__FreeBSD__) && defined(__powerpc64__))
    return CPUIDType(sched_getcpu());
#elif defined(__APPLE__) && defined(__aarch64__)
    uint64_t cpu;
    __asm__ __volatile__("mrs %0, tpidr_el0" : "=r"(cpu));
    return CPUIDType(cpu & 0xfff);
#elif defined(__APPLE__) && defined(__x86_64__)
    struct
    {
        uintptr_t p1;
        uintptr_t p2;
    } idtr;
    __asm__ __volatile__("sidt %0" : "=m"(idtr));
    return CPUIDType(idtr.p1 & 0xfff);
#else
    ALLOCATOR_NOT_REACHED();
    return -1;
#endif
}

/// Return the chosen arena index based on current cpu.
/// jemalloc: percpu_arena_choose
ALLOCATOR_ALWAYS_INLINE unsigned perCPUArenaChoose()
{
    ALLOCATOR_ASSERT(config::have_per_cpu_arena && perCPUArenaEnabled(options.per_cpu_arena));

    CPUIDType cpuid = mallocGetcpu();
    ALLOCATOR_ASSERT(cpuid >= 0);

    unsigned arena_idx;
    if ((options.per_cpu_arena == PerCPUArenaMode::PerCPU) || (unsigned(cpuid) < num_cpus / 2))
    {
        arena_idx = unsigned(cpuid);
    }
    else
    {
        ALLOCATOR_ASSERT(options.per_cpu_arena == PerCPUArenaMode::PerPhysicalCPU);
        /// Hyper threads on the same physical CPU share arena.
        arena_idx = unsigned(cpuid) - num_cpus / 2;
    }

    return arena_idx;
}

/// Return the limit of percpu auto arena range, i.e. arenas[0 .. ind_limit).
/// jemalloc: percpu_arena_ind_limit
ALLOCATOR_ALWAYS_INLINE unsigned perCPUArenaIdxLimit(PerCPUArenaMode mode)
{
    ALLOCATOR_ASSERT(config::have_per_cpu_arena && perCPUArenaEnabled(mode));
    if (mode == PerCPUArenaMode::PerPhysicalCPU && num_cpus > 1)
    {
        if (num_cpus % 2)
        {
            /// This likely means a misconfig.
            return num_cpus / 2 + 1;
        }
        return num_cpus / 2;
    }
    return num_cpus;
}

/// Migrates the thread to the arena of `cpu` (and reassociates its tcache).
/// jemalloc: percpu_arena_update
void perCPUArenaUpdate(ThreadState & thread_state, unsigned cpu);

/// --- Binding (jemalloc.c) ------------------------------------------------------------------------------------------

/// jemalloc: arena_bind (static)
void arenaBind(ThreadState & thread_state, unsigned idx, bool internal);

/// jemalloc: arena_migrate
void arenaMigrate(ThreadState & thread_state, Arena * old_arena, Arena * new_arena);

/// jemalloc: arena_unbind (static)
void arenaUnbind(ThreadState & thread_state, unsigned idx, bool internal);

/// Slow path, called only by `arenaChooseImpl`.
/// jemalloc: arena_choose_hard
Arena * arenaChooseHard(ThreadState & thread_state, bool internal);

/// The first-use part of `arena_choose_impl`: `arena_choose_hard` and the association of the thread's tcache.
Arena * arenaChooseFirstUse(ThreadState & thread_state, bool internal);

/// Choose an arena based on a per-thread value.
/// jemalloc: arena_choose_impl
ALLOCATOR_ALWAYS_INLINE Arena * arenaChooseImpl(ThreadState & thread_state, Arena * arena, bool internal)
{
    if (arena != nullptr)
        return arena;

    /// During reentrancy, arena 0 is the safest bet.
    if (ALLOCATOR_UNLIKELY(thread_state.reentrancyLevel() > 0))
        return arenaGet(&thread_state, 0, true);

    Arena * result = internal ? thread_state.internal_arena : thread_state.arena;
    if (ALLOCATOR_UNLIKELY(result == nullptr))
    {
        result = arenaChooseFirstUse(thread_state, internal);
        if (ALLOCATOR_UNLIKELY(result == nullptr))
            return nullptr;
    }

    /// Note that for percpu arena, if the current arena is outside of the auto percpu arena range, (i.e. thread is
    /// assigned to a manually managed arena), then percpu arena is skipped.
    if (config::have_per_cpu_arena && perCPUArenaEnabled(options.per_cpu_arena) && !internal
        && (arenaIdxGet(result) < perCPUArenaIdxLimit(options.per_cpu_arena)) && (result->last_thread != &thread_state))
    {
        unsigned idx = perCPUArenaChoose();
        if (arenaIdxGet(result) != idx)
        {
            perCPUArenaUpdate(thread_state, idx);
            result = thread_state.arena;
        }
        result->last_thread = &thread_state;
    }

    return result;
}

/// jemalloc: arena_choose
ALLOCATOR_ALWAYS_INLINE Arena * arenaChoose(ThreadState & thread_state, Arena * arena)
{
    return arenaChooseImpl(thread_state, arena, false);
}

/// jemalloc: arena_ichoose
ALLOCATOR_ALWAYS_INLINE Arena * arenaChooseInternal(ThreadState & thread_state, Arena * arena)
{
    return arenaChooseImpl(thread_state, arena, true);
}

/// For huge allocations, use the dedicated huge arena if both are true: 1) is using auto arena selection (i.e.
/// arena == null), and 2) the thread is not assigned to a manual arena.
/// jemalloc: arena_choose_maybe_huge
ALLOCATOR_ALWAYS_INLINE Arena * arenaChooseMaybeHuge(ThreadState & thread_state, Arena * arena, size_t size)
{
    if (arena != nullptr)
        return arena;

    Arena * thread_state_arena = thread_state.arena;
    if (thread_state_arena == nullptr)
        thread_state_arena = arenaChoose(thread_state, nullptr);
    if (ALLOCATOR_UNLIKELY(thread_state_arena == nullptr))
        return nullptr;

    size_t threshold = thread_state_arena->page_allocator_shard.page_allocator.oversize_threshold.load(std::memory_order_relaxed);
    if (ALLOCATOR_UNLIKELY(size >= threshold) && arenaIsAuto(thread_state_arena))
        return arenaChooseHuge(thread_state);

    return thread_state_arena;
}

/// `arenaCleanup`, `internalArenaCleanup` (jemalloc: arena_cleanup, iarena_cleanup) are declared in ThreadState.h.

/// --- Bootstrap allocation (jemalloc.c) -----------------------------------------------------------------------------

/// Initializes the allocator up to arena 0 if it is not yet initialized. Returns true on error.
/// Defined in Init.cpp. jemalloc: malloc_init_a0
bool mallocInitA0();

/// The `a0*` functions are used instead of `i{d,}alloc` in situations that cannot tolerate TLS variable access.
/// `arena0Allocate`, `arena0Deallocate` are declared in ThreadState.h.
/// jemalloc: a0ialloc (static)
void * arena0InternalAllocate(size_t size, bool zero, bool is_internal);
/// jemalloc: a0idalloc (static)
void arena0InternalDeallocate(void * ptr, bool is_internal);

/// FreeBSD's libc uses the `bootstrap_*` functions in bootstrap-sensitive situations that cannot tolerate TLS
/// variable access (TLS allocation and very early internal data structure initialization).
/// jemalloc: bootstrap_malloc, bootstrap_free (`bootstrapCalloc` is declared in Mutex.h)
void * bootstrapAllocate(size_t size);
void bootstrapFree(void * ptr);

/// Provided by BackgroundThread (jemalloc: background_thread_create). Returns true on error.
bool backgroundThreadCreate(ThreadState & thread_state, unsigned arena_idx);

}
