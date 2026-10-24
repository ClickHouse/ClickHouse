#include <allocator/Arenas.h>

#include <allocator/ArenaInlines.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Format.h>
#include <allocator/ThreadCache.h>

#include <cstdlib>

namespace jemalloc
{

alignas(CACHE_LINE) constinit std::atomic<Arena *> arenas[MALLOCX_ARENA_LIMIT] = {};
constinit std::atomic<unsigned> num_arenas_total{0};
constinit Arena * a0 = nullptr;
constinit unsigned num_arenas_auto = 0;
constinit unsigned manual_arena_base = 0;
constinit Mutex arenas_lock;

/// --- Bootstrap allocation ------------------------------------------------------------------------------------------

/// jemalloc: a0ialloc
void * arena0InternalAllocate(size_t size, bool zero, bool is_internal)
{
    if (ALLOCATOR_UNLIKELY(mallocInitA0()))
        return nullptr;

    /// iallocztm(TSDN_NULL, size, sz_size2index(size), zero, NULL, is_internal, arena_get(TSDN_NULL, 0, true), true)
    ThreadState * thread_state = nullptr;
    SizeClassIdx idx = size_classes::sizeToIndex(size);
    Arena * arena = arenaGet(thread_state, 0, true);
    bool slab = size_classes::canUseSlab(size);
    void * result = arenaMalloc(thread_state, arena, size, idx, zero, slab, nullptr, true);
    if (config::stats && is_internal && ALLOCATOR_LIKELY(result != nullptr))
        arenaInternalAdd(arenaOfPointer(thread_state, result), arenaAllocationSize(thread_state, result));
    return result;
}

/// jemalloc: a0idalloc
void arena0InternalDeallocate(void * ptr, bool is_internal)
{
    /// idalloctm(TSDN_NULL, ptr, NULL, NULL, is_internal, true)
    ThreadState * thread_state = nullptr;
    ALLOCATOR_ASSERT(ptr != nullptr);
    if (config::stats && is_internal)
        arenaInternalSub(arenaOfPointer(thread_state, ptr), arenaAllocationSize(thread_state, ptr));
    arenaDeallocate(thread_state, ptr, nullptr, nullptr, true);
}

/// jemalloc: a0malloc
void * arena0Allocate(size_t size)
{
    return arena0InternalAllocate(size, false, true);
}

/// jemalloc: a0dalloc
void arena0Deallocate(void * ptr)
{
    arena0InternalDeallocate(ptr, true);
}

/// jemalloc: bootstrap_malloc
void * bootstrapAllocate(size_t size)
{
    if (ALLOCATOR_UNLIKELY(size == 0))
        size = 1;

    return arena0InternalAllocate(size, false, false);
}

/// jemalloc: bootstrap_calloc
void * bootstrapAllocateZeroed(size_t num, size_t size)
{
    size_t num_size = num * size;
    if (ALLOCATOR_UNLIKELY(num_size == 0))
    {
        ALLOCATOR_ASSERT(num == 0 || size == 0);
        num_size = 1;
    }

    return arena0InternalAllocate(num_size, true, false);
}

/// jemalloc: bootstrap_free
void bootstrapFree(void * ptr)
{
    if (ALLOCATOR_UNLIKELY(ptr == nullptr))
        return;

    arena0InternalDeallocate(ptr, false);
}

/// --- Creation ------------------------------------------------------------------------------------------------------

/// jemalloc: arena_init_locked
Arena * arenaInitLocked(ThreadState * thread_state, unsigned idx, const ArenaConfig * config)
{
    ALLOCATOR_ASSERT(idx <= numArenasTotalGet());
    if (idx >= MALLOCX_ARENA_LIMIT)
        return nullptr;
    if (idx == numArenasTotalGet())
        numArenasTotalIncrement();

    /// Another thread may have already initialized arenas[ind] if it's an auto arena.
    Arena * arena = arenaGet(thread_state, idx, false);
    if (arena != nullptr)
    {
        ALLOCATOR_ASSERT(arenaIsAuto(arena));
        return arena;
    }

    /// Actually initialize the arena.
    arena = arenaNew(thread_state, idx, config);

    return arena;
}

/// jemalloc: arena_new_create_background_thread
static void arenaNewCreateBackgroundThread(ThreadState * thread_state, unsigned idx)
{
    if (idx == 0)
        return;

    if constexpr (config::background_thread)
    {
        if (backgroundThreadCreate(*thread_state, idx))
        {
            printMessage("<jemalloc>: error in background thread creation for arena %u. Abort.\n", idx);
            abort();
        }
    }
}

/// jemalloc: arena_init
Arena * arenaInit(ThreadState * thread_state, unsigned idx, const ArenaConfig * config)
{
    arenas_lock.lock(thread_state);
    Arena * arena = arenaInitLocked(thread_state, idx, config);
    arenas_lock.unlock(thread_state);

    arenaNewCreateBackgroundThread(thread_state, idx);

    return arena;
}

/// --- Binding -------------------------------------------------------------------------------------------------------

/// jemalloc: arena_bind
void arenaBind(ThreadState & thread_state, unsigned idx, bool internal)
{
    Arena * arena = arenaGet(&thread_state, idx, false);
    arenaNumThreadsIncrement(arena, internal);

    if (internal)
    {
        thread_state.internal_arena = arena;
    }
    else
    {
        thread_state.arena = arena;
        /// While shard acts as a random seed, the cast below should not make much difference.
        uint8_t shard = uint8_t(arena->bin_shard_next.fetch_add(1, std::memory_order_relaxed));
        ThreadStateBinShards * bins = &thread_state.bin_shards;
        for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        {
            ALLOCATOR_ASSERT(bin_infos[i].num_shards > 0 && bin_infos[i].num_shards <= BIN_SHARDS_MAX);
            bins->bin_shard[i] = uint8_t(shard % bin_infos[i].num_shards);
        }
    }
}

/// jemalloc: arena_migrate
void arenaMigrate(ThreadState & thread_state, Arena * old_arena, Arena * new_arena)
{
    ALLOCATOR_ASSERT(old_arena != nullptr);
    ALLOCATOR_ASSERT(new_arena != nullptr);

    arenaNumThreadsDecrement(old_arena, false);
    arenaNumThreadsIncrement(new_arena, false);
    thread_state.arena = new_arena;

    if (arenaNumThreadsGet(old_arena, false) == 0 && !backgroundThreadEnabled())
    {
        /// Purge if the old arena has no associated threads anymore and no background threads.
        arenaDecay(&thread_state, old_arena, /* is_background_thread */ false, /* all */ true);
    }
}

/// jemalloc: arena_unbind
void arenaUnbind(ThreadState & thread_state, unsigned idx, bool internal)
{
    Arena * arena = arenaGet(&thread_state, idx, false);
    arenaNumThreadsDecrement(arena, internal);

    if (internal)
        thread_state.internal_arena = nullptr;
    else
        thread_state.arena = nullptr;
}

/// jemalloc: arena_choose_hard
Arena * arenaChooseHard(ThreadState & thread_state, bool internal)
{
    ThreadState * thread_state_ptr = &thread_state;
    Arena * result = nullptr;

    if (config::have_per_cpu_arena && perCPUArenaEnabled(options.per_cpu_arena))
    {
        unsigned choose = perCPUArenaChoose();
        result = arenaGet(thread_state_ptr, choose, true);
        /// The arena may fail to initialize (out of memory). Unlike jemalloc, report the failure to the caller
        /// instead of dereferencing a null pointer.
        if (ALLOCATOR_UNLIKELY(result == nullptr))
            return nullptr;
        arenaBind(thread_state, arenaIdxGet(result), false);
        arenaBind(thread_state, arenaIdxGet(result), true);

        return result;
    }

    if (num_arenas_auto > 1)
    {
        unsigned choose[2];
        bool is_new_arena[2];

        /// Determine binding for both non-internal and internal allocation.
        ///   choose[0]: For application allocation.
        ///   choose[1]: For internal metadata allocation.
        for (unsigned j = 0; j < 2; ++j)
        {
            choose[j] = 0;
            is_new_arena[j] = false;
        }

        unsigned first_null = num_arenas_auto;
        arenas_lock.lock(thread_state_ptr);
        ALLOCATOR_ASSERT(arenaGet(thread_state_ptr, 0, false) != nullptr);
        for (unsigned i = 1; i < num_arenas_auto; ++i)
        {
            if (arenaGet(thread_state_ptr, i, false) != nullptr)
            {
                /// Choose the first arena that has the lowest number of threads assigned to it.
                for (unsigned j = 0; j < 2; ++j)
                {
                    if (arenaNumThreadsGet(arenaGet(thread_state_ptr, i, false), !!j)
                        < arenaNumThreadsGet(arenaGet(thread_state_ptr, choose[j], false), !!j))
                        choose[j] = i;
                }
            }
            else if (first_null == num_arenas_auto)
            {
                /// Record the index of the first uninitialized arena, in case all extant arenas are in use.
                ///
                /// NB: It is possible for there to be discontinuities in terms of initialized versus uninitialized
                /// arenas, due to the "thread.arena" mallctl.
                first_null = i;
            }
        }

        /// Unlike jemalloc, select (and initialize) the arenas for both sides before binding any of them, so that
        /// an arena initialization failure (out of memory) leaves the thread unbound and the thread counts intact.
        bool init_failed = false;
        for (unsigned j = 0; j < 2; ++j)
        {
            if (arenaNumThreadsGet(arenaGet(thread_state_ptr, choose[j], false), !!j) == 0 || first_null == num_arenas_auto)
            {
                /// Use an unloaded arena, or the least loaded arena if all arenas are already initialized.
                if (!!j == internal)
                    result = arenaGet(thread_state_ptr, choose[j], false);
            }
            else
            {
                /// Initialize a new arena.
                choose[j] = first_null;
                Arena * arena = arenaInitLocked(thread_state_ptr, choose[j], &arena_config_default);
                if (arena == nullptr)
                {
                    init_failed = true;
                    break;
                }
                is_new_arena[j] = true;
                if (!!j == internal)
                    result = arena;
            }
        }
        /// Binding one side only changes the thread count of that side, so it does not affect the choice for the
        /// other side, and the result is the same as binding inside the loop above as jemalloc does.
        if (!init_failed)
        {
            for (unsigned j = 0; j < 2; ++j)
                arenaBind(thread_state, choose[j], !!j);
        }
        arenas_lock.unlock(thread_state_ptr);

        for (unsigned j = 0; j < 2; ++j)
        {
            if (is_new_arena[j])
            {
                ALLOCATOR_ASSERT(choose[j] > 0);
                arenaNewCreateBackgroundThread(thread_state_ptr, choose[j]);
            }
        }

        /// An arena created for the other side stays initialized (and gets its background thread above) and can be
        /// chosen by later threads.
        if (init_failed)
            return nullptr;
    }
    else
    {
        result = arenaGet(thread_state_ptr, 0, false);
        arenaBind(thread_state, 0, false);
        arenaBind(thread_state, 0, true);
    }

    return result;
}

/// The cold part of `arena_choose_impl` (jemalloc_internal_inlines_b.h), when the thread has no arena yet.
Arena * arenaChooseFirstUse(ThreadState & thread_state, bool internal)
{
    Arena * result = arenaChooseHard(thread_state, internal);
    /// Out of memory while initializing an arena: the thread stays unbound, and the allocation fails.
    if (ALLOCATOR_UNLIKELY(result == nullptr))
        return nullptr;
    if (threadCacheAvailable(thread_state))
    {
        ThreadCacheSlow * thread_cache_slow = thread_state.threadCacheSlowGet();
        ThreadCache * thread_cache = thread_state.threadCacheGet();
        if (thread_cache_slow->arena != nullptr)
        {
            /// See comments in `threadCacheThreadStateDataInit`.
            ALLOCATOR_ASSERT(thread_cache_slow->arena == arenaGet(&thread_state, 0, false));
            if (thread_cache_slow->arena != result)
                threadCacheArenaReassociate(&thread_state, thread_cache_slow, thread_cache, result);
        }
        else
        {
            threadCacheArenaAssociate(&thread_state, thread_cache_slow, thread_cache, result);
        }
    }
    return result;
}

/// jemalloc: percpu_arena_update
void perCPUArenaUpdate(ThreadState & thread_state, unsigned cpu)
{
    ALLOCATOR_ASSERT(config::have_per_cpu_arena);
    Arena * old_arena = thread_state.arena;
    ALLOCATOR_ASSERT(old_arena != nullptr);
    unsigned old_idx = arenaIdxGet(old_arena);

    if (old_idx != cpu)
    {
        unsigned new_idx = cpu;
        Arena * new_arena = arenaGet(&thread_state, new_idx, true);
        /// Out of memory while initializing the arena of the new CPU: keep the thread bound to its current arena,
        /// which remains fully usable.
        if (ALLOCATOR_UNLIKELY(new_arena == nullptr))
            return;

        /// Set new arena/tcache associations.
        arenaMigrate(thread_state, old_arena, new_arena);
        ThreadCache * thread_cache = threadCacheGet(thread_state);
        if (thread_cache != nullptr)
        {
            ThreadCacheSlow * thread_cache_slow = thread_state.threadCacheSlowGet();
            ALLOCATOR_ASSERT(thread_cache_slow->arena != nullptr);
            threadCacheArenaReassociate(&thread_state, thread_cache_slow, thread_cache, new_arena);
        }
    }
}

/// jemalloc: iarena_cleanup
void internalArenaCleanup(ThreadState & thread_state)
{
    Arena * internal_arena = thread_state.internal_arena;
    if (internal_arena != nullptr)
        arenaUnbind(thread_state, arenaIdxGet(internal_arena), true);
}

/// jemalloc: arena_cleanup
void arenaCleanup(ThreadState & thread_state)
{
    Arena * arena = thread_state.arena;
    if (arena != nullptr)
        arenaUnbind(thread_state, arenaIdxGet(arena), false);
}

}
