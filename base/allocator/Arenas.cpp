#include <allocator/Arenas.h>

#include <allocator/ArenaInlines.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Format.h>
#include <allocator/ThreadCache.h>

#include <cstdlib>

namespace jemalloc
{

alignas(CACHELINE) constinit std::atomic<Arena *> arenas[MALLOCX_ARENA_LIMIT] = {};
constinit std::atomic<unsigned> narenas_total{0};
constinit Arena * a0 = nullptr;
constinit unsigned narenas_auto = 0;
constinit unsigned manual_arena_base = 0;
constinit Mutex arenas_lock;

/// --- Bootstrap allocation ------------------------------------------------------------------------------------------

/// jemalloc: a0ialloc
void * a0ialloc(size_t size, bool zero, bool is_internal)
{
    if (JE_UNLIKELY(mallocInitA0()))
        return nullptr;

    /// iallocztm(TSDN_NULL, size, sz_size2index(size), zero, NULL, is_internal, arena_get(TSDN_NULL, 0, true), true)
    ThreadState * tsdn = nullptr;
    szind_t ind = sz::sizeToIndex(size);
    Arena * arena = arenaGet(tsdn, 0, true);
    bool slab = sz::canUseSlab(size);
    void * ret = arenaMalloc(tsdn, arena, size, ind, zero, slab, nullptr, true);
    if (config::stats && is_internal && JE_LIKELY(ret != nullptr))
        arenaInternalAdd(arenaAalloc(tsdn, ret), arenaSalloc(tsdn, ret));
    return ret;
}

/// jemalloc: a0idalloc
void a0idalloc(void * ptr, bool is_internal)
{
    /// idalloctm(TSDN_NULL, ptr, NULL, NULL, is_internal, true)
    ThreadState * tsdn = nullptr;
    JE_ASSERT(ptr != nullptr);
    if (config::stats && is_internal)
        arenaInternalSub(arenaAalloc(tsdn, ptr), arenaSalloc(tsdn, ptr));
    arenaDalloc(tsdn, ptr, nullptr, nullptr, true);
}

/// jemalloc: a0malloc
void * a0malloc(size_t size)
{
    return a0ialloc(size, false, true);
}

/// jemalloc: a0dalloc
void a0dalloc(void * ptr)
{
    a0idalloc(ptr, true);
}

/// jemalloc: bootstrap_malloc
void * bootstrapMalloc(size_t size)
{
    if (JE_UNLIKELY(size == 0))
        size = 1;

    return a0ialloc(size, false, false);
}

/// jemalloc: bootstrap_calloc
void * bootstrapCalloc(size_t num, size_t size)
{
    size_t num_size = num * size;
    if (JE_UNLIKELY(num_size == 0))
    {
        JE_ASSERT(num == 0 || size == 0);
        num_size = 1;
    }

    return a0ialloc(num_size, true, false);
}

/// jemalloc: bootstrap_free
void bootstrapFree(void * ptr)
{
    if (JE_UNLIKELY(ptr == nullptr))
        return;

    a0idalloc(ptr, false);
}

/// --- Creation ------------------------------------------------------------------------------------------------------

/// jemalloc: arena_init_locked
Arena * arenaInitLocked(ThreadState * tsdn, unsigned ind, const ArenaConfig * config)
{
    JE_ASSERT(ind <= narenasTotalGet());
    if (ind >= MALLOCX_ARENA_LIMIT)
        return nullptr;
    if (ind == narenasTotalGet())
        narenasTotalInc();

    /// Another thread may have already initialized arenas[ind] if it's an auto arena.
    Arena * arena = arenaGet(tsdn, ind, false);
    if (arena != nullptr)
    {
        JE_ASSERT(arenaIsAuto(arena));
        return arena;
    }

    /// Actually initialize the arena.
    arena = arenaNew(tsdn, ind, config);

    return arena;
}

/// jemalloc: arena_new_create_background_thread
static void arenaNewCreateBackgroundThread(ThreadState * tsdn, unsigned ind)
{
    if (ind == 0)
        return;

    if constexpr (config::background_thread)
    {
        if (backgroundThreadCreate(*tsdn, ind))
        {
            printMessage("<jemalloc>: error in background thread creation for arena %u. Abort.\n", ind);
            abort();
        }
    }
}

/// jemalloc: arena_init
Arena * arenaInit(ThreadState * tsdn, unsigned ind, const ArenaConfig * config)
{
    arenas_lock.lock(tsdn);
    Arena * arena = arenaInitLocked(tsdn, ind, config);
    arenas_lock.unlock(tsdn);

    arenaNewCreateBackgroundThread(tsdn, ind);

    return arena;
}

/// --- Binding -------------------------------------------------------------------------------------------------------

/// jemalloc: arena_bind
void arenaBind(ThreadState & tsd, unsigned ind, bool internal)
{
    Arena * arena = arenaGet(&tsd, ind, false);
    arenaNthreadsInc(arena, internal);

    if (internal)
    {
        tsd.iarena = arena;
    }
    else
    {
        tsd.arena = arena;
        /// While shard acts as a random seed, the cast below should not make much difference.
        uint8_t shard = uint8_t(arena->binshard_next.fetch_add(1, std::memory_order_relaxed));
        TsdBinshards * bins = &tsd.binshards;
        for (unsigned i = 0; i < SC_NBINS; ++i)
        {
            JE_ASSERT(bin_infos[i].n_shards > 0 && bin_infos[i].n_shards <= BIN_SHARDS_MAX);
            bins->binshard[i] = uint8_t(shard % bin_infos[i].n_shards);
        }
    }
}

/// jemalloc: arena_migrate
void arenaMigrate(ThreadState & tsd, Arena * oldarena, Arena * newarena)
{
    JE_ASSERT(oldarena != nullptr);
    JE_ASSERT(newarena != nullptr);

    arenaNthreadsDec(oldarena, false);
    arenaNthreadsInc(newarena, false);
    tsd.arena = newarena;

    if (arenaNthreadsGet(oldarena, false) == 0 && !backgroundThreadEnabled())
    {
        /// Purge if the old arena has no associated threads anymore and no background threads.
        arenaDecay(&tsd, oldarena, /* is_background_thread */ false, /* all */ true);
    }
}

/// jemalloc: arena_unbind
void arenaUnbind(ThreadState & tsd, unsigned ind, bool internal)
{
    Arena * arena = arenaGet(&tsd, ind, false);
    arenaNthreadsDec(arena, internal);

    if (internal)
        tsd.iarena = nullptr;
    else
        tsd.arena = nullptr;
}

/// jemalloc: arena_choose_hard
Arena * arenaChooseHard(ThreadState & tsd, bool internal)
{
    ThreadState * tsdn = &tsd;
    Arena * ret = nullptr;

    if (config::have_percpu_arena && percpuArenaEnabled(opt.percpu_arena))
    {
        unsigned choose = percpuArenaChoose();
        ret = arenaGet(tsdn, choose, true);
        JE_ASSERT(ret != nullptr);
        arenaBind(tsd, arenaIndGet(ret), false);
        arenaBind(tsd, arenaIndGet(ret), true);

        return ret;
    }

    if (narenas_auto > 1)
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

        unsigned first_null = narenas_auto;
        arenas_lock.lock(tsdn);
        JE_ASSERT(arenaGet(tsdn, 0, false) != nullptr);
        for (unsigned i = 1; i < narenas_auto; ++i)
        {
            if (arenaGet(tsdn, i, false) != nullptr)
            {
                /// Choose the first arena that has the lowest number of threads assigned to it.
                for (unsigned j = 0; j < 2; ++j)
                {
                    if (arenaNthreadsGet(arenaGet(tsdn, i, false), !!j) < arenaNthreadsGet(arenaGet(tsdn, choose[j], false), !!j))
                        choose[j] = i;
                }
            }
            else if (first_null == narenas_auto)
            {
                /// Record the index of the first uninitialized arena, in case all extant arenas are in use.
                ///
                /// NB: It is possible for there to be discontinuities in terms of initialized versus uninitialized
                /// arenas, due to the "thread.arena" mallctl.
                first_null = i;
            }
        }

        for (unsigned j = 0; j < 2; ++j)
        {
            if (arenaNthreadsGet(arenaGet(tsdn, choose[j], false), !!j) == 0 || first_null == narenas_auto)
            {
                /// Use an unloaded arena, or the least loaded arena if all arenas are already initialized.
                if (!!j == internal)
                    ret = arenaGet(tsdn, choose[j], false);
            }
            else
            {
                /// Initialize a new arena.
                choose[j] = first_null;
                Arena * arena = arenaInitLocked(tsdn, choose[j], &arena_config_default);
                if (arena == nullptr)
                {
                    arenas_lock.unlock(tsdn);
                    return nullptr;
                }
                is_new_arena[j] = true;
                if (!!j == internal)
                    ret = arena;
            }
            arenaBind(tsd, choose[j], !!j);
        }
        arenas_lock.unlock(tsdn);

        for (unsigned j = 0; j < 2; ++j)
        {
            if (is_new_arena[j])
            {
                JE_ASSERT(choose[j] > 0);
                arenaNewCreateBackgroundThread(tsdn, choose[j]);
            }
        }
    }
    else
    {
        ret = arenaGet(tsdn, 0, false);
        arenaBind(tsd, 0, false);
        arenaBind(tsd, 0, true);
    }

    return ret;
}

/// The cold part of `arena_choose_impl` (jemalloc_internal_inlines_b.h), when the thread has no arena yet.
Arena * arenaChooseFirstUse(ThreadState & tsd, bool internal)
{
    Arena * ret = arenaChooseHard(tsd, internal);
    JE_ASSERT(ret);
    if (tcacheAvailable(tsd))
    {
        ThreadCacheSlow * tcache_slow = tsd.tcacheSlowGet();
        ThreadCache * tcache = tsd.tcacheGet();
        if (tcache_slow->arena != nullptr)
        {
            /// See comments in `tcacheTsdDataInit`.
            JE_ASSERT(tcache_slow->arena == arenaGet(&tsd, 0, false));
            if (tcache_slow->arena != ret)
                tcacheArenaReassociate(&tsd, tcache_slow, tcache, ret);
        }
        else
        {
            tcacheArenaAssociate(&tsd, tcache_slow, tcache, ret);
        }
    }
    return ret;
}

/// jemalloc: percpu_arena_update
void percpuArenaUpdate(ThreadState & tsd, unsigned cpu)
{
    JE_ASSERT(config::have_percpu_arena);
    Arena * oldarena = tsd.arena;
    JE_ASSERT(oldarena != nullptr);
    unsigned oldind = arenaIndGet(oldarena);

    if (oldind != cpu)
    {
        unsigned newind = cpu;
        Arena * newarena = arenaGet(&tsd, newind, true);
        JE_ASSERT(newarena != nullptr);

        /// Set new arena/tcache associations.
        arenaMigrate(tsd, oldarena, newarena);
        ThreadCache * tcache = tcacheGet(tsd);
        if (tcache != nullptr)
        {
            ThreadCacheSlow * tcache_slow = tsd.tcacheSlowGet();
            JE_ASSERT(tcache_slow->arena != nullptr);
            tcacheArenaReassociate(&tsd, tcache_slow, tcache, newarena);
        }
    }
}

/// jemalloc: iarena_cleanup
void iarenaCleanup(ThreadState & tsd)
{
    Arena * iarena = tsd.iarena;
    if (iarena != nullptr)
        arenaUnbind(tsd, arenaIndGet(iarena), true);
}

/// jemalloc: arena_cleanup
void arenaCleanup(ThreadState & tsd)
{
    Arena * arena = tsd.arena;
    if (arena != nullptr)
        arenaUnbind(tsd, arenaIndGet(arena), false);
}

}
