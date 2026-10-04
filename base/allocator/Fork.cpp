#include <allocator/Init.h>

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Ctl.h>
#include <allocator/Frontend.h>
#include <allocator/ProfHooks.h>
#include <allocator/Stats.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

/// The functions used by threading libraries for protection of malloc during fork (jemalloc: `jemalloc_prefork`,
/// `jemalloc_postfork_parent`, `jemalloc_postfork_child` in `src/jemalloc.c`). The lock acquisition order must be
/// preserved exactly. Registration: see `mallocInitHardRecursible` (Linux), `_malloc_prefork` / `_malloc_postfork`
/// below (FreeBSD), the zone's `force_lock` / `force_unlock` (Darwin).

namespace jemalloc
{

/// jemalloc: jemalloc_prefork (`_malloc_prefork` with `JEMALLOC_MUTEX_INIT_CB`)
void jemallocPrefork()
{
    if constexpr (config::mutex_init_cb)
    {
        if (!mallocInitialized())
            return;
    }
    JE_ASSERT(mallocInitialized());

    ThreadState & tsd = ThreadState::fetch();
    ThreadState * tsdn = &tsd;

    unsigned narenas = narenasTotalGet();

    /// `witness_prefork`: there is no witness.
    /// Acquire all mutexes in a safe order.
    ctlPrefork(tsdn);
    tcachePrefork(tsdn);
    arenas_lock.prefork(tsdn);
    if constexpr (config::background_thread)
        backgroundThreadPrefork0(tsdn);
    profPrefork0(tsdn);
    if constexpr (config::background_thread)
        backgroundThreadPrefork1(tsdn);
    /// Break arena prefork into stages to preserve lock order.
    for (unsigned i = 0; i < 9; ++i)
    {
        for (unsigned j = 0; j < narenas; ++j)
        {
            Arena * arena = arenaGet(tsdn, j, false);
            if (arena != nullptr)
            {
                switch (i)
                {
                    case 0:
                        arenaPrefork0(tsdn, arena);
                        break;
                    case 1:
                        arenaPrefork1(tsdn, arena);
                        break;
                    case 2:
                        arenaPrefork2(tsdn, arena);
                        break;
                    case 3:
                        arenaPrefork3(tsdn, arena);
                        break;
                    case 4:
                        arenaPrefork4(tsdn, arena);
                        break;
                    case 5:
                        arenaPrefork5(tsdn, arena);
                        break;
                    case 6:
                        arenaPrefork6(tsdn, arena);
                        break;
                    case 7:
                        arenaPrefork7(tsdn, arena);
                        break;
                    case 8:
                        arenaPrefork8(tsdn, arena);
                        break;
                    default:
                        JE_NOT_REACHED();
                }
            }
        }
    }
    profPrefork1(tsdn);
    statsPrefork(tsdn);
    tsd.prefork();
}

/// jemalloc: jemalloc_postfork_parent (`_malloc_postfork` with `JEMALLOC_MUTEX_INIT_CB`)
void jemallocPostforkParent()
{
    if constexpr (config::mutex_init_cb)
    {
        if (!mallocInitialized())
            return;
    }
    JE_ASSERT(mallocInitialized());

    ThreadState & tsd = ThreadState::fetch();
    ThreadState * tsdn = &tsd;

    tsd.postforkParent();

    /// `witness_postfork_parent`: there is no witness.
    /// Release all mutexes, now that fork() has completed.
    statsPostforkParent(tsdn);
    for (unsigned i = 0, narenas = narenasTotalGet(); i < narenas; ++i)
    {
        Arena * arena = arenaGet(tsdn, i, false);
        if (arena != nullptr)
            arenaPostforkParent(tsdn, arena);
    }
    profPostforkParent(tsdn);
    if constexpr (config::background_thread)
        backgroundThreadPostforkParent(tsdn);
    arenas_lock.postforkParent(tsdn);
    tcachePostforkParent(tsdn);
    ctlPostforkParent(tsdn);
}

/// jemalloc: jemalloc_postfork_child
void jemallocPostforkChild()
{
    JE_ASSERT(mallocInitialized());

    ThreadState & tsd = ThreadState::fetch();
    ThreadState * tsdn = &tsd;

    tsd.postforkChild();

    /// `witness_postfork_child`: there is no witness.
    /// Release all mutexes, now that fork() has completed.
    statsPostforkChild(tsdn);
    for (unsigned i = 0, narenas = narenasTotalGet(); i < narenas; ++i)
    {
        Arena * arena = arenaGet(tsdn, i, false);
        if (arena != nullptr)
            arenaPostforkChild(tsdn, arena);
    }
    profPostforkChild(tsdn);
    if constexpr (config::background_thread)
        backgroundThreadPostforkChild(tsdn);
    arenas_lock.postforkChild(tsdn);
    tcachePostforkChild(tsdn);
    ctlPostforkChild(tsdn);
}

}

#if defined(__FreeBSD__)
/// FreeBSD's libc calls these around `fork` (`JEMALLOC_MUTEX_INIT_CB`); the parent version is also used for the child.
/// jemalloc: _malloc_prefork, _malloc_postfork
extern "C" __attribute__((visibility("default"))) void _malloc_prefork()
{
    jemalloc::jemallocPrefork();
}

extern "C" __attribute__((visibility("default"))) void _malloc_postfork()
{
    jemalloc::jemallocPostforkParent();
}
#endif
