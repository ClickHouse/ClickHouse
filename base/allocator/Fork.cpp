#include <allocator/Init.h>

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Frontend.h>
#include <allocator/Mallctl.h>
#include <allocator/ProfilingHooks.h>
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
    if constexpr (config::mutex_init_callback)
    {
        if (!mallocInitialized())
            return;
    }
    ALLOCATOR_ASSERT(mallocInitialized());

    ThreadState & thread_state = ThreadState::fetch();
    ThreadState * thread_state_ptr = &thread_state;

    unsigned num_arenas = numArenasTotalGet();

    /// `witness_prefork`: there is no witness.
    /// Acquire all mutexes in a safe order.
    mallctlPrefork(thread_state_ptr);
    threadCachePrefork(thread_state_ptr);
    arenas_lock.prefork(thread_state_ptr);
    if constexpr (config::background_thread)
        backgroundThreadPrefork0(thread_state_ptr);
    profilingPrefork0(thread_state_ptr);
    if constexpr (config::background_thread)
        backgroundThreadPrefork1(thread_state_ptr);
    /// Break arena prefork into stages to preserve lock order.
    for (unsigned i = 0; i < 9; ++i)
    {
        for (unsigned j = 0; j < num_arenas; ++j)
        {
            Arena * arena = arenaGet(thread_state_ptr, j, false);
            if (arena != nullptr)
            {
                switch (i)
                {
                    case 0: arenaPrefork0(thread_state_ptr, arena); break;
                    case 1: arenaPrefork1(thread_state_ptr, arena); break;
                    case 2: arenaPrefork2(thread_state_ptr, arena); break;
                    case 3: arenaPrefork3(thread_state_ptr, arena); break;
                    case 4: arenaPrefork4(thread_state_ptr, arena); break;
                    case 5: arenaPrefork5(thread_state_ptr, arena); break;
                    case 6: arenaPrefork6(thread_state_ptr, arena); break;
                    case 7: arenaPrefork7(thread_state_ptr, arena); break;
                    case 8: arenaPrefork8(thread_state_ptr, arena); break;
                    default: ALLOCATOR_NOT_REACHED();
                }
            }
        }
    }
    profilingPrefork1(thread_state_ptr);
    statsPrefork(thread_state_ptr);
    thread_state.prefork();
}

/// jemalloc: jemalloc_postfork_parent (`_malloc_postfork` with `JEMALLOC_MUTEX_INIT_CB`)
void jemallocPostforkParent()
{
    if constexpr (config::mutex_init_callback)
    {
        if (!mallocInitialized())
            return;
    }
    ALLOCATOR_ASSERT(mallocInitialized());

    ThreadState & thread_state = ThreadState::fetch();
    ThreadState * thread_state_ptr = &thread_state;

    thread_state.postforkParent();

    /// `witness_postfork_parent`: there is no witness.
    /// Release all mutexes, now that fork() has completed.
    statsPostforkParent(thread_state_ptr);
    for (unsigned i = 0, num_arenas = numArenasTotalGet(); i < num_arenas; ++i)
    {
        Arena * arena = arenaGet(thread_state_ptr, i, false);
        if (arena != nullptr)
            arenaPostforkParent(thread_state_ptr, arena);
    }
    profilingPostforkParent(thread_state_ptr);
    if constexpr (config::background_thread)
        backgroundThreadPostforkParent(thread_state_ptr);
    arenas_lock.postforkParent(thread_state_ptr);
    threadCachePostforkParent(thread_state_ptr);
    mallctlPostforkParent(thread_state_ptr);
}

/// jemalloc: jemalloc_postfork_child
void jemallocPostforkChild()
{
    ALLOCATOR_ASSERT(mallocInitialized());

    ThreadState & thread_state = ThreadState::fetch();
    ThreadState * thread_state_ptr = &thread_state;

    thread_state.postforkChild();

    /// `witness_postfork_child`: there is no witness.
    /// Release all mutexes, now that fork() has completed.
    statsPostforkChild(thread_state_ptr);
    for (unsigned i = 0, num_arenas = numArenasTotalGet(); i < num_arenas; ++i)
    {
        Arena * arena = arenaGet(thread_state_ptr, i, false);
        if (arena != nullptr)
            arenaPostforkChild(thread_state_ptr, arena);
    }
    profilingPostforkChild(thread_state_ptr);
    if constexpr (config::background_thread)
        backgroundThreadPostforkChild(thread_state_ptr);
    arenas_lock.postforkChild(thread_state_ptr);
    threadCachePostforkChild(thread_state_ptr);
    mallctlPostforkChild(thread_state_ptr);
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
