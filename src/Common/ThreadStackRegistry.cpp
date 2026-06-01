#include <Common/ThreadStackRegistry.h>

#include <Common/SharedMutex.h>

#include <mutex>
#include <shared_mutex>

#if defined(OS_LINUX)
#include <pthread.h>
#endif

namespace DB
{

namespace
{

/// The mutex and the underlying set must outlive every thread, because
/// thread_local destructors fire during process exit and can run after
/// function-local statics have already been destroyed (workers joined
/// during late atexit, for example). We construct them once via placement
/// new into static aligned storage and never destroy them. No heap
/// allocation, so ASAN's leak checker is not affected.

SharedMutex & registryMutex()
{
    alignas(SharedMutex) static char storage[sizeof(SharedMutex)];
    static SharedMutex * mutex = new (&storage) SharedMutex;
    return *mutex;
}

std::unordered_set<uintptr_t> & registry()
{
    using Set = std::unordered_set<uintptr_t>;
    alignas(Set) static char storage[sizeof(Set)];
    static Set * stacks = new (&storage) Set;
    return *stacks;
}

void registryInsert(uintptr_t base)
{
    if (base == 0)
        return;
    std::unique_lock lock(registryMutex());
    registry().insert(base);
}

void registryErase(uintptr_t base)
{
    if (base == 0)
        return;
    std::unique_lock lock(registryMutex());
    registry().erase(base);
}

/// One instance per OS thread; constructs lazily on first
/// `ensureCurrentThreadRegistered` call, destructs when the thread exits.
struct ThreadStackTracker
{
    uintptr_t base = 0;

    ThreadStackTracker()
    {
#if defined(OS_LINUX)
        pthread_attr_t attr;
        if (pthread_getattr_np(pthread_self(), &attr) == 0)
        {
            void * addr = nullptr;
            size_t size = 0;
            if (pthread_attr_getstack(&attr, &addr, &size) == 0 && addr != nullptr)
            {
                base = reinterpret_cast<uintptr_t>(addr);
                registryInsert(base);
            }
            pthread_attr_destroy(&attr);
        }
#endif
    }

    ~ThreadStackTracker()
    {
        if (base)
            registryErase(base);
    }
};

}

void ThreadStackRegistry::ensureCurrentThreadRegistered()
{
    /// First access on this thread constructs the tracker (registers the
    /// stack); destructor runs on thread exit (deregisters).
    thread_local ThreadStackTracker tracker;
    (void)tracker;
}

std::unordered_set<uintptr_t> ThreadStackRegistry::snapshot()
{
    std::shared_lock lock(registryMutex());
    return registry();
}

}
