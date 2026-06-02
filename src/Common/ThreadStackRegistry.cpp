#include <Common/ThreadStackRegistry.h>

#include <Common/SharedMutex.h>
#include <base/scope_guard.h>

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

[[maybe_unused]] void registryInsert(uintptr_t base)
{
    if (base == 0)
        return;
    std::unique_lock lock(registryMutex());
    registry().insert(base);
}

[[maybe_unused]] void registryErase(uintptr_t base)
{
    if (base == 0)
        return;
    std::unique_lock lock(registryMutex());
    registry().erase(base);
}

/// One instance per OS thread; constructs lazily on first
/// `ensureCurrentThreadRegistered` call, destructs when the thread exits.
///
/// The constructor is `noexcept`: registration is best-effort and must not
/// propagate exceptions to call sites in thread-entry hot paths. If
/// `registryInsert` throws (e.g. unordered_set rehash under OOM), the scope
/// guard still runs `pthread_attr_destroy` and `base` stays 0 so the
/// destructor does not attempt to erase.
struct ThreadStackTracker
{
    uintptr_t base = 0;

    ThreadStackTracker() noexcept
    {
#if defined(OS_LINUX)
        try
        {
            pthread_attr_t attr;
            if (pthread_getattr_np(pthread_self(), &attr) != 0)
                return;
            SCOPE_EXIT({ pthread_attr_destroy(&attr); });

            void * addr = nullptr;
            size_t size = 0;
            if (pthread_attr_getstack(&attr, &addr, &size) != 0 || addr == nullptr)
                return;

            uintptr_t candidate = reinterpret_cast<uintptr_t>(addr);
            registryInsert(candidate);
            base = candidate;
        }
        catch (...) /// NOLINT(bugprone-empty-catch) Ok, best-effort registration; surfaced metric will undercount.
        {
        }
#endif
    }

    ~ThreadStackTracker()
    {
        if (base)
        {
            try
            {
                registryErase(base);
            }
            catch (...) /// NOLINT(bugprone-empty-catch) Ok, ~ThreadStackTracker runs during thread exit.
            {
            }
        }
    }
};

}

void ThreadStackRegistry::ensureCurrentThreadRegistered() noexcept
{
    /// First access on this thread constructs the tracker (registers the
    /// stack); destructor runs on thread exit (deregisters). The tracker
    /// constructor itself is noexcept, but `thread_local` initialization
    /// could still throw on out-of-memory for the TLS slot on some libc
    /// versions; swallow defensively so callers in thread-entry hot paths
    /// never see an exception from registration.
    try
    {
        thread_local ThreadStackTracker tracker;
        (void)tracker;
    }
    catch (...) /// NOLINT(bugprone-empty-catch) Ok, registration is advisory; metric will undercount this thread.
    {
    }
}

std::unordered_set<uintptr_t> ThreadStackRegistry::snapshot()
{
    std::shared_lock lock(registryMutex());
    return registry();
}

}
