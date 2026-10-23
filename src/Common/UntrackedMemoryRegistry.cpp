#include <Common/UntrackedMemoryRegistry.h>
#include <Common/MemoryTracker.h>


namespace DB
{

UntrackedMemoryCounter::UntrackedMemoryCounter()
{
    UntrackedMemoryRegistry::instance().add(this);
}

UntrackedMemoryCounter::~UntrackedMemoryCounter()
{
    UntrackedMemoryRegistry::instance().remove(this);
}


UntrackedMemoryRegistry & UntrackedMemoryRegistry::instance()
{
    /// Function-local static, destroyed during static destruction.
    /// Counters live in ThreadStatus and unregister themselves in the destructor,
    /// so every thread that owns a ThreadStatus must be joined before main returns
    /// (GlobalThreadPool::shutdown, StaticThreadPool::shutdownAll in each entry point);
    /// otherwise the counter destructor touches an already destroyed registry.
    /// Poco's default thread pool needs special care: its singleton is a namespace-scope
    /// static constructed before main, hence destroyed *after* this registry, and its
    /// pooled threads create a ThreadStatus in PooledThread::run - such threads must be
    /// stopped explicitly via Poco::ThreadPool::defaultPool().stopAll().
    static UntrackedMemoryRegistry registry;
    return registry;
}

void UntrackedMemoryRegistry::add(UntrackedMemoryCounter * counter)
{
    std::lock_guard lock(mutex);
    DENY_ALLOCATIONS_IN_SCOPE;
    counters.push_back(*counter);
}

void UntrackedMemoryRegistry::remove(UntrackedMemoryCounter * counter)
{
    std::lock_guard lock(mutex);
    DENY_ALLOCATIONS_IN_SCOPE;
    counters.erase(counters.iterator_to(*counter));
}

Int64 UntrackedMemoryRegistry::sum() const
{
    std::lock_guard lock(mutex);
    DENY_ALLOCATIONS_IN_SCOPE;
    Int64 total = 0;
    for (const auto & counter : counters)
        total += counter.load();
    return total;
}

}
