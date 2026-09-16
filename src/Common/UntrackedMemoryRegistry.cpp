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
    /// Never destroyed. Counters live in ThreadStatus and unregister themselves in the
    /// destructor, and a thread owning a ThreadStatus can outlive static destruction: the
    /// libFuzzer entry points do not own main, so they cannot join the global thread pool
    /// before it runs. A destroyed registry would then be written to by ~UntrackedMemoryCounter.
    /// The object stays reachable through this pointer, so it is not reported as a leak.
    static UntrackedMemoryRegistry * registry = new UntrackedMemoryRegistry;
    return *registry;
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
