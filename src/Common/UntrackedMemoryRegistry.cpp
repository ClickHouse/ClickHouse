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
