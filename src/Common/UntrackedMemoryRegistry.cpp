#include <Common/UntrackedMemoryRegistry.h>


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
    counters.insert(counter);
}

void UntrackedMemoryRegistry::remove(UntrackedMemoryCounter * counter)
{
    std::lock_guard lock(mutex);
    counters.erase(counter);
}

Int64 UntrackedMemoryRegistry::sum() const
{
    Int64 total = 0;
    std::lock_guard lock(mutex);
    for (const UntrackedMemoryCounter * counter : counters)
        total += counter->load();
    return total;
}

}
