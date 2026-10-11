#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/InjectFault.h>
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#    include <fmt/format.h>
#    include <utility>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

void Mutex::lock(std::source_location location)
{
    while (locked.exchange(1, std::memory_order::seq_cst, location) == 1)
    {
        UInt32 still_locked = 1;
        futexWaitFetch(locked, still_locked, location);
    }
    trace(fmt::format("{}: mutex::lock -> acquired", locked.getName()));
}

bool Mutex::try_lock(std::source_location location)
{
    if (spuriousFailure(location))
    {
        trace(fmt::format("{}: mutex::try_lock -> false, spurious", locked.getName()));
        return false;
    }

    UInt32 unlocked = 0;
    const bool acquired = locked.compare_exchange_strong(unlocked, 1, std::memory_order::seq_cst, std::memory_order::seq_cst, location);
    trace(fmt::format("{}: mutex::try_lock -> {}", locked.getName(), acquired));
    return acquired;
}

void Mutex::unlock(std::source_location location)
{
    locked.store(0, std::memory_order::seq_cst, location);
    futexWakeOne(locked, location);
}

void Mutex::setName(std::string name)
{
    locked.setName(std::move(name));
}

#elif defined(WYRD_ADVERSARY)

void Mutex::lock(std::source_location location)
{
    while (locked.exchange(1, std::memory_order::seq_cst, location) == 1)
    {
        UInt32 still_locked = 1;
        futexWaitFetch(locked, still_locked, location);
    }
}

bool Mutex::try_lock(std::source_location location)
{
    if (spuriousFailure(location))
        return false;

    UInt32 unlocked = 0;
    return locked.compare_exchange_strong(unlocked, 1, std::memory_order::seq_cst, std::memory_order::seq_cst, location);
}

void Mutex::unlock(std::source_location location)
{
    locked.store(0, std::memory_order::seq_cst, location);
    futexWakeOne(locked, location);
}

#endif

}
