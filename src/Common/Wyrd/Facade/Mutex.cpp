#include <Common/Wyrd/Facade/Mutex.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Facade/Futex.h>
#    include <Common/Wyrd/Strategies/Simulation/InjectFault.h>
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Adversary/InjectFault.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <fmt/format.h>
#    include <utility>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

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

}

namespace AdversaryMode
{

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

}

WyrdMutex::WyrdMutex()
{
    if (getMode() == Mode::Simulation)
        simulation_impl.emplace();
    else
        adversary_impl.emplace();
}

void WyrdMutex::lock(std::source_location location)
{
    if (getMode() == Mode::Simulation)
        simulation_impl->lock(location);
    else
        adversary_impl->lock(location);
}

bool WyrdMutex::try_lock(std::source_location location)
{
    if (getMode() == Mode::Simulation)
        return simulation_impl->try_lock(location);
    else
        return adversary_impl->try_lock(location);
}

void WyrdMutex::unlock(std::source_location location)
{
    if (getMode() == Mode::Simulation)
        simulation_impl->unlock(location);
    else
        adversary_impl->unlock(location);
}

SimulationMode::Mutex & WyrdMutex::simulation()
{
    return *simulation_impl;
}

AdversaryMode::Mutex & WyrdMutex::adversary()
{
    return *adversary_impl;
}

#endif

}
