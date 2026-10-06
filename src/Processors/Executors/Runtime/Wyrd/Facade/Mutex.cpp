#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#    include <Processors/Executors/Runtime/Wyrd/Simulation/InjectFault.h>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#    include <Processors/Executors/Runtime/Wyrd/Adversary/InjectFault.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

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
