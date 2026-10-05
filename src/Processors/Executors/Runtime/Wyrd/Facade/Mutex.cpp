#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

void Mutex::lock(std::source_location location)
{
    while (locked.exchange(1, location) == 1)
    {
        UInt32 still_locked = 1;
        futexWaitFetch(locked, still_locked, location);
    }
}

void Mutex::unlock(std::source_location location)
{
    locked.store(0, location);
    futexWakeOne(locked, location);
}

#endif

}
