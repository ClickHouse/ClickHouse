#include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Mutex.h>

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)

void Mutex::lock()
{
    while (locked.exchange(1) == 1)
    {
        UInt32 still_locked = 1;
        futexWaitFetch(locked, still_locked);
    }
}

void Mutex::unlock()
{
    locked.store(0);
    futexWakeOne(locked);
}

#endif

}
