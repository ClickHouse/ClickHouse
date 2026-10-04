#include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Futex.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/InjectFault.h>
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Syscall.h>
#    include <limits>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/InjectFault.h>
#    include <Common/futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value)
{
    if (address.load() == value)
        futexWait(&address);
    value = address.load();
}

void futexWakeOne(Atomic<UInt32> & address)
{
    injectFault("futex::wakeOne");
    futexWake(&address, 1);
}

void futexWakeAll(Atomic<UInt32> & address)
{
    injectFault("futex::wakeAll");
    futexWake(&address, std::numeric_limits<size_t>::max());
}

#elif defined(WYRD_ADVERSARY)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value)
{
    injectFault("futex::waitFetch");
    if (spuriousFailure())
        value = address.load();
    else
        DB::futexWaitFetch(address.underlying(), value);
}

void futexWakeOne(Atomic<UInt32> & address)
{
    injectFault("futex::wakeOne");
    DB::futexWakeOne(address.underlying());
}

void futexWakeAll(Atomic<UInt32> & address)
{
    injectFault("futex::wakeAll");
    DB::futexWakeAll(address.underlying());
}

#endif

}
