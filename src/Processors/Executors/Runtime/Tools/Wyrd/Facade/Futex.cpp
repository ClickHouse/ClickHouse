#include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Futex.h>

#if defined(EXECUTOR_SIM)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/InjectFault.h>
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Syscall.h>
#    include <limits>
#elif defined(EXECUTOR_FAULT_INJECTION)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/InjectFault.h>
#    include <Common/futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(EXECUTOR_SIM)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value)
{
    if (address.load() == value)
        futexWait(&address);
    value = address.load();
}

void futexWakeOne(Atomic<UInt32> & address)
{
    injectFaultBefore("futex::wakeOne");
    futexWake(&address, 1);
}

void futexWakeAll(Atomic<UInt32> & address)
{
    injectFaultBefore("futex::wakeAll");
    futexWake(&address, std::numeric_limits<size_t>::max());
}

#elif defined(EXECUTOR_FAULT_INJECTION)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value)
{
    injectFaultBefore("futex::waitFetch");
    if (spuriousFailure())
        value = address.load();
    else
        DB::futexWaitFetch(address.underlying(), value);
    injectFaultAfter("futex::waitFetch");
}

void futexWakeOne(Atomic<UInt32> & address)
{
    injectFaultBefore("futex::wakeOne");
    DB::futexWakeOne(address.underlying());
    injectFaultAfter("futex::wakeOne");
}

void futexWakeAll(Atomic<UInt32> & address)
{
    injectFaultBefore("futex::wakeAll");
    DB::futexWakeAll(address.underlying());
    injectFaultAfter("futex::wakeAll");
}

#endif

}
