#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/InjectFault.h>
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#    include <fmt/format.h>
#    include <limits>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/InjectFault.h>
#    include <Common/futex.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value, std::source_location location)
{
    if (address.load(std::memory_order::seq_cst, location) == value)
        futexWait(&address, location);
    value = address.load(std::memory_order::seq_cst, location);
    trace(fmt::format("{}: futex::waitFetch -> {}", address.getName(), value));
}

void futexWakeOne(Atomic<UInt32> & address, std::source_location location)
{
    injectFault("futex::wakeOne", location);
    futexWake(&address, 1, location);
}

void futexWakeAll(Atomic<UInt32> & address, std::source_location location)
{
    injectFault("futex::wakeAll", location);
    futexWake(&address, std::numeric_limits<size_t>::max(), location);
}

#elif defined(WYRD_ADVERSARY)

void futexWaitFetch(Atomic<UInt32> & address, UInt32 & value, std::source_location location)
{
    injectFault("futex::waitFetch", location);
    if (spuriousFailure(location))
        value = address.load(std::memory_order::seq_cst, location);
    else
        DB::futexWaitFetch(address.underlying(), value);
}

void futexWakeOne(Atomic<UInt32> & address, std::source_location location)
{
    injectFault("futex::wakeOne", location);
    DB::futexWakeOne(address.underlying());
}

void futexWakeAll(Atomic<UInt32> & address, std::source_location location)
{
    injectFault("futex::wakeAll", location);
    DB::futexWakeAll(address.underlying());
}

#endif

}
