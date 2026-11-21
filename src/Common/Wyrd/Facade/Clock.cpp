#include <Common/Wyrd/Facade/Clock.h>

#if defined(WYRD_SIMULATION)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#else
#    include <Common/Stopwatch.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_SIMULATION)

UInt64 monotonicNanoseconds()
{
    return advanceClock();
}

#elif defined(WYRD_ADVERSARY)

UInt64 monotonicNanoseconds()
{
    return clock_gettime_ns(CLOCK_MONOTONIC);
}

#else

UInt64 monotonicNanoseconds()
{
    return clock_gettime_ns(CLOCK_MONOTONIC);
}

#endif

}
