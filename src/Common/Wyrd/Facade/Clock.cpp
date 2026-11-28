#include <Common/Wyrd/Facade/Clock.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <Common/Stopwatch.h>
#else
#    include <Common/Stopwatch.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

static UInt64 monotonicNanoseconds()
{
    return advanceClock();
}

}

namespace AdversaryMode
{

static UInt64 monotonicNanoseconds()
{
    return clock_gettime_ns(CLOCK_MONOTONIC);
}

}

UInt64 monotonicNanoseconds()
{
    if (getMode() == Mode::Simulation)
        return SimulationMode::monotonicNanoseconds();
    else
        return AdversaryMode::monotonicNanoseconds();
}

#else

UInt64 monotonicNanoseconds()
{
    return clock_gettime_ns(CLOCK_MONOTONIC);
}

#endif

}
